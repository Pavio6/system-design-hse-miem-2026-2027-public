package pg_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	bookingpg "github.com/nikolaysavelev/soldout/internal/booking/adapters/pg"
	"github.com/nikolaysavelev/soldout/internal/booking/api"
	"github.com/nikolaysavelev/soldout/internal/booking/app"
	catalogapi "github.com/nikolaysavelev/soldout/internal/catalog/api"
	"github.com/nikolaysavelev/soldout/internal/platform/db"
	queueapi "github.com/nikolaysavelev/soldout/internal/queue/api"
)

// Интеграционный тест: два (и более) конкурентных hold на одно место → ровно один успех (I1).
// Требует Docker; пропускается с -short.
func TestConcurrentHolds_ExactlyOneWins(t *testing.T) {
	if testing.Short() {
		t.Skip("интеграционный тест: нужен Docker (пропуск с -short)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pgc, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("soldout"), tcpostgres.WithUsername("soldout"), tcpostgres.WithPassword("soldout"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		t.Fatalf("postgres container: %v", err)
	}
	t.Cleanup(func() { _ = pgc.Terminate(context.Background()) })

	url, err := pgc.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, url, slog.Default()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := db.NewPool(ctx, url, db.PoolOptions{MaxConns: 50}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := db.Seed(ctx, pool, 3000, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	eventID := uuid.MustParse("10000000-0000-4000-8000-000000000001")
	venueID := uuid.MustParse("20000000-0000-4000-8000-000000000001")
	svc := app.New(bookingpg.New(pool), fakeCatalog{eventID: eventID, venueID: venueID}, fakeQueue{}, fakePayment{}, fakeTickets{},
		app.Options{HoldTTL: time.Minute, TicketPriceMinor: 100}, slog.Default())

	const workers = 20
	var wg sync.WaitGroup
	results := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.CreateHold(ctx, api.CreateHoldInput{EventID: eventID, SeatID: 1, UserID: uuid.New(), AdmissionToken: "ok"})
			results <- err
		}()
	}
	wg.Wait()
	close(results)

	var ok, held int
	for err := range results {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, api.ErrSeatHeld):
			held++
		default:
			t.Errorf("неожиданная ошибка: %v", err)
		}
	}
	if ok != 1 || held != workers-1 {
		t.Fatalf("ожидался ровно один успех и %d конфликтов, получено ok=%d held=%d", workers-1, ok, held)
	}

	// Лимит 4: пятый hold того же пользователя на другое место → ErrHoldLimit.
	user := uuid.New()
	for seat := int64(10); seat < 14; seat++ {
		if _, err := svc.CreateHold(ctx, api.CreateHoldInput{EventID: eventID, SeatID: seat, UserID: user, AdmissionToken: "ok"}); err != nil {
			t.Fatalf("hold %d: %v", seat, err)
		}
	}
	if _, err := svc.CreateHold(ctx, api.CreateHoldInput{EventID: eventID, SeatID: 14, UserID: user, AdmissionToken: "ok"}); !errors.Is(err, api.ErrHoldLimit) {
		t.Fatalf("пятый hold должен упереться в лимит, получено %v", err)
	}

	// Освобождаем один hold → лимит снова позволяет; полный цикл order → pay → sold, повтор pay идемпотентен.
	orders0, err := bookingpg.New(pool).ListUserOrders(ctx, user)
	if err != nil || len(orders0) != 0 {
		t.Fatalf("у пользователя не должно быть заказов: %v %d", err, len(orders0))
	}
	first, err := svc.SeatStates(ctx, eventID)
	if err != nil {
		t.Fatal(err)
	}
	if first[10] != api.SeatStateHeld {
		t.Fatalf("место 10 должно быть held: %v", first[10])
	}
	var holdID uuid.UUID
	// находим id hold на место 13 через прямой запрос
	if err := pool.QueryRow(ctx, "/* test */ SELECT id FROM holds WHERE seat_id = 13 AND status = 'active'").Scan(&holdID); err != nil {
		t.Fatal(err)
	}
	if err := svc.ReleaseHold(ctx, holdID); err != nil {
		t.Fatalf("release: %v", err)
	}
	h, err := svc.CreateHold(ctx, api.CreateHoldInput{EventID: eventID, SeatID: 100, UserID: user, AdmissionToken: "ok"})
	if err != nil {
		t.Fatalf("hold после освобождения: %v", err)
	}
	order, created, err := svc.CreateOrder(ctx, api.CreateOrderInput{HoldIDs: []uuid.UUID{h.ID}, UserID: user, IdempotencyKey: "k-1"})
	if err != nil || !created || order.Status != api.OrderPending || order.AmountMinor != 100 {
		t.Fatalf("create order: %+v created=%v err=%v", order, created, err)
	}
	if again, created2, err := svc.CreateOrder(ctx, api.CreateOrderInput{HoldIDs: []uuid.UUID{h.ID}, UserID: user, IdempotencyKey: "k-1"}); err != nil || created2 || again.ID != order.ID {
		t.Fatalf("повтор с тем же Idempotency-Key должен вернуть тот же заказ: %+v created=%v err=%v", again, created2, err)
	}
	if _, _, err := svc.CreateOrder(ctx, api.CreateOrderInput{HoldIDs: []uuid.UUID{h.ID}, UserID: user, IdempotencyKey: "k-2"}); !errors.Is(err, api.ErrHoldAlreadyOrdered) {
		t.Fatalf("hold в двух заказах запрещён: %v", err)
	}
	paid, err := svc.PayOrder(ctx, api.PayOrderInput{OrderID: order.ID})
	if err != nil || paid.Status != api.OrderPaid {
		t.Fatalf("pay: %+v %v", paid, err)
	}
	if again, err := svc.PayOrder(ctx, api.PayOrderInput{OrderID: order.ID}); err != nil || again.Status != api.OrderPaid {
		t.Fatalf("повторный pay должен быть идемпотентен: %+v %v", again, err)
	}
	states, err := svc.SeatStates(ctx, eventID)
	if err != nil || states[100] != api.SeatStateSold {
		t.Fatalf("место 100 должно быть sold: %v %v", states[100], err)
	}
	if n, err := svc.SoldCount(ctx, eventID); err != nil || n != 1 {
		t.Fatalf("SoldCount = %d, %v; ожидалось 1", n, err)
	}
	if _, err := svc.CreateHold(ctx, api.CreateHoldInput{EventID: eventID, SeatID: 100, UserID: uuid.New(), AdmissionToken: "ok"}); !errors.Is(err, api.ErrSeatSold) {
		t.Fatalf("проданное место нельзя удержать: %v", err)
	}
	// Отсутствие токена допуска → ErrAdmissionRequired.
	if _, err := svc.CreateHold(ctx, api.CreateHoldInput{EventID: eventID, SeatID: 101, UserID: uuid.New()}); !errors.Is(err, api.ErrAdmissionRequired) {
		t.Fatalf("без токена ожидался ErrAdmissionRequired: %v", err)
	}
}

type fakeCatalog struct{ eventID, venueID uuid.UUID }

func (f fakeCatalog) ListEvents(context.Context) ([]catalogapi.Event, error) { return nil, nil }
func (f fakeCatalog) GetEvent(_ context.Context, id uuid.UUID) (catalogapi.Event, error) {
	if id != f.eventID {
		return catalogapi.Event{}, catalogapi.ErrEventNotFound
	}
	return catalogapi.Event{ID: id, VenueID: f.venueID, SalesState: catalogapi.SalesOpen}, nil
}
func (f fakeCatalog) GetSeat(_ context.Context, seatID int64) (catalogapi.Seat, error) {
	return catalogapi.Seat{ID: seatID, VenueID: f.venueID}, nil
}
func (f fakeCatalog) SeatMap(context.Context, uuid.UUID) (catalogapi.SeatMap, error) {
	return catalogapi.SeatMap{}, nil
}
func (f fakeCatalog) SectorSeatMap(context.Context, uuid.UUID, uuid.UUID) (catalogapi.SectorSeatMapResult, error) {
	return catalogapi.SectorSeatMapResult{}, nil
}
func (f fakeCatalog) CreateEvent(context.Context, catalogapi.CreateEventInput) (catalogapi.Event, error) {
	return catalogapi.Event{}, nil
}
func (f fakeCatalog) OpenSales(context.Context, uuid.UUID) (catalogapi.Event, error) {
	return catalogapi.Event{}, nil
}
func (f fakeCatalog) GetSector(context.Context, uuid.UUID) (catalogapi.Sector, error) {
	return catalogapi.Sector{}, catalogapi.ErrSectorNotFound
}
func (f fakeCatalog) SeatsBySector(context.Context, uuid.UUID) ([]catalogapi.Seat, error) {
	return nil, nil
}

type fakeQueue struct{}

func (fakeQueue) Join(context.Context, uuid.UUID, uuid.UUID) (queueapi.Admission, error) {
	return queueapi.Admission{Token: "ok"}, nil
}
func (fakeQueue) Validate(_ context.Context, token string, _, _ uuid.UUID) error {
	if token != "ok" {
		return queueapi.ErrAdmissionInvalid
	}
	return nil
}

type fakePayment struct{}

func (fakePayment) Charge(context.Context, api.ChargeRequest) (api.ChargeResult, error) {
	return api.ChargeResult{Succeeded: true, PSPRef: "test"}, nil
}

type fakeTickets struct{}

func (fakeTickets) Issue(context.Context, api.IssueRequest) error { return nil }
