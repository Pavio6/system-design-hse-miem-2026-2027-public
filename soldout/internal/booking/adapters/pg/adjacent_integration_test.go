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
	catalogpg "github.com/nikolaysavelev/soldout/internal/catalog/adapters/pg"
	catalogapp "github.com/nikolaysavelev/soldout/internal/catalog/app"
	"github.com/nikolaysavelev/soldout/internal/platform/db"
)

// Соседние места: атомарность, лимит, идемпотентность, конкуренция, отказ танцпола.
func TestAdjacentHolds(t *testing.T) {
	if testing.Short() {
		t.Skip("интеграционный тест: нужен Docker (пропуск с -short)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
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
	pool, err := db.NewPool(ctx, url, db.PoolOptions{MaxConns: 20}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := db.Seed(ctx, pool, 3000, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	event1 := uuid.MustParse("10000000-0000-4000-8000-000000000001")
	event2 := uuid.MustParse("10000000-0000-4000-8000-000000000002")
	// seed: sector id = 30000000-0000-4000-8000-<hex(event*1000+sector)>, 3 сектора по 1000.
	// Сектор 1 сидячий (20×50). Сектор 2 — танцпол, потому что sec > n_sectors-2.
	sector1 := uuid.MustParse("30000000-0000-4000-8000-0000000003e9")
	standing := uuid.MustParse("30000000-0000-4000-8000-0000000003ea")
	sector2event2 := uuid.MustParse("30000000-0000-4000-8000-0000000007d1")

	svc := app.New(bookingpg.New(pool), catalogapp.New(catalogpg.New(pool), nil, catalogapp.Options{}, nil, slog.Default()),
		fakeQueue{}, fakePayment{}, fakeTickets{}, app.Options{HoldTTL: time.Minute, TicketPriceMinor: 100}, slog.Default())

	t.Run("первый ряд", func(t *testing.T) {
		g, created, err := svc.CreateAdjacentHolds(ctx, api.CreateAdjacentHoldsInput{
			EventID: event1, SectorID: sector1, UserID: uuid.New(), Count: 4, AdmissionToken: "ok", IdempotencyKey: "first",
		})
		if err != nil || !created {
			t.Fatalf("created=%v err=%v", created, err)
		}
		if g.RowNo != 1 || g.SeatFrom != 1 || g.Count != 4 || len(g.HoldIDs) != 4 || len(g.SeatIDs) != 4 {
			t.Fatalf("группа %+v", g)
		}
	})

	t.Run("дырка не оставляет частичный hold", func(t *testing.T) {
		// Мероприятие 2, сектор 1: место 2 ряда 1 имеет id (2-1)*3000+2 = 3002.
		other := uuid.New()
		if _, err := svc.CreateHold(ctx, api.CreateHoldInput{EventID: event2, SeatID: 3002, UserID: other, AdmissionToken: "ok"}); err != nil {
			t.Fatal(err)
		}
		g, _, err := svc.CreateAdjacentHolds(ctx, api.CreateAdjacentHoldsInput{
			EventID: event2, SectorID: sector2event2, UserID: uuid.New(), Count: 4, AdmissionToken: "ok",
		})
		if err != nil {
			t.Fatal(err)
		}
		if g.RowNo != 1 || g.SeatFrom != 3 {
			t.Fatalf("ожидался отрезок с места 3, получено ряд %d с %d", g.RowNo, g.SeatFrom)
		}
		var held int
		if err := pool.QueryRow(ctx, "/* test */ SELECT count(*) FROM holds WHERE event_id = $1 AND seat_id = 3001 AND status = 'active'", event2).Scan(&held); err != nil {
			t.Fatal(err)
		}
		if held != 0 {
			t.Fatal("место 1 не должно быть удержано: отрезок через занятое место 2 невозможен")
		}
	})

	t.Run("лимит не создаёт группу", func(t *testing.T) {
		user := uuid.New()
		for _, seat := range []int64{999, 1000} {
			if _, err := svc.CreateHold(ctx, api.CreateHoldInput{EventID: event1, SeatID: seat, UserID: user, AdmissionToken: "ok"}); err != nil {
				t.Fatal(err)
			}
		}
		var before int
		if err := pool.QueryRow(ctx, "/* test */ SELECT count(*) FROM hold_groups WHERE user_id = $1", user).Scan(&before); err != nil {
			t.Fatal(err)
		}
		if _, _, err := svc.CreateAdjacentHolds(ctx, api.CreateAdjacentHoldsInput{
			EventID: event1, SectorID: sector1, UserID: user, Count: 3, AdmissionToken: "ok",
		}); !errors.Is(err, api.ErrHoldLimit) {
			t.Fatalf("ожидался лимит, получено %v", err)
		}
		var after, holds int
		if err := pool.QueryRow(ctx, "/* test */ SELECT count(*) FROM hold_groups WHERE user_id = $1", user).Scan(&after); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, "/* test */ SELECT count(*) FROM holds WHERE user_id = $1 AND status = 'active'", user).Scan(&holds); err != nil {
			t.Fatal(err)
		}
		if after != before || holds != 2 {
			t.Fatalf("после отказа групп %d→%d, hold %d", before, after, holds)
		}
		if _, _, err := svc.CreateAdjacentHolds(ctx, api.CreateAdjacentHoldsInput{
			EventID: event1, SectorID: sector1, UserID: user, Count: 2, AdmissionToken: "ok",
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("идемпотентность", func(t *testing.T) {
		user := uuid.New()
		in := api.CreateAdjacentHoldsInput{
			EventID: event1, SectorID: sector1, UserID: user, Count: 2, AdmissionToken: "ok", IdempotencyKey: "same-key",
		}
		first, created, err := svc.CreateAdjacentHolds(ctx, in)
		if err != nil || !created {
			t.Fatalf("created=%v err=%v", created, err)
		}
		second, created, err := svc.CreateAdjacentHolds(ctx, in)
		if err != nil || created {
			t.Fatalf("повтор created=%v err=%v", created, err)
		}
		if second.ID != first.ID {
			t.Fatalf("повтор создал другую группу %s", second.ID)
		}
		var n int
		if err := pool.QueryRow(ctx, "/* test */ SELECT count(*) FROM holds WHERE group_id = $1", first.ID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatalf("hold в группе %d", n)
		}
		in.Count = 4
		if _, _, err := svc.CreateAdjacentHolds(ctx, in); !errors.Is(err, api.ErrIdempotencyConflict) {
			t.Fatalf("тот же ключ с другим count: %v", err)
		}
	})

	t.Run("танцпол", func(t *testing.T) {
		if _, _, err := svc.CreateAdjacentHolds(ctx, api.CreateAdjacentHoldsInput{
			EventID: event1, SectorID: standing, UserID: uuid.New(), Count: 2, AdmissionToken: "ok",
		}); !errors.Is(err, api.ErrStandingSector) {
			t.Fatalf("танцпол: %v", err)
		}
	})

	t.Run("конкуренция", func(t *testing.T) {
		const workers = 8
		var wg sync.WaitGroup
		groups := make(chan api.HoldGroup, workers)
		errs := make(chan error, workers)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				g, _, err := svc.CreateAdjacentHolds(ctx, api.CreateAdjacentHoldsInput{
					EventID: event2, SectorID: sector2event2, UserID: uuid.New(), Count: 4, AdmissionToken: "ok",
				})
				if err != nil {
					errs <- err
					return
				}
				groups <- g
			}()
		}
		wg.Wait()
		close(errs)
		close(groups)
		for err := range errs {
			t.Errorf("конкурентная группа: %v", err)
		}
		seen := map[int64]uuid.UUID{}
		n := 0
		for g := range groups {
			n++
			if g.Count != 4 || g.RowNo <= 0 || len(g.SeatIDs) != 4 {
				t.Errorf("битая группа %+v", g)
			}
			for _, id := range g.SeatIDs {
				if prev, ok := seen[id]; ok {
					t.Errorf("место %d в группах %s и %s", id, prev, g.ID)
				}
				seen[id] = g.ID
			}
		}
		if n != workers {
			t.Fatalf("успешных групп %d из %d", n, workers)
		}
		var bad int
		err := pool.QueryRow(ctx, `/* test.i7 */ SELECT count(*) FROM (
			SELECT g.id
			FROM hold_groups g
			JOIN holds h ON h.group_id = g.id
			JOIN seats s ON s.id = h.seat_id
			GROUP BY g.id, g.seat_from, g.seat_count, g.row_no, g.sector_id
			HAVING count(*) <> g.seat_count
				OR min(s.seat_no) <> g.seat_from
				OR max(s.seat_no) <> g.seat_from + g.seat_count - 1
				OR count(DISTINCT s.row_no) <> 1
		) d`).Scan(&bad)
		if err != nil {
			t.Fatal(err)
		}
		if bad != 0 {
			t.Fatalf("I7 нарушен у %d групп", bad)
		}
	})
}
