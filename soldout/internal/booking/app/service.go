// Package app — сценарии бронирования: CreateHold, ReleaseHold, CreateOrder, PayOrder, expirer.
//
// Транзакционные границы: каждая мутация — одна транзакция PostgreSQL через Store.InTx.
// Блокировка места — advisory-lock на (event_id, seat_id): booking не трогает таблицу seats (она принадлежит catalog).
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/nikolaysavelev/soldout/internal/booking/api"
	"github.com/nikolaysavelev/soldout/internal/booking/domain"
	catalogapi "github.com/nikolaysavelev/soldout/internal/catalog/api"
	"github.com/nikolaysavelev/soldout/internal/platform/eventbus"
	"github.com/nikolaysavelev/soldout/internal/platform/faults"
	"github.com/nikolaysavelev/soldout/internal/platform/otel"
	queueapi "github.com/nikolaysavelev/soldout/internal/queue/api"
)

// Tx — операции внутри транзакции.
type Tx interface {
	LockSeat(ctx context.Context, eventID uuid.UUID, seatID int64) error
	TryLockSeat(ctx context.Context, eventID uuid.UUID, seatID int64) (bool, error)
	LockUser(ctx context.Context, eventID, userID uuid.UUID) error
	CountUserHolds(ctx context.Context, eventID, userID uuid.UUID) (int, error)
	SeatSold(ctx context.Context, eventID uuid.UUID, seatID int64) (bool, error)
	SeatTaken(ctx context.Context, eventID uuid.UUID, seatID int64) (bool, error)
	InsertHold(ctx context.Context, h domain.Hold) error
	InsertGroup(ctx context.Context, g domain.HoldGroup) error
	GetHoldsForUpdate(ctx context.Context, ids []uuid.UUID) ([]domain.Hold, error)
	UpdateHold(ctx context.Context, h domain.Hold) error
	HoldsInOpenOrders(ctx context.Context, ids []uuid.UUID) (bool, error)
	InsertOrder(ctx context.Context, o domain.Order) error
	GetOrderForUpdate(ctx context.Context, id uuid.UUID) (domain.Order, error)
	UpdateOrder(ctx context.Context, o domain.Order) error
	// AppendOutbox — событие в outbox в этой же транзакции (занятие 4): commit публикует, rollback отменяет.
	AppendOutbox(ctx context.Context, e api.OrderPaidEvent) error
}

// Store — хранилище booking (реализация в adapters/pg).
type Store interface {
	InTx(ctx context.Context, fn func(tx Tx) error) error
	GetHold(ctx context.Context, id uuid.UUID) (domain.Hold, error)
	GetOrder(ctx context.Context, id uuid.UUID) (domain.Order, error)
	FindOrderByIdempotencyKey(ctx context.Context, key string) (domain.Order, error)
	ListUserOrders(ctx context.Context, userID uuid.UUID) ([]domain.Order, error)
	SeatStates(ctx context.Context, eventID uuid.UUID) (map[int64]api.SeatState, error)
	SoldCount(ctx context.Context, eventID uuid.UUID) (int, error)
	ExpireHoldsBatch(ctx context.Context, now time.Time, limit int) ([]ReleasedHold, error)
	ExpireOrders(ctx context.Context, now time.Time) (int64, error)
	CountActiveHolds(ctx context.Context) (int64, error)
	TakenSeatIDs(ctx context.Context, eventID uuid.UUID, seatIDs []int64) (map[int64]struct{}, error)
	FindGroupByIdempotencyKey(ctx context.Context, key string) (domain.HoldGroup, error)
	HoldsByGroup(ctx context.Context, groupID uuid.UUID) ([]domain.Hold, error)
}

// ReleasedHold — hold, освобождённый expirer'ом (для событий карты зала).
type ReleasedHold struct {
	ID      uuid.UUID
	EventID uuid.UUID
	SeatID  int64
}

// Options — настройки сервиса.
type Options struct {
	HoldTTL          time.Duration
	TicketPriceMinor int64
	Faults           faults.Flags
	Events           eventbus.Publisher // события SeatStateChanged после commit; nil = не публиковать
}

// Service — реализация api.Service.
type Service struct {
	store   Store
	catalog catalogapi.Service
	queue   queueapi.Service
	payment api.PaymentGateway
	tickets api.TicketIssuer
	opts    Options
	logger  *slog.Logger
	now     func() time.Time
}

// New собирает сервис.
func New(store Store, catalog catalogapi.Service, queue queueapi.Service, payment api.PaymentGateway, tickets api.TicketIssuer, opts Options, logger *slog.Logger) *Service {
	if opts.HoldTTL <= 0 {
		opts.HoldTTL = 10 * time.Minute
	}
	if opts.Events == nil {
		opts.Events = eventbus.Nop{}
	}
	return &Service{store: store, catalog: catalog, queue: queue, payment: payment, tickets: tickets, opts: opts, logger: logger, now: time.Now}
}

var _ api.Service = (*Service)(nil)

// CreateHold — FR-3/FR-7: удержать место на HoldTTL; 409 если занято, 422 если лимит.
func (s *Service) CreateHold(ctx context.Context, in api.CreateHoldInput) (api.Hold, error) {
	if in.EventID == uuid.Nil || in.UserID == uuid.Nil || in.SeatID <= 0 {
		return api.Hold{}, fmt.Errorf("%w: event_id, seat_id и user_id обязательны", api.ErrValidation)
	}
	ev, err := s.catalog.GetEvent(ctx, in.EventID)
	if err != nil {
		return api.Hold{}, err
	}
	if ev.SalesState != catalogapi.SalesOpen {
		return api.Hold{}, fmt.Errorf("%w: состояние %s", api.ErrSalesClosed, ev.SalesState)
	}
	seat, err := s.catalog.GetSeat(ctx, in.SeatID)
	if err != nil {
		return api.Hold{}, err
	}
	if seat.VenueID != ev.VenueID {
		return api.Hold{}, api.ErrSeatNotInEvent
	}
	if err := s.queue.Validate(ctx, in.AdmissionToken, in.EventID, in.UserID); err != nil {
		if errors.Is(err, queueapi.ErrAdmissionInvalid) {
			return api.Hold{}, fmt.Errorf("%w: %v", api.ErrAdmissionRequired, err)
		}
		return api.Hold{}, err
	}

	hold := domain.NewHold(in.EventID, in.SeatID, in.UserID, s.now(), s.opts.HoldTTL)
	err = s.store.InTx(ctx, func(tx Tx) error {
		// Сериализуем конкурентов на одно место. На L1 — без try-lock: очередь на «горячем» месте (см. занятие 2).
		if err := tx.LockSeat(ctx, in.EventID, in.SeatID); err != nil {
			return err
		}
		n, err := tx.CountUserHolds(ctx, in.EventID, in.UserID)
		if err != nil {
			return err
		}
		if !domain.CanHoldMore(n) {
			return fmt.Errorf("%w: уже %d", api.ErrHoldLimit, n)
		}
		sold, err := tx.SeatSold(ctx, in.EventID, in.SeatID)
		if err != nil {
			return err
		}
		if sold {
			return api.ErrSeatSold
		}
		return tx.InsertHold(ctx, hold) // I1: uniq_active_hold → ErrSeatHeld
	})
	if err != nil {
		return api.Hold{}, err
	}
	s.publishSeat(ctx, hold.EventID, hold.SeatID, api.SeatStateHeld, hold.ID)
	return hold.ToAPI(), nil
}

// publishSeat публикует SeatStateChanged после commit (карта зала — проекция в catalog).
func (s *Service) publishSeat(ctx context.Context, eventID uuid.UUID, seatID int64, state api.SeatState, holdID uuid.UUID) {
	_ = s.opts.Events.Publish(ctx, api.SeatStateChanged{EventID: eventID, SeatID: seatID, State: state, HoldID: holdID, At: s.now()})
}

// ReleaseHold — DELETE /v1/holds/{id}.
func (s *Service) ReleaseHold(ctx context.Context, holdID uuid.UUID) error {
	var released domain.Hold
	err := s.store.InTx(ctx, func(tx Tx) error {
		holds, err := tx.GetHoldsForUpdate(ctx, []uuid.UUID{holdID})
		if err != nil {
			return err
		}
		if len(holds) != 1 {
			return api.ErrHoldNotFound
		}
		h := holds[0]
		if err := h.Release(); err != nil {
			return err
		}
		released = h
		return tx.UpdateHold(ctx, h)
	})
	if err != nil {
		return err
	}
	s.publishSeat(ctx, released.EventID, released.SeatID, api.SeatStateFree, released.ID)
	return nil
}

// GetHold — чтение.
func (s *Service) GetHold(ctx context.Context, holdID uuid.UUID) (api.Hold, error) {
	h, err := s.store.GetHold(ctx, holdID)
	if err != nil {
		return api.Hold{}, err
	}
	return h.ToAPI(), nil
}

// CreateOrder — POST /v1/orders с Idempotency-Key. Повтор с тем же ключом возвращает существующий заказ.
func (s *Service) CreateOrder(ctx context.Context, in api.CreateOrderInput) (api.Order, bool, error) {
	if in.IdempotencyKey == "" {
		return api.Order{}, false, fmt.Errorf("%w: заголовок Idempotency-Key обязателен", api.ErrValidation)
	}
	if in.UserID == uuid.Nil || len(in.HoldIDs) == 0 {
		return api.Order{}, false, fmt.Errorf("%w: user_id и hold_ids обязательны", api.ErrValidation)
	}
	if existing, err := s.store.FindOrderByIdempotencyKey(ctx, in.IdempotencyKey); err == nil {
		if existing.UserID != in.UserID || !sameIDs(existing.HoldIDs, in.HoldIDs) {
			return api.Order{}, false, api.ErrIdempotencyConflict
		}
		return existing.ToAPI(), false, nil
	} else if !errors.Is(err, api.ErrOrderNotFound) {
		return api.Order{}, false, err
	}

	var order domain.Order
	err := s.store.InTx(ctx, func(tx Tx) error {
		holds, err := tx.GetHoldsForUpdate(ctx, in.HoldIDs)
		if err != nil {
			return err
		}
		if len(holds) != len(uniqueIDs(in.HoldIDs)) {
			return api.ErrHoldNotFound
		}
		order, err = domain.NewOrder(holds, in.UserID, in.IdempotencyKey, s.opts.TicketPriceMinor, s.now())
		if err != nil {
			return err
		}
		busy, err := tx.HoldsInOpenOrders(ctx, order.HoldIDs)
		if err != nil {
			return err
		}
		if busy {
			return api.ErrHoldAlreadyOrdered
		}
		return tx.InsertOrder(ctx, order)
	})
	if errors.Is(err, api.ErrIdempotencyConflict) {
		// гонка двух одинаковых запросов: второй получает уже созданный заказ
		if existing, err2 := s.store.FindOrderByIdempotencyKey(ctx, in.IdempotencyKey); err2 == nil && existing.UserID == in.UserID {
			return existing.ToAPI(), false, nil
		}
	}
	if err != nil {
		return api.Order{}, false, err
	}
	return order.ToAPI(), true, nil
}

// PayOrder — POST /v1/orders/{id}/pay: платёж → confirm hold'ов → выпуск билетов (синхронно на L1).
func (s *Service) PayOrder(ctx context.Context, in api.PayOrderInput) (_ api.Order, err error) {
	ctx, span := otel.Start(ctx, "booking.PayOrder", otel.String("order.id", in.OrderID.String()))
	defer otel.End(span, &err)
	return s.payOrder(ctx, in)
}

func (s *Service) payOrder(ctx context.Context, in api.PayOrderInput) (api.Order, error) {
	order, err := s.store.GetOrder(ctx, in.OrderID)
	if err != nil {
		return api.Order{}, err
	}
	otel.SetUser(ctx, order.UserID.String())
	switch order.Status {
	case api.OrderPaid:
		// идемпотентный повтор: убеждаемся, что билеты выпущены (Issue идемпотентен)
		return order.ToAPI(), s.issueTickets(ctx, order)
	case api.OrderFailed, api.OrderExpired:
		return api.Order{}, fmt.Errorf("%w: статус %s", api.ErrOrderNotPending, order.Status)
	}

	// 1. hold'ы ещё живы? иначе заказ истекает
	now := s.now()
	err = s.store.InTx(ctx, func(tx Tx) error {
		o, err := tx.GetOrderForUpdate(ctx, in.OrderID)
		if err != nil {
			return err
		}
		if o.Status != api.OrderPending {
			return nil
		}
		holds, err := tx.GetHoldsForUpdate(ctx, o.HoldIDs)
		if err != nil {
			return err
		}
		for _, h := range holds {
			if !h.IsUsable(now) {
				if err := o.MarkExpired(now); err != nil {
					return err
				}
				if err := tx.UpdateOrder(ctx, o); err != nil {
					return err
				}
				return api.ErrOrderExpired
			}
		}
		return nil
	})
	if err != nil {
		return api.Order{}, err
	}

	// 2. платёж (идемпотентен по ключу; повтор с тем же ключом не списывает второй раз)
	key := in.IdempotencyKey
	if key == "" {
		key = "order:" + order.ID.String()
	}
	res, err := s.payment.Charge(ctx, api.ChargeRequest{OrderID: order.ID, UserID: order.UserID, AmountMinor: order.AmountMinor, IdempotencyKey: key})
	if err != nil {
		return api.Order{}, fmt.Errorf("booking: платёжный шлюз: %w", err)
	}
	now = s.now()
	if !res.Succeeded {
		// FR-4: при неудаче hold снимается
		var freed []domain.Hold
		err = s.store.InTx(ctx, func(tx Tx) error {
			o, err := tx.GetOrderForUpdate(ctx, order.ID)
			if err != nil {
				return err
			}
			if o.Status != api.OrderPending {
				return nil
			}
			if err := o.MarkFailed(now); err != nil {
				return err
			}
			if err := tx.UpdateOrder(ctx, o); err != nil {
				return err
			}
			holds, err := tx.GetHoldsForUpdate(ctx, o.HoldIDs)
			if err != nil {
				return err
			}
			for i := range holds {
				if holds[i].Status == api.HoldActive {
					if err := holds[i].Release(); err != nil {
						return err
					}
					if err := tx.UpdateHold(ctx, holds[i]); err != nil {
						return err
					}
					freed = append(freed, holds[i])
				}
			}
			return nil
		})
		if err != nil {
			return api.Order{}, err
		}
		for _, h := range freed {
			s.publishSeat(ctx, h.EventID, h.SeatID, api.SeatStateFree, h.ID)
		}
		return api.Order{}, fmt.Errorf("%w: %s", api.ErrPaymentFailed, res.Reason)
	}

	// 3. подтверждаем hold'ы и заказ в одной транзакции
	var paid domain.Order
	var confirmed []domain.Hold
	err = s.store.InTx(ctx, func(tx Tx) error {
		o, err := tx.GetOrderForUpdate(ctx, order.ID)
		if err != nil {
			return err
		}
		if o.Status == api.OrderPaid { // гонка двух pay
			paid = o
			return nil
		}
		holds, err := tx.GetHoldsForUpdate(ctx, o.HoldIDs)
		if err != nil {
			return err
		}
		for i := range holds {
			if err := holds[i].Confirm(now); err != nil {
				// деньги списаны, место потеряно: на L1 фиксируем failed и пишем в лог; компенсация — занятие 7
				s.logger.ErrorContext(ctx, "оплата прошла, но hold нельзя подтвердить — нужна компенсация", "order_id", o.ID, "hold_id", holds[i].ID, "err", err)
				if err := o.MarkFailed(now); err != nil {
					return err
				}
				if err := tx.UpdateOrder(ctx, o); err != nil {
					return err
				}
				return fmt.Errorf("%w: %v", api.ErrOrderExpired, err)
			}
			if err := tx.UpdateHold(ctx, holds[i]); err != nil {
				return err
			}
			confirmed = append(confirmed, holds[i])
		}
		if err := o.MarkPaid(now); err != nil {
			return err
		}
		if err := tx.UpdateOrder(ctx, o); err != nil {
			return err
		}
		// письмо (и любой другой потребитель) — через событие в той же транзакции, не вызовом после commit
		if err := tx.AppendOutbox(ctx, api.OrderPaidEvent{
			ID: uuid.New(), OrderID: o.ID, EventID: o.EventID, UserID: o.UserID, SeatIDs: o.SeatIDs,
			AmountMinor: o.AmountMinor, At: now,
		}); err != nil {
			return err
		}
		paid = o
		return nil
	})
	if err != nil {
		return api.Order{}, err
	}
	for _, h := range confirmed {
		s.publishSeat(ctx, h.EventID, h.SeatID, api.SeatStateSold, h.ID)
	}
	s.opts.Faults.MaybeCrashAfterCommit(s.logger, "order.paid")

	// 4. выпуск билетов — синхронно после commit: билет нужен в ответе, Issue идемпотентен, повтор pay довыпускает.
	// Известное ограничение (ADR-003): падение здесь оставляет paid без билета до повтора; перенос выпуска
	// в потребителя OrderPaid и доказательство I3 — задача со звёздочкой HW2. Письмо уходит из outbox.
	if err := s.issueTickets(ctx, paid); err != nil { // nolint:after-commit идемпотентно, restore при повторе pay
		return paid.ToAPI(), err
	}
	return paid.ToAPI(), nil
}

func (s *Service) issueTickets(ctx context.Context, o domain.Order) error {
	err := s.tickets.Issue(ctx, api.IssueRequest{OrderID: o.ID, EventID: o.EventID, UserID: o.UserID, SeatIDs: o.SeatIDs})
	if err != nil {
		return fmt.Errorf("booking: выпуск билетов для заказа %s: %w", o.ID, err)
	}
	return nil
}

// GetOrder — чтение.
func (s *Service) GetOrder(ctx context.Context, orderID uuid.UUID) (api.Order, error) {
	o, err := s.store.GetOrder(ctx, orderID)
	if err != nil {
		return api.Order{}, err
	}
	return o.ToAPI(), nil
}

// ListUserOrders — заказы пользователя (для ticketing).
func (s *Service) ListUserOrders(ctx context.Context, userID uuid.UUID) ([]api.Order, error) {
	orders, err := s.store.ListUserOrders(ctx, userID)
	if err != nil {
		return nil, err
	}
	out := make([]api.Order, 0, len(orders))
	for _, o := range orders {
		out = append(out, o.ToAPI())
	}
	return out, nil
}

// SeatStates — held/sold по местам мероприятия.
func (s *Service) SeatStates(ctx context.Context, eventID uuid.UUID) (map[int64]api.SeatState, error) {
	return s.store.SeatStates(ctx, eventID)
}

// SoldCount — число проданных мест мероприятия (confirmed hold).
func (s *Service) SoldCount(ctx context.Context, eventID uuid.UUID) (int, error) {
	return s.store.SoldCount(ctx, eventID)
}

func uniqueIDs(ids []uuid.UUID) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{}, len(ids))
	out := ids[:0:0]
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func sameIDs(a, b []uuid.UUID) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[uuid.UUID]struct{}, len(a))
	for _, id := range a {
		set[id] = struct{}{}
	}
	for _, id := range b {
		if _, ok := set[id]; !ok {
			return false
		}
	}
	return true
}
