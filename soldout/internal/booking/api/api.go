// Package api — публичный контракт модуля booking: hold/order, ошибки, порты для payment и ticketing.
package api

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// HoldStatus — статус удержания.
type HoldStatus string

const (
	HoldActive    HoldStatus = "active"
	HoldReleased  HoldStatus = "released"
	HoldConfirmed HoldStatus = "confirmed"
)

// OrderStatus — статус заказа.
type OrderStatus string

const (
	OrderPending OrderStatus = "pending"
	OrderPaid    OrderStatus = "paid"
	OrderFailed  OrderStatus = "failed"
	OrderExpired OrderStatus = "expired"
)

// SeatState — состояние места с точки зрения booking (для карты зала).
type SeatState string

const (
	SeatStateHeld SeatState = "held"
	SeatStateSold SeatState = "sold"
	SeatStateFree SeatState = "free"
)

// TopicSeatState — топик событий изменения статуса места (platform/eventbus).
const TopicSeatState = "booking.seat_state"

// SeatStateChanged публикуется booking после commit: место удержано (held), продано (sold) или освобождено (free).
// Подписчик — catalog (проекция карты зала). Версия сектора считается подписчиком в БД, не здесь.
type SeatStateChanged struct {
	EventID uuid.UUID
	SeatID  int64
	State   SeatState
	HoldID  uuid.UUID
	At      time.Time
}

// Topic — для eventbus.
func (SeatStateChanged) Topic() string { return TopicSeatState }

// Key — порядок событий на одно место сохраняется.
func (e SeatStateChanged) Key() string {
	return e.EventID.String() + ":" + strconv.FormatInt(e.SeatID, 10)
}

// OrderPaidEvent — событие «заказ оплачен» (занятие 4). Имя OrderPaid занято статусом заказа. Пишется в outbox в транзакции pay (сразу после MarkPaid),
// доставляется Debezium → Kafka (топик outbox.event.order, ключ order_id) потребителям: notifier (письмо),
// с занятия 7 — аналитика. Кодов билетов в событии нет: письмо ссылается на заказ, коды — GET /v1/users/{id}/tickets.
// Схема и заголовки — docs/asyncapi.yaml.
type OrderPaidEvent struct {
	ID          uuid.UUID `json:"id"`       // id события (заголовок id, ключ дедупликации у потребителя)
	OrderID     uuid.UUID `json:"order_id"` // ключ сообщения: порядок внутри заказа
	EventID     uuid.UUID `json:"event_id"` // мероприятие
	UserID      uuid.UUID `json:"user_id"`  // без PII (NFR-7)
	SeatIDs     []int64   `json:"seat_ids"`
	AmountMinor int64     `json:"amount_minor"`
	At          time.Time `json:"at"` // момент commit оплаты: задержка доставки = now − at
}

// Имена для outbox (формат Debezium EventRouter).
const (
	AggregateOrder     = "order"     // → топик outbox.event.order
	EventTypeOrderPaid = "OrderPaid" // заголовок type
)

// OutboxID, AggregateType, AggregateID, EventType — platform/outbox.Event.
func (e OrderPaidEvent) OutboxID() uuid.UUID   { return e.ID }
func (e OrderPaidEvent) AggregateType() string { return AggregateOrder }
func (e OrderPaidEvent) AggregateID() string   { return e.OrderID.String() }
func (e OrderPaidEvent) EventType() string     { return EventTypeOrderPaid }

// MaxTicketsPerUser — FR-7: не более 4 билетов на пользователя на мероприятие.
const MaxTicketsPerUser = 4

// Hold — удержание места.
type Hold struct {
	ID        uuid.UUID  `json:"id"`
	EventID   uuid.UUID  `json:"event_id"`
	SeatID    int64      `json:"seat_id"`
	UserID    uuid.UUID  `json:"user_id"`
	Status    HoldStatus `json:"status"`
	ExpiresAt time.Time  `json:"expires_at"`
	CreatedAt time.Time  `json:"created_at"`
	GroupID   *uuid.UUID `json:"group_id,omitempty"`
}

// HoldGroup — атомарное удержание N соседних мест одного ряда (V3).
type HoldGroup struct {
	ID        uuid.UUID   `json:"id"`
	EventID   uuid.UUID   `json:"event_id"`
	SectorID  uuid.UUID   `json:"sector_id"`
	UserID    uuid.UUID   `json:"user_id"`
	RowNo     int         `json:"row"`
	SeatFrom  int         `json:"seat_from"`
	Count     int         `json:"count"`
	HoldIDs   []uuid.UUID `json:"hold_ids"`
	SeatIDs   []int64     `json:"seat_ids"`
	ExpiresAt time.Time   `json:"expires_at"`
	CreatedAt time.Time   `json:"created_at"`
}

// Order — заказ.
type Order struct {
	ID             uuid.UUID   `json:"id"`
	EventID        uuid.UUID   `json:"event_id"`
	UserID         uuid.UUID   `json:"user_id"`
	Status         OrderStatus `json:"status"`
	AmountMinor    int64       `json:"amount_minor"` // в копейках
	Amount         string      `json:"amount"`       // "5000.00"
	HoldIDs        []uuid.UUID `json:"hold_ids"`
	SeatIDs        []int64     `json:"seat_ids"`
	IdempotencyKey string      `json:"-"`
	CreatedAt      time.Time   `json:"created_at"`
	UpdatedAt      time.Time   `json:"updated_at"`
}

// CreateHoldInput — POST /v1/holds.
type CreateHoldInput struct {
	EventID        uuid.UUID
	SeatID         int64
	UserID         uuid.UUID
	AdmissionToken string
}

// CreateAdjacentHoldsInput — POST /v1/holds/adjacent.
type CreateAdjacentHoldsInput struct {
	EventID        uuid.UUID
	SectorID       uuid.UUID
	UserID         uuid.UUID
	Count          int
	AdmissionToken string
	IdempotencyKey string
}

// CreateOrderInput — POST /v1/orders.
type CreateOrderInput struct {
	HoldIDs        []uuid.UUID
	UserID         uuid.UUID
	IdempotencyKey string
}

// PayOrderInput — POST /v1/orders/{id}/pay.
type PayOrderInput struct {
	OrderID        uuid.UUID
	IdempotencyKey string // пусто — используется id заказа
}

// Ошибки модуля.
var (
	ErrSeatHeld            = errors.New("booking: место уже удержано")
	ErrSeatSold            = errors.New("booking: место уже продано")
	ErrSeatNotInEvent      = errors.New("booking: место не относится к площадке мероприятия")
	ErrSalesClosed         = errors.New("booking: продажи не открыты")
	ErrHoldLimit           = errors.New("booking: превышен лимит билетов на пользователя")
	ErrHoldNotFound        = errors.New("booking: hold не найден")
	ErrHoldNotActive       = errors.New("booking: hold не активен")
	ErrHoldExpired         = errors.New("booking: hold истёк")
	ErrHoldAlreadyOrdered  = errors.New("booking: hold уже входит в заказ")
	ErrHoldsMismatch       = errors.New("booking: hold'ы принадлежат разным пользователям или мероприятиям")
	ErrOrderNotFound       = errors.New("booking: заказ не найден")
	ErrOrderNotPending     = errors.New("booking: заказ не в состоянии pending")
	ErrOrderExpired        = errors.New("booking: заказ истёк — hold'ы освобождены")
	ErrPaymentFailed       = errors.New("booking: оплата отклонена")
	ErrIdempotencyConflict = errors.New("booking: Idempotency-Key уже использован с другими параметрами")
	ErrValidation          = errors.New("booking: некорректные данные")
	ErrAdmissionRequired   = errors.New("booking: нужен действующий токен допуска (X-Admission-Token)")
	ErrNoAdjacentSeats     = errors.New("booking: в секторе нет непрерывного отрезка свободных мест")
	ErrStandingSector      = errors.New("booking: соседние места есть только в сидячем секторе")
	ErrSectorNotInEvent    = errors.New("booking: сектор не относится к площадке мероприятия")
	ErrGroupNotFound       = errors.New("booking: группа hold не найдена")
)

// PaymentGateway — порт к модулю payment (реализует payment/app).
type PaymentGateway interface {
	Charge(ctx context.Context, req ChargeRequest) (ChargeResult, error)
}

// ChargeRequest — запрос на списание.
type ChargeRequest struct {
	OrderID        uuid.UUID
	UserID         uuid.UUID
	AmountMinor    int64
	IdempotencyKey string
}

// ChargeResult — результат списания. Succeeded=false — бизнес-отказ (не ошибка инфраструктуры).
type ChargeResult struct {
	Succeeded bool
	PSPRef    string
	Reason    string
}

// TicketIssuer — порт к модулю ticketing (реализует ticketing/app). Issue идемпотентен.
type TicketIssuer interface {
	Issue(ctx context.Context, req IssueRequest) error
}

// IssueRequest — данные для выпуска билетов по оплаченному заказу.
type IssueRequest struct {
	OrderID uuid.UUID
	EventID uuid.UUID
	UserID  uuid.UUID
	SeatIDs []int64
}

// SeatStateReader — статусы мест мероприятия (для карты зала в catalog).
type SeatStateReader interface {
	SeatStates(ctx context.Context, eventID uuid.UUID) (map[int64]SeatState, error)
}

// SoldCounter — число проданных билетов мероприятия (для catalog/analytics).
type SoldCounter interface {
	SoldCount(ctx context.Context, eventID uuid.UUID) (int, error)
}

// Service — публичный сервис бронирования.
type Service interface {
	SeatStateReader
	SoldCounter
	CreateHold(ctx context.Context, in CreateHoldInput) (Hold, error)
	CreateAdjacentHolds(ctx context.Context, in CreateAdjacentHoldsInput) (HoldGroup, bool, error)
	ReleaseHold(ctx context.Context, holdID uuid.UUID) error
	GetHold(ctx context.Context, holdID uuid.UUID) (Hold, error)
	CreateOrder(ctx context.Context, in CreateOrderInput) (Order, bool, error) // bool: true — заказ создан, false — повтор
	PayOrder(ctx context.Context, in PayOrderInput) (Order, error)
	GetOrder(ctx context.Context, orderID uuid.UUID) (Order, error)
	ListUserOrders(ctx context.Context, userID uuid.UUID) ([]Order, error)
}
