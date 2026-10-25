// Package api — публичный контракт модуля catalog: DTO, ошибки и интерфейс сервиса.
// Другие модули импортируют только этот пакет (правило зависимостей, .go-arch-lint.yml).
package api

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// SalesState — состояние продаж мероприятия.
type SalesState string

const (
	SalesScheduled SalesState = "scheduled"
	SalesOpen      SalesState = "open"
	SalesClosed    SalesState = "closed"
)

// SectorKind — тип сектора: нумерованные места или танцпол (квота).
type SectorKind string

const (
	SectorSeated   SectorKind = "seated"
	SectorStanding SectorKind = "standing"
)

// SeatStatus — статус места на карте зала.
type SeatStatus string

const (
	SeatFree SeatStatus = "free"
	SeatHeld SeatStatus = "held"
	SeatSold SeatStatus = "sold"
)

// Event — мероприятие.
type Event struct {
	ID          uuid.UUID  `json:"id"`
	Name        string     `json:"name"`
	VenueID     uuid.UUID  `json:"venue_id"`
	StartsAt    time.Time  `json:"starts_at"`
	SalesOpenAt time.Time  `json:"sales_open_at"`
	SalesState  SalesState `json:"sales_state"`
}

// Seat — место; VenueID нужен booking, чтобы проверить принадлежность места мероприятию.
type Seat struct {
	ID       int64     `json:"id"`
	SectorID uuid.UUID `json:"sector_id"`
	VenueID  uuid.UUID `json:"venue_id"`
	RowNo    int       `json:"row"`
	SeatNo   int       `json:"num"`
}

// Sector — сектор площадки. Kind отличает сидячие ряды от танцпола.
type Sector struct {
	ID       uuid.UUID  `json:"id"`
	VenueID  uuid.UUID  `json:"venue_id"`
	Name     string     `json:"name"`
	Kind     SectorKind `json:"kind"`
	Capacity int        `json:"capacity"`
}

// SeatMap — сводка карты зала по секторам (с занятия 2 полная карта не отдаётся: см. SectorSeatMap).
type SeatMap struct {
	EventID uuid.UUID       `json:"event_id"`
	Sectors []SectorSummary `json:"sectors"`
}

// SectorSummary — счётчики сектора и версия его карты (ETag карты сектора).
type SectorSummary struct {
	ID      uuid.UUID  `json:"id"`
	Name    string     `json:"name"`
	Kind    SectorKind `json:"kind"`
	Total   int        `json:"total"`
	Free    int        `json:"free"`
	Held    int        `json:"held"`
	Sold    int        `json:"sold"`
	Version int64      `json:"version"`
}

// SeatMapSector — карта одного сектора.
type SeatMapSector struct {
	EventID uuid.UUID     `json:"event_id"`
	ID      uuid.UUID     `json:"id"`
	Name    string        `json:"name"`
	Kind    SectorKind    `json:"kind"`
	Version int64         `json:"version"`
	Seats   []SeatMapSeat `json:"seats"`
}

// SectorSeatMapResult — предсобранный JSON карты сектора и его версия (для ETag и кэша).
type SectorSeatMapResult struct {
	JSON    []byte
	Version int64
	Cached  bool
	Stale   bool // данные из кэша устарели, обновление идёт фоном (stale-while-revalidate)
}

// SeatMapSeat — место на карте зала.
type SeatMapSeat struct {
	ID     int64      `json:"id"`
	RowNo  int        `json:"row"`
	SeatNo int        `json:"num"`
	Status SeatStatus `json:"status"`
}

// CreateEventInput — данные для создания мероприятия (админ).
type CreateEventInput struct {
	Name        string    `json:"name"`
	VenueID     uuid.UUID `json:"venue_id"`
	StartsAt    time.Time `json:"starts_at"`
	SalesOpenAt time.Time `json:"sales_open_at"`
}

// Ошибки модуля.
var (
	ErrEventNotFound     = errors.New("catalog: мероприятие не найдено")
	ErrSectorNotFound    = errors.New("catalog: сектор не найден")
	ErrSeatNotFound      = errors.New("catalog: место не найдено")
	ErrVenueNotFound     = errors.New("catalog: площадка не найдена")
	ErrInvalidTransition = errors.New("catalog: недопустимый переход состояния продаж")
	ErrValidation        = errors.New("catalog: некорректные данные")
)

// Service — публичный сервис каталога.
type Service interface {
	ListEvents(ctx context.Context) ([]Event, error)
	GetEvent(ctx context.Context, id uuid.UUID) (Event, error)
	GetSeat(ctx context.Context, seatID int64) (Seat, error)
	SeatMap(ctx context.Context, eventID uuid.UUID) (SeatMap, error)
	SectorSeatMap(ctx context.Context, eventID, sectorID uuid.UUID) (SectorSeatMapResult, error)
	CreateEvent(ctx context.Context, in CreateEventInput) (Event, error)
	OpenSales(ctx context.Context, id uuid.UUID) (Event, error)
	GetSector(ctx context.Context, id uuid.UUID) (Sector, error)
	SeatsBySector(ctx context.Context, sectorID uuid.UUID) ([]Seat, error)
}
