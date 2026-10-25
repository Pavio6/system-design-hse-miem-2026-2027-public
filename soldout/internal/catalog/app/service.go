// Package app — сценарии каталога и подписчик проекции карты зала.
//
// Карта зала — проекция (catalog_seat_state), а не чтение holds: booking публикует SeatStateChanged после commit,
// подписчик HandleSeatState обновляет проекцию и версию сектора в одной транзакции, затем инвалидирует кэш.
// Кэш хранит только проекцию с версией из БД и никогда не читается в пути hold (constitution 4, I1).
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sync"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"

	bookingapi "github.com/nikolaysavelev/soldout/internal/booking/api"
	"github.com/nikolaysavelev/soldout/internal/catalog/api"
	"github.com/nikolaysavelev/soldout/internal/catalog/domain"
	"github.com/nikolaysavelev/soldout/internal/platform/eventbus"
)

// Repository — хранилище каталога (реализация в adapters/pg).
type Repository interface {
	ListEvents(ctx context.Context) ([]domain.Event, error)
	GetEvent(ctx context.Context, id uuid.UUID) (domain.Event, error)
	GetSeat(ctx context.Context, id int64) (domain.Seat, error)
	GetSector(ctx context.Context, id uuid.UUID) (domain.Sector, error)
	SectorsByVenue(ctx context.Context, venueID uuid.UUID) ([]domain.Sector, error)
	SeatsBySector(ctx context.Context, sectorID uuid.UUID) ([]domain.Seat, error)
	VenueExists(ctx context.Context, id uuid.UUID) (bool, error)
	InsertEvent(ctx context.Context, e domain.Event) error
	UpdateSalesState(ctx context.Context, id uuid.UUID, state api.SalesState) error
	// проекция
	SectorCounts(ctx context.Context, eventID uuid.UUID) (map[uuid.UUID]domain.SectorCounts, error)
	SectorStates(ctx context.Context, eventID, sectorID uuid.UUID) ([]domain.SeatState, int64, error)
	// ApplySeatState обновляет проекцию и инкрементирует версию сектора в одной транзакции; возвращает sector_id и новую версию.
	ApplySeatState(ctx context.Context, eventID uuid.UUID, seatID int64, state api.SeatStatus) (uuid.UUID, int64, error)
}

// Cache — кэш карты сектора (реализация в adapters/valkey). Версия — из БД.
//
// Stale-while-revalidate: после инвалидации данные не удаляются, а помечаются устаревшими (в кэше хранится
// «желаемая» версия). Читатель получает устаревшую карту сразу (ETag — версия именно этих данных), а обновление
// идёт фоном одним запросом на сектор (singleflight). Под штормом это превращает тысячи промахов в один rebuild.
type Cache interface {
	// Get: ok=false — промах; stale=true — данные есть, но их версия меньше желаемой.
	Get(ctx context.Context, eventID, sectorID uuid.UUID) (payload []byte, version int64, ok, stale bool, err error)
	// Set записывает только если version >= версии в кэше (CAS).
	Set(ctx context.Context, eventID, sectorID uuid.UUID, payload []byte, version int64, ttl time.Duration) error
	// Invalidate поднимает желаемую версию до version: данные становятся stale, Set с меньшей версией отклоняются.
	Invalidate(ctx context.Context, eventID, sectorID uuid.UUID, version int64) error
}

// CacheMetrics — счётчик операций кэша (metrics.CacheOps).
type CacheMetrics interface{ Inc(op string) }

// Options — настройки.
type Options struct {
	CacheTTL        time.Duration
	CacheInvalidate bool // слом №1: false — кэш не инвалидируется, карта врёт до TTL
}

// Service — реализация api.Service.
type Service struct {
	repo    Repository
	cache   Cache
	opts    Options
	cm      CacheMetrics
	logger  *slog.Logger
	sf      singleflight.Group
	seats   sync.Map // sectorID → []domain.Seat: то же для сборки карты (1 000 строк не читаются из БД на каждый промах)
	refresh singleflight.Group
}

// New собирает сервис. cache может быть nil (без кэша).
func New(repo Repository, cache Cache, opts Options, cm CacheMetrics, logger *slog.Logger) *Service {
	if opts.CacheTTL <= 0 {
		opts.CacheTTL = 30 * time.Second
	}
	if cm == nil {
		cm = nopMetrics{}
	}
	return &Service{repo: repo, cache: cache, opts: opts, cm: cm, logger: logger}
}

var _ api.Service = (*Service)(nil)

type nopMetrics struct{}

func (nopMetrics) Inc(string) {}

// ListEvents — все мероприятия.
func (s *Service) ListEvents(ctx context.Context) ([]api.Event, error) {
	events, err := s.repo.ListEvents(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]api.Event, 0, len(events))
	for _, e := range events {
		out = append(out, e.ToAPI())
	}
	return out, nil
}

// GetEvent — мероприятие по id.
func (s *Service) GetEvent(ctx context.Context, id uuid.UUID) (api.Event, error) {
	e, err := s.repo.GetEvent(ctx, id)
	if err != nil {
		return api.Event{}, err
	}
	return e.ToAPI(), nil
}

// GetSeat — место по id.
func (s *Service) GetSeat(ctx context.Context, seatID int64) (api.Seat, error) {
	seat, err := s.repo.GetSeat(ctx, seatID)
	if err != nil {
		return api.Seat{}, err
	}
	return seat.ToAPI(), nil
}

// GetSector — сектор по id.
func (s *Service) GetSector(ctx context.Context, id uuid.UUID) (api.Sector, error) {
	sec, err := s.repo.GetSector(ctx, id)
	if err != nil {
		return api.Sector{}, err
	}
	return sec.ToAPI(), nil
}

// SeatsBySector — геометрия одного сектора (около 1 000 мест). Занятость сюда не входит.
// Места сектора не меняются во время дропа, поэтому читаем их один раз и держим в процессе.
func (s *Service) SeatsBySector(ctx context.Context, sectorID uuid.UUID) ([]api.Seat, error) {
	seats, err := s.sectorSeats(ctx, sectorID)
	if err != nil {
		return nil, err
	}
	out := make([]api.Seat, 0, len(seats))
	for _, seat := range seats {
		out = append(out, seat.ToAPI())
	}
	return out, nil
}

// SeatMap — сводка по секторам из проекции (один GROUP BY вместо 40 000 строк).
func (s *Service) SeatMap(ctx context.Context, eventID uuid.UUID) (api.SeatMap, error) {
	ev, err := s.repo.GetEvent(ctx, eventID)
	if err != nil {
		return api.SeatMap{}, err
	}
	sectors, err := s.repo.SectorsByVenue(ctx, ev.VenueID)
	if err != nil {
		return api.SeatMap{}, err
	}
	counts, err := s.repo.SectorCounts(ctx, eventID)
	if err != nil {
		return api.SeatMap{}, err
	}
	return domain.BuildSummary(eventID, sectors, counts), nil
}

// SectorSeatMap — карта сектора: cache-aside по версии из БД; промах — singleflight; stale — отдаём и обновляем фоном.
func (s *Service) SectorSeatMap(ctx context.Context, eventID, sectorID uuid.UUID) (api.SectorSeatMapResult, error) {
	if s.cache != nil {
		payload, version, ok, stale, err := s.cache.Get(ctx, eventID, sectorID)
		switch {
		case err != nil:
			s.cm.Inc("error")
			s.logger.WarnContext(ctx, "catalog: кэш недоступен, читаем БД", "err", err)
		case ok && !stale:
			s.cm.Inc("hit")
			return api.SectorSeatMapResult{JSON: payload, Version: version, Cached: true}, nil
		case ok && stale:
			s.cm.Inc("stale")
			s.refreshAsync(eventID, sectorID)
			return api.SectorSeatMapResult{JSON: payload, Version: version, Cached: true, Stale: true}, nil
		default:
			s.cm.Inc("miss")
		}
	}
	v, err, _ := s.sf.Do(eventID.String()+":"+sectorID.String(), func() (any, error) {
		return s.rebuild(ctx, eventID, sectorID)
	})
	if err != nil {
		return api.SectorSeatMapResult{}, err
	}
	return v.(api.SectorSeatMapResult), nil
}

// rebuild собирает карту из БД и кладёт в кэш (CAS по версии).
func (s *Service) rebuild(ctx context.Context, eventID, sectorID uuid.UUID) (api.SectorSeatMapResult, error) {
	res, err := s.buildSector(ctx, eventID, sectorID)
	if err != nil {
		return api.SectorSeatMapResult{}, err
	}
	if s.cache != nil {
		if err := s.cache.Set(ctx, eventID, sectorID, res.JSON, res.Version, s.opts.CacheTTL); err != nil {
			s.cm.Inc("error")
			s.logger.WarnContext(ctx, "catalog: запись в кэш не удалась", "err", err)
		}
	}
	return res, nil
}

// refreshAsync — одно фоновое обновление на сектор, остальные читатели получают stale-данные.
func (s *Service) refreshAsync(eventID, sectorID uuid.UUID) {
	key := eventID.String() + ":" + sectorID.String()
	go func() {
		_, _, _ = s.refresh.Do(key, func() (any, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			return s.rebuild(ctx, eventID, sectorID)
		})
	}()
}

func (s *Service) buildSector(ctx context.Context, eventID, sectorID uuid.UUID) (api.SectorSeatMapResult, error) {
	ev, err := s.repo.GetEvent(ctx, eventID)
	if err != nil {
		return api.SectorSeatMapResult{}, err
	}
	sec, err := s.repo.GetSector(ctx, sectorID)
	if err != nil {
		return api.SectorSeatMapResult{}, err
	}
	if sec.VenueID != ev.VenueID {
		return api.SectorSeatMapResult{}, api.ErrSectorNotFound
	}
	seats, err := s.sectorSeats(ctx, sectorID)
	if err != nil {
		return api.SectorSeatMapResult{}, err
	}
	states, version, err := s.repo.SectorStates(ctx, eventID, sectorID)
	if err != nil {
		return api.SectorSeatMapResult{}, err
	}
	sm := domain.BuildSectorSeatMap(eventID, sec, seats, states, version)
	payload, err := json.Marshal(sm)
	if err != nil {
		return api.SectorSeatMapResult{}, fmt.Errorf("catalog: marshal sector: %w", err)
	}
	return api.SectorSeatMapResult{JSON: payload, Version: version}, nil
}

// sectorSeats — места сектора из in-process кэша (неизменяемы).
func (s *Service) sectorSeats(ctx context.Context, sectorID uuid.UUID) ([]domain.Seat, error) {
	if v, ok := s.seats.Load(sectorID); ok {
		return v.([]domain.Seat), nil
	}
	seats, err := s.repo.SeatsBySector(ctx, sectorID)
	if err != nil {
		return nil, err
	}
	s.seats.Store(sectorID, seats)
	return seats, nil
}

// HandleSeatState — подписчик eventbus: проекция + версия сектора в транзакции, затем инвалидация кэша.
func (s *Service) HandleSeatState(ctx context.Context, e eventbus.Event) error {
	ev, ok := e.(bookingapi.SeatStateChanged)
	if !ok {
		return fmt.Errorf("catalog: неожиданное событие %T", e)
	}
	var state api.SeatStatus
	switch ev.State {
	case bookingapi.SeatStateHeld:
		state = api.SeatHeld
	case bookingapi.SeatStateSold:
		state = api.SeatSold
	default:
		state = api.SeatFree
	}
	sectorID, version, err := s.repo.ApplySeatState(ctx, ev.EventID, ev.SeatID, state)
	if err != nil {
		return err
	}
	if s.cache != nil && s.opts.CacheInvalidate {
		if err := s.cache.Invalidate(ctx, ev.EventID, sectorID, version); err != nil {
			s.cm.Inc("error")
			return fmt.Errorf("catalog: инвалидация кэша: %w", err)
		}
		s.cm.Inc("invalidate")
	}
	return nil
}

// CreateEvent — админ: создать мероприятие на существующей площадке.
func (s *Service) CreateEvent(ctx context.Context, in api.CreateEventInput) (api.Event, error) {
	e, err := domain.NewEvent(in)
	if err != nil {
		return api.Event{}, err
	}
	ok, err := s.repo.VenueExists(ctx, e.VenueID)
	if err != nil {
		return api.Event{}, err
	}
	if !ok {
		return api.Event{}, api.ErrVenueNotFound
	}
	if err := s.repo.InsertEvent(ctx, e); err != nil {
		return api.Event{}, err
	}
	return e.ToAPI(), nil
}

// OpenSales — админ: открыть продажи.
func (s *Service) OpenSales(ctx context.Context, id uuid.UUID) (api.Event, error) {
	e, err := s.repo.GetEvent(ctx, id)
	if err != nil {
		return api.Event{}, err
	}
	if err := e.Open(); err != nil {
		return api.Event{}, err
	}
	if err := s.repo.UpdateSalesState(ctx, e.ID, e.SalesState); err != nil {
		return api.Event{}, err
	}
	return e.ToAPI(), nil
}

// ErrNotSubscribed — защитная ошибка для сборки без шины.
var ErrNotSubscribed = errors.New("catalog: подписчик проекции не подключён")
