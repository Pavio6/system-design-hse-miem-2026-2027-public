package app

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"

	"github.com/nikolaysavelev/soldout/internal/booking/api"
	"github.com/nikolaysavelev/soldout/internal/booking/domain"
	catalogapi "github.com/nikolaysavelev/soldout/internal/catalog/api"
	queueapi "github.com/nikolaysavelev/soldout/internal/queue/api"
)

// maxAdjacentAttempts — сколько первых свободных отрезков пробуем, прежде чем ответить 409.
// Каждая попытка — своя транзакция: конфликт откатывает все N hold.
const maxAdjacentAttempts = 64

// errSegmentBusy — отрезок заняли между чтением и блокировкой. Не ошибка API: берём следующий.
var errSegmentBusy = errors.New("booking: отрезок уже занят")

// CreateAdjacentHolds — FR-V3: N соседних мест в сидячем секторе, все или ничего.
func (s *Service) CreateAdjacentHolds(ctx context.Context, in api.CreateAdjacentHoldsInput) (api.HoldGroup, bool, error) {
	if in.EventID == uuid.Nil || in.UserID == uuid.Nil || in.SectorID == uuid.Nil {
		return api.HoldGroup{}, false, fmt.Errorf("%w: event_id, sector_id и user_id обязательны", api.ErrValidation)
	}
	if in.Count < 1 || in.Count > api.MaxTicketsPerUser {
		return api.HoldGroup{}, false, fmt.Errorf("%w: count от 1 до %d", api.ErrValidation, api.MaxTicketsPerUser)
	}
	if in.IdempotencyKey != "" {
		existing, err := s.store.FindGroupByIdempotencyKey(ctx, in.IdempotencyKey)
		if err == nil {
			return s.replayGroup(ctx, in, existing)
		}
		if !errors.Is(err, api.ErrGroupNotFound) {
			return api.HoldGroup{}, false, err
		}
	}

	ev, err := s.catalog.GetEvent(ctx, in.EventID)
	if err != nil {
		return api.HoldGroup{}, false, err
	}
	if ev.SalesState != catalogapi.SalesOpen {
		return api.HoldGroup{}, false, fmt.Errorf("%w: состояние %s", api.ErrSalesClosed, ev.SalesState)
	}
	sector, err := s.catalog.GetSector(ctx, in.SectorID)
	if err != nil {
		return api.HoldGroup{}, false, err
	}
	if sector.VenueID != ev.VenueID {
		return api.HoldGroup{}, false, api.ErrSectorNotInEvent
	}
	if sector.Kind != catalogapi.SectorSeated {
		return api.HoldGroup{}, false, api.ErrStandingSector
	}
	if err := s.queue.Validate(ctx, in.AdmissionToken, in.EventID, in.UserID); err != nil {
		if errors.Is(err, queueapi.ErrAdmissionInvalid) {
			return api.HoldGroup{}, false, fmt.Errorf("%w: %v", api.ErrAdmissionRequired, err)
		}
		return api.HoldGroup{}, false, err
	}

	seats, err := s.catalog.SeatsBySector(ctx, in.SectorID)
	if err != nil {
		return api.HoldGroup{}, false, err
	}
	refs := make([]domain.SeatRef, len(seats))
	ids := make([]int64, len(seats))
	for i, seat := range seats {
		refs[i] = domain.SeatRef{ID: seat.ID, RowNo: seat.RowNo, SeatNo: seat.SeatNo}
		ids[i] = seat.ID
	}

	for attempt := 0; attempt < maxAdjacentAttempts; attempt++ {
		taken, err := s.store.TakenSeatIDs(ctx, in.EventID, ids)
		if err != nil {
			return api.HoldGroup{}, false, err
		}
		runs := domain.ContiguousRuns(refs, taken, in.Count)
		if len(runs) == 0 {
			return api.HoldGroup{}, false, api.ErrNoAdjacentSeats
		}
		// Не стоим в очереди на первом отрезке: если его уже берут, пробуем следующие
		// свободные из того же снимка. Так горячий ряд не сериализует все запросы.
		nruns := len(runs)
		if nruns > 8 {
			nruns = 8
		}
		var group domain.HoldGroup
		var holds []domain.Hold
		busy := false
		for i := 0; i < nruns; i++ {
			group, holds, err = s.holdSegment(ctx, in, runs[i])
			if errors.Is(err, errSegmentBusy) {
				busy = true
				continue
			}
			busy = false
			break
		}
		if busy {
			continue
		}
		if errors.Is(err, api.ErrIdempotencyConflict) && in.IdempotencyKey != "" {
			existing, findErr := s.store.FindGroupByIdempotencyKey(ctx, in.IdempotencyKey)
			if findErr != nil {
				return api.HoldGroup{}, false, findErr
			}
			return s.replayGroup(ctx, in, existing)
		}
		if err != nil {
			return api.HoldGroup{}, false, err
		}
		for _, h := range holds {
			s.publishSeat(ctx, h.EventID, h.SeatID, api.SeatStateHeld, h.ID)
		}
		return group.ToAPI(holds), true, nil
	}
	return api.HoldGroup{}, false, api.ErrNoAdjacentSeats
}

func (s *Service) replayGroup(ctx context.Context, in api.CreateAdjacentHoldsInput, g domain.HoldGroup) (api.HoldGroup, bool, error) {
	if g.UserID != in.UserID || g.EventID != in.EventID || g.SectorID != in.SectorID || g.Count != in.Count {
		return api.HoldGroup{}, false, api.ErrIdempotencyConflict
	}
	holds, err := s.store.HoldsByGroup(ctx, g.ID)
	if err != nil {
		return api.HoldGroup{}, false, err
	}
	ordered, err := s.orderHoldsBySeat(ctx, holds)
	if err != nil {
		return api.HoldGroup{}, false, err
	}
	if len(ordered) != g.Count {
		return api.HoldGroup{}, false, fmt.Errorf("booking: в группе %s %d hold, ожидалось %d", g.ID, len(ordered), g.Count)
	}
	return g.ToAPI(ordered), false, nil
}

func (s *Service) orderHoldsBySeat(ctx context.Context, holds []domain.Hold) ([]domain.Hold, error) {
	type numbered struct {
		h   domain.Hold
		num int
	}
	items := make([]numbered, len(holds))
	for i, h := range holds {
		seat, err := s.catalog.GetSeat(ctx, h.SeatID)
		if err != nil {
			return nil, err
		}
		items[i] = numbered{h: h, num: seat.SeatNo}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].num < items[j].num })
	out := make([]domain.Hold, len(items))
	for i := range items {
		out[i] = items[i].h
	}
	return out, nil
}

// holdSegment удерживает один уже выбранный отрезок. Конфликт мест — errSegmentBusy, транзакция откатана.
func (s *Service) holdSegment(ctx context.Context, in api.CreateAdjacentHoldsInput, seats []domain.SeatRef) (domain.HoldGroup, []domain.Hold, error) {
	var group domain.HoldGroup
	var holds []domain.Hold
	err := s.store.InTx(ctx, func(tx Tx) error {
		if err := tx.LockUser(ctx, in.EventID, in.UserID); err != nil {
			return err
		}
		n, err := tx.CountUserHolds(ctx, in.EventID, in.UserID)
		if err != nil {
			return err
		}
		if n+len(seats) > api.MaxTicketsPerUser {
			return fmt.Errorf("%w: уже %d", api.ErrHoldLimit, n)
		}
		locked := append([]domain.SeatRef(nil), seats...)
		sort.Slice(locked, func(i, j int) bool { return locked[i].ID < locked[j].ID })
		for _, seat := range locked {
			ok, err := tx.TryLockSeat(ctx, in.EventID, seat.ID)
			if err != nil {
				return err
			}
			if !ok {
				return errSegmentBusy
			}
		}
		for _, seat := range locked {
			taken, err := tx.SeatTaken(ctx, in.EventID, seat.ID)
			if err != nil {
				return err
			}
			if taken {
				return errSegmentBusy
			}
		}
		group, holds, err = domain.NewAdjacentHolds(in.EventID, in.UserID, in.SectorID, seats, s.now(), s.opts.HoldTTL)
		if err != nil {
			return err
		}
		group.IdempotencyKey = in.IdempotencyKey
		if err := tx.InsertGroup(ctx, group); err != nil {
			return err
		}
		for _, h := range holds {
			if err := tx.InsertHold(ctx, h); err != nil {
				if errors.Is(err, api.ErrSeatHeld) {
					return errSegmentBusy
				}
				return err
			}
		}
		return nil
	})
	if err != nil {
		return domain.HoldGroup{}, nil, err
	}
	return group, holds, nil
}
