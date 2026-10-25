package domain

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/nikolaysavelev/soldout/internal/booking/api"
)

// SeatRef — место так, как его видит booking: без обращения к таблицам catalog.
type SeatRef struct {
	ID     int64
	RowNo  int
	SeatNo int
}

// HoldGroup — атомарная группа соседних мест.
type HoldGroup struct {
	ID             uuid.UUID
	EventID        uuid.UUID
	UserID         uuid.UUID
	SectorID       uuid.UUID
	RowNo          int
	SeatFrom       int
	Count          int
	IdempotencyKey string
	ExpiresAt      time.Time
	CreatedAt      time.Time
}

// ContiguousRuns возвращает непересекающиеся свободные отрезки длины n.
// Порядок: меньший ряд, внутри ряда меньший номер места. Ряд 0 (танцпол) пропускается.
// Занятость передаёт booking: catalog о ней не знает.
func ContiguousRuns(seats []SeatRef, taken map[int64]struct{}, n int) [][]SeatRef {
	if n < 1 || n > api.MaxTicketsPerUser || len(seats) < n {
		return nil
	}
	sorted := append([]SeatRef(nil), seats...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].RowNo != sorted[j].RowNo {
			return sorted[i].RowNo < sorted[j].RowNo
		}
		if sorted[i].SeatNo != sorted[j].SeatNo {
			return sorted[i].SeatNo < sorted[j].SeatNo
		}
		return sorted[i].ID < sorted[j].ID
	})

	var runs [][]SeatRef
	i := 0
	for i < len(sorted) {
		row := sorted[i].RowNo
		var rowSeats []SeatRef
		for i < len(sorted) && sorted[i].RowNo == row {
			rowSeats = append(rowSeats, sorted[i])
			i++
		}
		if row <= 0 {
			continue
		}
		start := 0
		for start < len(rowSeats) {
			if _, busy := taken[rowSeats[start].ID]; busy {
				start++
				continue
			}
			run := []SeatRef{rowSeats[start]}
			for k := start + 1; k < len(rowSeats) && len(run) < n; k++ {
				cur := rowSeats[k]
				if _, busy := taken[cur.ID]; busy {
					break
				}
				if cur.SeatNo != run[len(run)-1].SeatNo+1 {
					break
				}
				run = append(run, cur)
			}
			if len(run) == n {
				runs = append(runs, append([]SeatRef(nil), run...))
				endNo := run[len(run)-1].SeatNo
				for start < len(rowSeats) && rowSeats[start].SeatNo <= endNo {
					start++
				}
				continue
			}
			start++
		}
	}
	return runs
}

// NewAdjacentHolds собирает группу и N hold. Места уже выбраны: один ряд, номера подряд, N в пределах лимита.
func NewAdjacentHolds(eventID, userID, sectorID uuid.UUID, seats []SeatRef, now time.Time, ttl time.Duration) (HoldGroup, []Hold, error) {
	if eventID == uuid.Nil || userID == uuid.Nil || sectorID == uuid.Nil {
		return HoldGroup{}, nil, fmt.Errorf("%w: event_id, user_id и sector_id обязательны", api.ErrValidation)
	}
	if len(seats) < 1 || len(seats) > api.MaxTicketsPerUser {
		return HoldGroup{}, nil, fmt.Errorf("%w: число мест от 1 до %d", api.ErrValidation, api.MaxTicketsPerUser)
	}
	if ttl <= 0 {
		return HoldGroup{}, nil, fmt.Errorf("%w: некорректный ttl", api.ErrValidation)
	}
	row := seats[0].RowNo
	if row <= 0 {
		return HoldGroup{}, nil, api.ErrStandingSector
	}
	seen := make(map[int64]struct{}, len(seats))
	for i, seat := range seats {
		if seat.ID <= 0 {
			return HoldGroup{}, nil, fmt.Errorf("%w: место без id", api.ErrValidation)
		}
		if _, ok := seen[seat.ID]; ok {
			return HoldGroup{}, nil, fmt.Errorf("%w: место %d повторяется", api.ErrValidation, seat.ID)
		}
		seen[seat.ID] = struct{}{}
		if seat.RowNo != row {
			return HoldGroup{}, nil, fmt.Errorf("%w: места не в одном ряду", api.ErrValidation)
		}
		if i > 0 && seat.SeatNo != seats[i-1].SeatNo+1 {
			return HoldGroup{}, nil, fmt.Errorf("%w: номера мест не подряд", api.ErrValidation)
		}
	}
	groupID := uuid.New()
	group := HoldGroup{
		ID: groupID, EventID: eventID, UserID: userID, SectorID: sectorID,
		RowNo: row, SeatFrom: seats[0].SeatNo, Count: len(seats),
		ExpiresAt: now.Add(ttl), CreatedAt: now,
	}
	holds := make([]Hold, 0, len(seats))
	for _, seat := range seats {
		h := NewHold(eventID, seat.ID, userID, now, ttl)
		h.GroupID = groupID
		holds = append(holds, h)
	}
	return group, holds, nil
}

// ToAPI собирает ответ из группы и hold в порядке мест.
func (g HoldGroup) ToAPI(holds []Hold) api.HoldGroup {
	out := api.HoldGroup{
		ID: g.ID, EventID: g.EventID, SectorID: g.SectorID, UserID: g.UserID,
		RowNo: g.RowNo, SeatFrom: g.SeatFrom, Count: g.Count,
		ExpiresAt: g.ExpiresAt, CreatedAt: g.CreatedAt,
		HoldIDs: make([]uuid.UUID, 0, len(holds)),
		SeatIDs: make([]int64, 0, len(holds)),
	}
	for _, h := range holds {
		out.HoldIDs = append(out.HoldIDs, h.ID)
		out.SeatIDs = append(out.SeatIDs, h.SeatID)
	}
	return out
}
