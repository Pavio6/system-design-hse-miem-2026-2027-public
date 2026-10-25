package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nikolaysavelev/soldout/internal/booking/api"
)

func seatsRow(row, from, to int, idBase int64) []SeatRef {
	out := make([]SeatRef, 0, to-from+1)
	for n := from; n <= to; n++ {
		out = append(out, SeatRef{ID: idBase + int64(n), RowNo: row, SeatNo: n})
	}
	return out
}

func TestContiguousRuns_PrefersFirstRowAndSkipsGap(t *testing.T) {
	seats := append(seatsRow(2, 1, 6, 100), seatsRow(1, 1, 6, 0)...)
	taken := map[int64]struct{}{2: {}} // ряд 1, место 2 (id = 0+2)
	runs := ContiguousRuns(seats, taken, 4)
	if len(runs) < 1 {
		t.Fatal("ожидался хотя бы один отрезок")
	}
	if runs[0][0].RowNo != 1 || runs[0][0].SeatNo != 3 || runs[0][3].SeatNo != 6 {
		t.Fatalf("первый отрезок = ряд %d места %d..%d", runs[0][0].RowNo, runs[0][0].SeatNo, runs[0][3].SeatNo)
	}
}

func TestContiguousRuns_DoesNotCrossRowsOrStanding(t *testing.T) {
	seats := append(seatsRow(1, 49, 50, 0), seatsRow(2, 1, 2, 100)...)
	seats = append(seats, seatsRow(0, 1, 10, 200)...)
	if runs := ContiguousRuns(seats, nil, 4); len(runs) != 0 {
		t.Fatalf("ряд нельзя склеивать и танцпол не подходит, получено %d", len(runs))
	}
	row := seatsRow(1, 1, 8, 0)
	runs := ContiguousRuns(row, nil, 4)
	if len(runs) != 2 || runs[0][0].SeatNo != 1 || runs[1][0].SeatNo != 5 {
		t.Fatalf("непересекающиеся отрезки 1..4 и 5..8, получено %+v", runs)
	}
}

func TestNewAdjacentHolds_Rules(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	event, user, sector := uuid.New(), uuid.New(), uuid.New()
	seats := []SeatRef{{ID: 3, RowNo: 1, SeatNo: 3}, {ID: 4, RowNo: 1, SeatNo: 4}, {ID: 5, RowNo: 1, SeatNo: 5}}
	g, holds, err := NewAdjacentHolds(event, user, sector, seats, now, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if g.Count != 3 || g.RowNo != 1 || g.SeatFrom != 3 || len(holds) != 3 {
		t.Fatalf("группа %+v, hold %d", g, len(holds))
	}
	for _, h := range holds {
		if h.GroupID != g.ID || h.UserID != user || h.Status != api.HoldActive {
			t.Fatalf("hold не в группе: %+v", h)
		}
	}
	apiGroup := g.ToAPI(holds)
	if len(apiGroup.HoldIDs) != 3 || apiGroup.SeatIDs[0] != 3 || apiGroup.SeatIDs[2] != 5 {
		t.Fatalf("ответ %+v", apiGroup)
	}

	broken := []SeatRef{{ID: 1, RowNo: 1, SeatNo: 1}, {ID: 2, RowNo: 1, SeatNo: 3}}
	if _, _, err := NewAdjacentHolds(event, user, sector, broken, now, time.Minute); !errors.Is(err, api.ErrValidation) {
		t.Fatalf("дырка должна быть ошибкой валидации, получено %v", err)
	}
	standing := []SeatRef{{ID: 1, RowNo: 0, SeatNo: 1}, {ID: 2, RowNo: 0, SeatNo: 2}}
	if _, _, err := NewAdjacentHolds(event, user, sector, standing, now, time.Minute); !errors.Is(err, api.ErrStandingSector) {
		t.Fatalf("танцпол должен отклоняться, получено %v", err)
	}
	if _, _, err := NewAdjacentHolds(event, user, sector, seatsRow(1, 1, 5, 0), now, time.Minute); !errors.Is(err, api.ErrValidation) {
		t.Fatalf("5 мест сверх лимита, получено %v", err)
	}
}
