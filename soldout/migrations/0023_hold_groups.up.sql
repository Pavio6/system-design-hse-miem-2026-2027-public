-- 0023: группа соседних мест (V3). Владелец — booking. 0022 на main уже занят catalog_seat_state_hold.
-- event_id и sector_id без FK: это идентификаторы catalog (конституция, раздел 5).

CREATE TABLE hold_groups (
    id              uuid        PRIMARY KEY,
    event_id        uuid        NOT NULL,
    user_id         uuid        NOT NULL,
    sector_id       uuid        NOT NULL,
    row_no          int         NOT NULL,
    seat_from       int         NOT NULL,
    seat_count      int         NOT NULL CHECK (seat_count BETWEEN 1 AND 4),
    idempotency_key text        UNIQUE,
    expires_at      timestamptz NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE hold_groups IS 'booking: атомарное удержание непрерывного отрезка мест в одном ряду (V3, I7)';

ALTER TABLE holds ADD COLUMN group_id uuid REFERENCES hold_groups (id);
COMMENT ON COLUMN holds.group_id IS 'booking: группа соседних мест, NULL у одиночного hold';
CREATE INDEX holds_group_id_idx ON holds (group_id) WHERE group_id IS NOT NULL;
