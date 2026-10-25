DROP INDEX IF EXISTS holds_group_id_idx;
ALTER TABLE holds DROP COLUMN IF EXISTS group_id;
DROP TABLE IF EXISTS hold_groups;
