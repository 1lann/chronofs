-- CREATE TABLE IF NOT EXISTS files (
--     file_id         BLOB          PRIMARY KEY,
--     parent          BLOB          NOT NULL,
--     name            VARCHAR(4096) NOT NULL,
--     file_type       TINYINT       NOT NULL,
--     length          INTEGER       NOT NULL,
--     last_write_at   INTEGER, -- unix timestamp in millis
--     last_access_at  INTEGER -- unix timestamp in millis
-- );

-- CREATE INDEX IF NOT EXISTS files_last_access_at ON files (last_access_at);
-- CREATE INDEX IF NOT EXISTS files_parent ON files (parent);

-- CREATE TABLE IF NOT EXISTS pages (
--     file_id         BLOB          NOT NULL,
--     page_num        INTEGER       NOT NULL,
--     page_size_power TINYINT       NOT NULL,
--     data            BLOB          NOT NULL,
--     last_write_at   INTEGER, -- unix timestamp in millis
--     last_read_at    INTEGER, -- unix timestamp in millis
--     last_acceess_at INTEGER GENERATED ALWAYS AS (GREATEST(last_write_at, last_read_at)) VIRTUAL,
--     PRIMARY KEY (file_id, page_num, page_size_power)
-- )

-- CREATE INDEX IF NOT EXISTS pages_last_access_at ON pages (last_access_at);

-- name: GetFile :one
SELECT * FROM files WHERE file_id = ?;

-- name: UpsertFileLength :exec
INSERT INTO files (file_id, length) VALUES (?, ?)
ON CONFLICT (file_id) DO UPDATE SET length = excluded.length;

-- name: CreateFile :exec
INSERT INTO files (file_id, file_type, length) VALUES (?, ?, ?);

-- name: UpsertPage :exec
INSERT INTO pages (file_id, page_num, page_size_power, last_write_at, data)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT (file_id, page_num, page_size_power) DO UPDATE SET
    last_write_at = excluded.last_write_at,
    data = excluded.data;

-- name: UpdatePageLastWriteAt :execrows
UPDATE pages SET last_write_at = ? WHERE file_id = ? AND page_num = ? AND page_size_power = ?;

-- name: UpdatePageLastReadAt :execrows
UPDATE pages SET last_read_at = ? WHERE file_id = ? AND page_num = ? AND page_size_power = ?;

-- name: GetPage :one
SELECT data FROM pages WHERE file_id = ? AND page_num = ? AND page_size_power = ?;

-- name: DeleteFile :execrows
DELETE FROM files WHERE file_id = ?;

-- name: DeletePages :execrows
DELETE FROM pages WHERE file_id = ?;

-- name: DeletePage :execrows
DELETE FROM pages WHERE file_id = ? AND page_num = ? AND page_size_power = ?;

-- name: RenameFile :exec
UPDATE files SET file_id = ?, parent = ? WHERE file_id = ?;

-- name: RenamePage :exec
UPDATE pages SET file_id = ? WHERE file_id = ?;

-- name: GetDirectoryFiles :many
SELECT * FROM files WHERE parent = ?;

-- name: GetFileInDirectory :one
SELECT * FROM files WHERE parent = ? AND name = ?;
