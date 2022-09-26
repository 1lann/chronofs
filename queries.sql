-- CREATE TABLE IF NOT EXISTS files (
--     file_id         INTEGER       PRIMARY KEY AUTOINCREMENT,
--     parent          INTEGER       NOT NULL,
--     name            VARCHAR(4096) NOT NULL,
--     file_type       TINYINT       NOT NULL,
--     length          INTEGER       NOT NULL,
--     last_write_at   INTEGER       NOT NULL, -- unix timestamp in millis
--     last_access_at  INTEGER       NOT NULL -- unix timestamp in millis
-- );

-- INSERT OR IGNORE INTO files (file_id, parent, name, file_type, length, last_write_at, last_access_at) VALUES (0, 0, '__internal_stub_1', 0, 0, 0, 0);
-- INSERT OR IGNORE INTO files (file_id, parent, name, file_type, length, last_write_at, last_access_at) VALUES (1, 0, '__internal_stub_2', 0, 0, 0, 0);
-- INSERT OR IGNORE INTO files (file_id, parent, name, file_type, length, last_write_at, last_access_at) VALUES (2, 0, '__internal_root', 0, 0, 0, 0);

-- CREATE INDEX IF NOT EXISTS files_last_access_at ON files (last_access_at);
-- CREATE INDEX IF NOT EXISTS files_parent ON files (parent);

-- CREATE TABLE IF NOT EXISTS pages (
--     file_id         INTEGER       NOT NULL,
--     page_num        INTEGER       NOT NULL,
--     page_size_power TINYINT       NOT NULL,
--     data            BLOB          NOT NULL
--     PRIMARY KEY (file_id, page_num, page_size_power)
-- );

-- CREATE INDEX IF NOT EXISTS pages_last_access_at ON pages (last_access_at);

-- name: GetFile :one
SELECT * FROM files WHERE file_id = ?;

-- name: UpdateFile :exec
UPDATE files SET parent = ?, name = ?, file_type = ?, length = ?, last_write_at = ?, last_access_at = ? WHERE file_id = ?;

-- name: RenameFile :execrows
UPDATE files SET parent = ?, name = ? WHERE file_id = ?;

-- name: CreateFile :one
INSERT INTO files (parent, name, file_type, length, last_write_at, last_access_at) VALUES (?, ?, ?, ?, ?, ?)
RETURNING file_id;

-- name: UpsertPage :exec
INSERT INTO pages (file_id, page_num, page_size_power, data)
VALUES (?, ?, ?, ?)
ON CONFLICT (file_id, page_num, page_size_power) DO UPDATE SET
    data = excluded.data;

-- name: GetPage :one
SELECT data FROM pages WHERE file_id = ? AND page_num = ? AND page_size_power = ?;

-- name: DeleteFile :execrows
DELETE FROM files WHERE file_id = ?;

-- name: DeletePage :execrows
DELETE FROM pages WHERE file_id = ? AND page_num = ? AND page_size_power = ?;

-- name: GetDirectoryFiles :many
SELECT * FROM files WHERE parent = ?;

-- name: GetFileInDirectory :one
SELECT * FROM files WHERE parent = ? AND name = ?;
