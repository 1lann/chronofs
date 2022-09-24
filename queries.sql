-- CREATE TABLE files (
--     filepath        VARCHAR(2048) PRIMARY KEY,
--     length          INTEGER       NOT NULL
-- );

-- CREATE INDEX files_last_access_at ON files (last_access_at);

-- CREATE TABLE pages (
--     filepath        VARCHAR(2048) NOT NULL,
--     page_num        INTEGER       NOT NULL,
--     page_size_power TINYINT       NOT NULL,
--     data            BLOB          NOT NULL,
--     last_write_at   INTEGER, -- unix timestamp in millis
--     last_read_at    INTEGER, -- unix timestamp in millis
--     last_acceess_at INTEGER VIRTUAL ALWAYS AS (GREATEST(last_write_at, last_read_at)) STORED,
--     PRIMARY KEY (filepath, page_num, page_size_power)
-- )

-- CREATE INDEX pages_last_access_at ON pages (last_access_at);

-- name: GetFileLength :one
SELECT length FROM files WHERE filepath = ?;

-- name: UpsertFileLength :exec
INSERT INTO files (filepath, length) VALUES (?, ?)
ON CONFLICT (filepath) DO UPDATE SET length = excluded.length;

-- name: UpsertPage :exec
INSERT INTO pages (filepath, page_num, page_size_power, last_write_at, data)
VALUES (?, ?, ?, ?, ?);

-- name: UpdatePageLastWriteAt :exec
UPDATE pages SET last_write_at = ? WHERE filepath = ? AND page_num = ? AND page_size_power = ?;

-- name: UpdatePageLastReadAt :exec
UPDATE pages SET last_read_at = ? WHERE filepath = ? AND page_num = ? AND page_size_power = ?;

-- name: GetPage :one
SELECT data FROM pages WHERE filepath = ? AND page_num = ? AND page_size_power = ?;

-- name: DeleteFile :exec
DELETE FROM files WHERE filepath = ?;

-- name: DeletePages :exec
DELETE FROM pages WHERE filepath = ?;

-- name: DeletePage :exec
DELETE FROM pages WHERE filepath = ? AND page_num = ? AND page_size_power = ?;
