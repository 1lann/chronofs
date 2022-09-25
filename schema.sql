CREATE TABLE IF NOT EXISTS files (
    file_id         BLOB          PRIMARY KEY,
    parent          BLOB          NOT NULL,
    name            VARCHAR(4096) NOT NULL,
    file_type       TINYINT       NOT NULL,
    length          INTEGER       NOT NULL,
    last_write_at   INTEGER, -- unix timestamp in millis
    last_access_at  INTEGER -- unix timestamp in millis
);

CREATE INDEX IF NOT EXISTS files_last_access_at ON files (last_access_at);
CREATE INDEX IF NOT EXISTS files_parent ON files (parent);

CREATE TABLE IF NOT EXISTS pages (
    file_id         BLOB          NOT NULL,
    page_num        INTEGER       NOT NULL,
    page_size_power TINYINT       NOT NULL,
    data            BLOB          NOT NULL,
    last_write_at   INTEGER, -- unix timestamp in millis
    last_read_at    INTEGER, -- unix timestamp in millis
    last_access_at INTEGER GENERATED ALWAYS AS (MAX(last_write_at, last_read_at)) VIRTUAL,
    PRIMARY KEY (file_id, page_num, page_size_power)
);

CREATE INDEX IF NOT EXISTS pages_last_access_at ON pages (last_access_at);
