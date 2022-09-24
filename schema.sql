CREATE TABLE files (
    filepath        VARCHAR(2048) PRIMARY KEY,
    length          INTEGER       NOT NULL
);

CREATE INDEX files_last_access_at ON files (last_access_at);

CREATE TABLE pages (
    filepath        VARCHAR(2048) NOT NULL,
    page_num        INTEGER       NOT NULL,
    page_size_power TINYINT       NOT NULL,
    data            BLOB          NOT NULL,
    last_write_at   INTEGER, -- unix timestamp in millis
    last_read_at    INTEGER, -- unix timestamp in millis
    last_acceess_at INTEGER GENERATED ALWAYS AS (GREATEST(last_write_at, last_read_at)) VIRTUAL
)

CREATE INDEX pages_last_access_at ON pages (last_access_at);
