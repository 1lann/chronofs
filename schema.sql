CREATE TABLE IF NOT EXISTS files (
    file_id         INTEGER       PRIMARY KEY AUTOINCREMENT,
    parent          INTEGER       NOT NULL,
    name            VARCHAR(4096) NOT NULL,
    file_type       TINYINT       NOT NULL,
    length          INTEGER       NOT NULL,
    link            VARCHAR(4096) NOT NULL, -- symlink target
    permissions     INTEGER       NOT NULL, -- unix permissions
    owner_id        INTEGER       NOT NULL, -- unix owner
    group_id        INTEGER       NOT NULL, -- unix group
    last_write_at   INTEGER       NOT NULL, -- unix timestamp in millis
    last_access_at  INTEGER       NOT NULL -- unix timestamp in millis
);

-- reserve inode numbers that have special meaning
INSERT OR IGNORE INTO files (file_id, parent, name, file_type, length, link, last_write_at, last_access_at) VALUES (0, 0, '__internal_stub_0', 0, 0, '', 0, 0);
INSERT OR IGNORE INTO files (file_id, parent, name, file_type, length, link, last_write_at, last_access_at) VALUES (1, 0, '__internal_stub_1', 0, 0, '', 0, 0);
INSERT OR IGNORE INTO files (file_id, parent, name, file_type, length, link, last_write_at, last_access_at) VALUES (2, 0, '__internal_root', 0, 0, '', 0, 0);

CREATE INDEX IF NOT EXISTS files_last_access_at ON files (last_access_at);
CREATE INDEX IF NOT EXISTS files_parent ON files (parent);

CREATE TABLE IF NOT EXISTS pages (
    file_id         INTEGER       NOT NULL,
    page_num        INTEGER       NOT NULL,
    page_size_power TINYINT       NOT NULL,
    data            BLOB          NOT NULL,
    PRIMARY KEY (file_id, page_num, page_size_power)
);

CREATE TABLE IF NOT EXISTS schema_version (
    one             INTEGER       PRIMARY KEY, -- always 1, so there's only 1 row
    version         INTEGER       NOT NULL     -- for migrations in the future, maybe?
);

INSERT OR IGNORE INTO schema_version (one, version) VALUES (1, 1);
