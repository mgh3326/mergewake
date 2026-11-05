CREATE TABLE IF NOT EXISTS mw_deliveries (
 source VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 delivery_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 repository VARCHAR(201) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 number BIGINT UNSIGNED NOT NULL,
 received_at DATETIME(6) NOT NULL,
 PRIMARY KEY (source,delivery_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
