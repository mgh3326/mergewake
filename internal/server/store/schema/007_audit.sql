CREATE TABLE IF NOT EXISTS mw_audit (
 id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
 repository VARCHAR(201) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 number BIGINT UNSIGNED NOT NULL,
 version BIGINT UNSIGNED NOT NULL,
 event_kind VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 occurred_at DATETIME(6) NOT NULL,
 PRIMARY KEY (id),
 KEY pr_history (repository,number,id),
 FOREIGN KEY (repository,number) REFERENCES mw_prs (repository,number)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
