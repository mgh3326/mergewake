CREATE TABLE IF NOT EXISTS mw_attempts (
 job_id BIGINT UNSIGNED NOT NULL,
 epoch BIGINT UNSIGNED NOT NULL,
 request_uuid CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 status VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 created_at DATETIME(6) NOT NULL,
 PRIMARY KEY (job_id,epoch),
 UNIQUE KEY request_identity (request_uuid),
 FOREIGN KEY (job_id) REFERENCES mw_jobs (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
