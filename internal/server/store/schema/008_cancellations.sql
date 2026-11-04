CREATE TABLE IF NOT EXISTS mw_cancellations (
 job_id BIGINT UNSIGNED NOT NULL,
 through_generation BIGINT UNSIGNED NOT NULL,
 PRIMARY KEY (job_id),
 FOREIGN KEY (job_id) REFERENCES mw_jobs (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
