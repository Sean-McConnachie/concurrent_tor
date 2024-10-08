
CREATE TYPE platform_enum AS ENUM (
    'GoogleRss'
);

CREATE TYPE job_status_enum AS ENUM (
    'Pending',
    'Success',
    'Failed'
);

CREATE TABLE job_cache (
    id SERIAL PRIMARY KEY,
    status job_status_enum NOT NULL,
    platform platform_enum NOT NULL,
    hash TEXT NOT NULL,
    num_attempts INT NOT NULL,
    request TEXT NOT NULL,

    UNIQUE (hash)
);

CREATE INDEX job_cache_hash_platform_idx ON job_cache (hash, platform);