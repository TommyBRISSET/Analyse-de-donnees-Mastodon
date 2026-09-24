CREATE TABLE IF NOT EXISTS toots (
    instance text NOT NULL,
    id text NOT NULL,
    created_at timestamptz NOT NULL,
    text text NOT NULL,
    user_id text NOT NULL,
    username text,
    language text,
    hashtags text[] NOT NULL,
    location text,
    favourites_count bigint,
    reblogs_count bigint,
    text_length integer NOT NULL,
    PRIMARY KEY (instance, id)
);
CREATE INDEX IF NOT EXISTS toots_created_at_idx ON toots(created_at);
CREATE TABLE IF NOT EXISTS window_stats (
    window_start timestamptz PRIMARY KEY,
    window_end timestamptz NOT NULL,
    toot_count bigint NOT NULL
);
CREATE TABLE IF NOT EXISTS user_stats (
    instance text NOT NULL,
    user_id text NOT NULL,
    toot_count bigint NOT NULL,
    total_length bigint NOT NULL,
    avg_toot_length double precision GENERATED ALWAYS AS
        (total_length::double precision / NULLIF(toot_count, 0)) STORED,
    PRIMARY KEY (instance, user_id)
);
