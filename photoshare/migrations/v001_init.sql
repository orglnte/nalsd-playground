CREATE TABLE IF NOT EXISTS photos (
    id          TEXT PRIMARY KEY,
    filename    TEXT NOT NULL,
    content_type TEXT NOT NULL,
    size_bytes  BIGINT NOT NULL,
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
