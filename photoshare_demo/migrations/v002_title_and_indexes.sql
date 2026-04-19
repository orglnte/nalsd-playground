ALTER TABLE photos ADD COLUMN IF NOT EXISTS title TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_photos_created ON photos (uploaded_at DESC);
CREATE INDEX IF NOT EXISTS idx_photos_title ON photos USING gin (to_tsvector('english', title));
