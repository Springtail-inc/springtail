## test

-- Create table with JSONB column
CREATE TABLE IF NOT EXISTS events (
    id serial PRIMARY KEY,
    payload jsonb NOT NULL
);

-- Insert sample rows
INSERT INTO events (payload) VALUES
('{"user": "alice", "action": "login", "ip": "10.0.0.1"}'),
('{"user": "bob", "action": "logout", "ip": "10.0.0.2"}'),
('{"user": "alice", "action": "purchase", "item": "book"}');

-- Create GIN index on JSONB column
CREATE INDEX idx_events_payload ON events(payload);

## verify

### index_exists public events idx_events_payload false

-- Query using the index
SELECT *
FROM events
WHERE payload @> '{"user": "alice"}';

## cleanup
DROP TABLE IF EXISTS events CASCADE;
