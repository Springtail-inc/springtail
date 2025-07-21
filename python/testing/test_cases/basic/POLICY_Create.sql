## test
DROP POLICY IF EXISTS read_own_documents ON documents;
DROP ROLE IF EXISTS alice;

-- Create test table
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    content TEXT,
    owner TEXT
);

-- Insert test data
INSERT INTO documents (content, owner) VALUES
('Doc 1', 'alice'),
('Doc 2', 'bob'),
('Doc 3', 'alice'),
('Doc 4', 'charlie');

-- Create test user
CREATE ROLE alice LOGIN;

-- Enable row-level security on the table
ALTER TABLE documents ENABLE ROW LEVEL SECURITY;

-- Create RLS policy allowing a user to read only their own rows
CREATE POLICY read_own_documents
    ON documents
    FOR SELECT
    TO alice
    USING (owner = current_user);

## verify
### role_exists alice
### policy_check public documents read_own_documents
### schema_check public documents

## cleanup
DROP POLICY IF EXISTS read_own_documents ON documents;
DROP TABLE IF EXISTS documents;
DROP ROLE IF EXISTS alice;
