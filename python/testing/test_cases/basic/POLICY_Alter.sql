## test
DROP POLICY IF EXISTS read_own_documents ON documents;
DROP ROLE IF EXISTS alice;
DROP ROLE IF EXISTS joe;
DROP ROLE IF EXISTS sync_point_role;

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

-- Enable row-level security on the table
ALTER TABLE documents ENABLE ROW LEVEL SECURITY;

CREATE ROLE alice LOGIN;

-- Create RLS policy allowing a user to read only their own rows
CREATE POLICY read_own_documents
    ON documents
    FOR SELECT
    TO alice
    USING (owner = current_user);

### policy_sync read_own_documents

CREATE ROLE joe BYPASSRLS;

ALTER POLICY read_own_documents
    ON documents
    TO joe
    USING (owner = current_user AND id > 2);

CREATE ROLE sync_point_role;

## verify
### role_exists sync_point_role
### role_exists alice
### role_exists joe
### policy_check public documents read_own_documents
### schema_check public documents

## cleanup
DROP POLICY IF EXISTS read_own_documents ON documents;
DROP TABLE IF EXISTS documents;
DROP ROLE IF EXISTS alice;
DROP ROLE IF EXISTS joe;
DROP ROLE IF EXISTS sync_point_role;