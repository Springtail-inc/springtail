## setup
-- Create test table
CREATE TABLE document_contents (
    id SERIAL PRIMARY KEY,
    content TEXT,
    owner TEXT
);

-- Insert test data
INSERT INTO document_contents (content, owner) VALUES
('Doc 1', 'alice'),
('Doc 2', 'bob'),
('Doc 3', 'alice'),
('Doc 4', 'charlie');

-- Create test user
CREATE ROLE alice_susan LOGIN;
CREATE ROLE john_logan LOGIN BYPASSRLS;
CREATE ROLE membership_role;
GRANT membership_role TO alice_susan;

-- Enable row-level security on the table
ALTER TABLE document_contents ENABLE ROW LEVEL SECURITY;

-- Create RLS policy allowing a user to read only their own rows
CREATE POLICY read_all_documents
    ON document_contents
    FOR SELECT
    TO alice_susan
    USING (owner = current_user);

## cleanup
DROP POLICY IF EXISTS read_all_documents ON document_contents;
DROP TABLE IF EXISTS document_contents;
DROP ROLE IF EXISTS alice_susan;
DROP ROLE IF EXISTS john_logan;
DROP ROLE IF EXISTS membership_role;
