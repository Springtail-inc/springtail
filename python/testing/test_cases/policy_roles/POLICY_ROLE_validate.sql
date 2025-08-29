## test
DROP ROLE IF EXISTS sync_role;

DROP ROLE john_logan;
REVOKE membership_role FROM alice_susan;

CREATE ROLE sync_role;

## verify
### role_check sync_role
### role_check alice_susan
### role_check john_logan -1
### policy_check public document_contents read_all_documents
### schema_check public document_contents

## cleanup
