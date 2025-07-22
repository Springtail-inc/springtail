## test
DROP ROLE IF EXISTS sync_role;

DROP ROLE john_logan;

CREATE ROLE sync_role;

## verify
### role_exists sync_role
### role_exists alice_susan
### role_exists john_logan -1
### policy_check public document_contents read_all_documents
### schema_check public document_contents

## cleanup
