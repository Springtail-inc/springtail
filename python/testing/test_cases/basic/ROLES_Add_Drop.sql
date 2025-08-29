## test
CREATE ROLE test_role1 WITH LOGIN;

DROP ROLE IF EXISTS test_role1;

CREATE ROLE test_role2 WITH LOGIN BYPASSRLS INHERIT;

-- Create test role for membership check
CREATE ROLE test_member_role NOLOGIN;
GRANT test_member_role TO test_role2;

CREATE ROLE test_role3 WITH SUPERUSER;

-- The order matters, we can't wait for the non-existence of a role,
-- so we wait for the existence of a role.
## verify
### role_check test_role3
### role_check test_role2
### role_check test_role1 -1

## cleanup
DROP ROLE IF EXISTS test_role2;
DROP ROLE IF EXISTS test_role3;
DROP ROLE IF EXISTS test_member_role;