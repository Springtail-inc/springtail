## test
CREATE ROLE test_role1 WITH LOGIN;

DROP ROLE IF EXISTS test_role1;

CREATE ROLE test_role2 WITH LOGIN BYPASSRLS INHERIT;

CREATE ROLE test_role3 WITH SUPERUSER;

-- The order matters, we can't wait for the non-existence of a role,
-- so we wait for the existence of a role.
## verify
### role_exists test_role3 true 30
### role_exists test_role2 true
### role_exists test_role1 false 0

## cleanup
DROP ROLE IF EXISTS test_role2;
DROP ROLE IF EXISTS test_role3;
