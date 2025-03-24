import psycopg2
import logging
import sys
import os
import string
from random import SystemRandom
from typing import Optional
from psycopg2 import sql

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))

from common import (
    run_command
)

class PostgresHelper:
    """
    PostgresHelper class to help with Postgres database operations.
    """

    def __init__(self, is_production: bool = False):
        """
        Initialize the PostgresHelper with database connection parameters.
        """
        self.host = 'localhost'
        self.port = 5432
        self.conn : Optional[psycopg2.extensions.connection] = None
        self.cur : Optional[psycopg2.extensions.cursor] = None
        self.logger = logging.getLogger('springtail')

        if is_production:
            (self.user, self.password) = self._setup_production_postgres()
        else:
            (self.user, self.password) = self._setup_development_postgres()


    def _gen_random_string(self, length: int) -> str:
        """
        Generate a random string of the specified length.
        Arguments:
            length -- the length of the string
        Returns:
            a random string of the specified length
        """
        return ''.join(SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(length))


    def _setup_production_postgres(self) -> tuple[str, str]:
        """
        Setup Postgres connection parameters for production.

        Returns:
            tuple: (user, password)
        """
        # Change the password for the postgres user
        user = "postgres"
        password = self._gen_random_string(16)
        run_command('sudo', ['-u', 'postgres', 'psql', '-c', f"ALTER USER postgres WITH PASSWORD '{password}';"])

        return (user, password)


    def _setup_development_postgres(self) -> tuple[str, str]:
        """
        Setup Postgres connection parameters for development.

        Returns:
            tuple: (user, password)
        """
        return (os.environ.get('REPLICATION_USER', 'springtail'),
                os.environ.get('REPLICATION_USER_PASSWORD', 'springtail'))


    def create_fdw_user(self) -> tuple[str, str]:
        """
        Create the FDW user.
        """
        self.fdw_user = os.environ.get('FDW_USER', None)
        self.fdw_password = os.environ.get('FDW_USER_PASSWORD', None)

        if not self.fdw_user or not self.fdw_password:
            self.logger.error("FDW user and password not set")
            raise Exception("FDW user and password not set")

        try:
            self.create_user(self.fdw_user, self.fdw_password, False)
            return (self.fdw_user, self.fdw_password)
        finally:
            self.disconnect()


    def create_ddl_user(self) -> tuple[str, str]:
        """
        Create the DDL user.
        """
        self.ddl_user = 'ddl_user'
        self.ddl_password = self._gen_random_string(16)

        try:
            self.create_user(self.ddl_user, self.ddl_password, True)
            return (self.ddl_user, self.ddl_password)
        finally:
            self.disconnect()


    def connect(self, dbname) -> None:
        """
        Connect to the database.
        """
        if self.conn and self.cur and self.dbname == dbname:
            return

        self.disconnect()

        self.dbname = dbname
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )

            if not self.conn:
                self.logger.error("Failed to establish database connection")
                raise Exception("Failed to establish database connection")

            self.conn.autocommit = True
            self.cur = self.conn.cursor()

        except psycopg2.Error as e:
            self.logger.error(f"Could not connect to database: {e.pgerror}")
            raise e


    def disconnect(self) -> None:
        """
        Close the database connection.
        """
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        self.conn = None
        self.cur = None


    def create_user(self, user : str, password : str, superuser : bool = False) -> None:
        """
        Create a new user with the given password.

        If user exists, alter the password for the existing user to match the new password.
        """
        try:
            self.connect("template1")

            if self.user_exists(user):
                # alter password to new password
                self.cur.execute(
                    sql.SQL("ALTER USER {} WITH PASSWORD {};").format(
                        sql.Identifier(user),
                        sql.Literal(password)
                    )
                )
                self.logger.info(f"User '{user}' already exists, password updated.")
                return

            self.cur.execute(
                sql.SQL("CREATE USER {} WITH PASSWORD {} {};").format(
                    sql.Identifier(user),
                    sql.Literal(password),
                    sql.SQL(" WITH SUPERUSER" if superuser else "")
                )
            )
            self.logger.info(f"User '{user}' created successfully.")

        except psycopg2.Error as e:
            self.logger.error(f"Could not create user '{user}': {e.pgerror}")
            raise e


    def user_exists(self, user : str) -> bool:
        """
        Check if the user exists in the database.
        """
        try:
            self.connect("template1")
            self.cur.execute(
                sql.SQL("SELECT 1 FROM pg_roles WHERE rolname = {};").format(
                    sql.Literal(user)
                )
            )
            return self.cur.fetchone() is not None
        except psycopg2.Error as e:
            self.logger.error(f"Could not check if user '{user}' exists: {e.pgerror}")
            raise e

