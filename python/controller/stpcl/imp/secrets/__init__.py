from .models import *  # noqa: F403
from .aws_secrets_accessor import *  # noqa: F403


class Secrets:
    def __init__(
        self, org_id: int, account_id: int, instance_id: int, aws_secretsmanager_client
    ):
        """
        :param org_id: The organization ID to bind the secrets manager to.
        :param account_id: The account ID to bind the secrets manager to.
        :param instance_id: The DB instance ID to bind the secrets manager to.
        :param aws_secretsmanager_client:
        """
        self.client = aws_secretsmanager_client
        self.k_primary_db_credentials = (
            f"sk/{org_id}/{account_id}/aws/dbi/{instance_id}/primary_db_password"
        )

    @property
    def primary_db_credentials(self) -> AWSSecretsAccessor[PostgresCredentialList]:
        return bind_secrets_model(
            PostgresCredentialList, lambda: self.k_primary_db_credentials, self.client
        )
