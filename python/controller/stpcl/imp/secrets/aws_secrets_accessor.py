# Description: Accessor for AWS Secrets Manager secrets with type safety serialization support.

from typing import Generic, Type, TypeVar, Optional, Callable

import orjson as json

T = TypeVar("T")


class AWSSecretsAccessor(Generic[T]):
    """
    Represents an accessor to a Secret entry in AWS Secrets Manager.
    Supported models include:
        - Primitive types: int, str, float, bool; their Optional variants
        - Dataclasses: Any dataclass with fields of supported types
        - List of supported types: list[int], list[str], list[YourDataclass], etc.
        - Dict of supported types: dict[str, int], dict[str, YourDataclass], etc.
    """

    def __init__(self, key: str, secretsmanager_client, data_class: Type[T]):
        self._key = key
        self._data_class = data_class
        self._client = secretsmanager_client

    def __str__(self, *args, **kwargs):
        secret_value = self.get()
        if secret_value is None:
            return None
        return json.loads(secret_value)

    def __call__(self, *args, **kwargs):
        return self.get()

    def get(self) -> T | None:
        try:
            resp = self._client.get_secret_value(SecretId=self._key)
        except self._client.exceptions.ResourceNotFoundException:
            return None
        else:
            rv = resp.get("SecretString", None)
            if self._data_class in [int, str, float, bool]:
                return self._data_class(rv)
            elif self._data_class == Optional[int]:
                return None if rv is None else int(rv)
            elif self._data_class == Optional[float]:
                return None if rv is None else float(rv)
            elif self._data_class == Optional[bool]:
                return None if rv is None else bool(rv)
            elif self._data_class == Optional[str]:
                return None if rv is None else str(rv)
            elif hasattr(self._data_class, "__dataclass_fields__"):
                return self._data_class(**json.loads(rv))
            elif (
                    hasattr(self._data_class, "__origin__")
                    and self._data_class.__origin__ == list
            ):
                item_type = self._data_class.__args__[0]
                return [
                    (
                        item_type(**item)
                        if hasattr(item_type, "__dataclass_fields__")
                        else item_type(item)
                    )
                    for item in json.loads(rv)
                ]
            elif (
                    hasattr(self._data_class, "__origin__")
                    and self._data_class.__origin__ == dict
            ):
                key_type, value_type = self._data_class.__args__
                return {
                    key_type(k): (
                        value_type(v)
                        if not hasattr(value_type, "__dataclass_fields__")
                        else value_type(**v)
                    )
                    for k, v in json.loads(rv).items()
                }

        return resp.get("SecretString", None)

    def commit(self, secret_value: T):
        """
        AWS Secrets Manager has rate limiting. Be cautious about how often you call this method.
        This is the reason why I did not make this a property setter, which triggers writes on every assignment.
        :param secret_value: Conforms to the type T used to instantiate this accessor.
        :return:
        """
        if self._data_class in [int, str, float, bool]:
            if not isinstance(secret_value, self._data_class):
                raise TypeError(f"secret_value must be of type {self._data_class}")
            value_to_store = str(secret_value)
        else:
            # Json serialize for complex types
            value_to_store = json.dumps(secret_value).decode("utf-8")
        try:
            self._client.put_secret_value(
                SecretId=self._key, SecretString=value_to_store
            )
        except self._client.exceptions.ResourceNotFoundException:
            self._client.create_secret(Name=self._key, SecretString=value_to_store)


def bind_secrets_model[T](
        t: Type[T], key_fn: Callable[[], str], client
) -> AWSSecretsAccessor[T]:
    """
    Factory function to create an AWSSecretsAccessor for a given type T.

    Later the bound secret can rewrite content via `commit`.

    :param t:
    :param key_fn:
    :param client:
    :return:
    """
    return AWSSecretsAccessor[T](
        key=key_fn(), secretsmanager_client=client, data_class=t
    )
