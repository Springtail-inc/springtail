# Description: A proxy class to access a Redis hash as a dataclass/dict. Getting and setting values are simply
# done via attribute operations.

from typing import Generic, Type, TypeVar, Optional, Union, Callable

import orjson as json
import redis

T = TypeVar("T")


class HProxy(Generic[T]):
    """
    A proxy class to access a Redis Hash Map as a dataclass/dict.
    Getting and setting values are simply done via attribute operations.
    E.g.,
        # To retrieve values
        obj.hostname_proxy
        obj["hostname_proxy"]

        # To set values
        obj["hostname:proxy"] = "value"

        # To retrieve all values
        obj()
    """

    def __init__(self, key: str, client: redis.Redis, data_class: Type[T]):
        self._data_class = data_class
        self._client = client
        self._key = key

    def __call__(self, *args, **kwargs):
        return self._client.hgetall(self._key)

    def __setattr__(self, name, value):
        if name in ["_data_class", "_client", "_key"]:
            super().__setattr__(name, value)
            return
        prop_name, key_name, is_dict = self._extract_prop_and_key_names(name)
        if isinstance(prop_name, str) and prop_name.startswith("_"):
            raise AttributeError(f"Cannot set attribute {name}")
        if is_dict:
            if value and hasattr(value, "__dataclass_fields__"):
                value = json.dumps(value.__dict__)
            elif value and isinstance(value, dict) or isinstance(value, list):
                value = json.dumps(value)
            elif value and isinstance(value, set):
                value = json.dumps(sorted(list(value)))
            self._client.hset(self._key, key_name, value)
            return

        # Get the annotated type of the attribute
        t = self._data_class.__annotations__[prop_name]
        # check if the value is of the correct type
        eventual_value = value
        if t in [int, str, float, bool]:
            if not isinstance(value, t):
                raise TypeError(f"{name} must be of type {t}")
        elif t == Optional[int]:
            if value is not None and not isinstance(value, int):
                raise TypeError(f"{name} must be of type {t}")
        elif t == Optional[str]:
            if value is not None and not isinstance(value, str):
                raise TypeError(f"{name} must be of type {t}")
        elif t == Optional[float]:
            if value is not None and not isinstance(value, float):
                raise TypeError(f"{name} must be of type {t}")
        elif t == Optional[bool]:
            if value is not None and not isinstance(value, bool):
                raise TypeError(f"{name} must be of type {t}")
        # Check if t is a dataclass
        elif t and hasattr(t, "__dataclass_fields__"):
            if not isinstance(value, t):
                raise TypeError(f"{name} must be of type {t}")
            eventual_value = json.dumps(value.__dict__)
        elif (
                hasattr(t, "__origin__")
                and t.__origin__ == list
                or t.__origin__ == dict
                or t.__origin__ == set
        ):
            # Assume list or dict is stored as a JSON string
            eventual_value = json.dumps(value)
            self._client.hset(self._key, key_name, json.dumps(value))
        else:
            raise TypeError(f"{name} must be of type {t}")

        self._client.hset(self._key, key_name, eventual_value)

    def __getattr__(self, name):
        """Fetch the given field name for the given config from Redis.
        Name can be either the actual attribute name or the mapped name if __key_map__ is defined.
        E.g., hostname_proxy or hostname:proxy
        obj["hostname_proxy"] or obj["hostname:proxy"] or obj.hostname_proxy
        """
        prop_name, key_name, is_dict = self._extract_prop_and_key_names(name)
        if is_dict:
            return self._client.hget(self._key, key_name)

        # Get the annotated type of the attribute
        t = self._data_class.__annotations__[prop_name]
        value = self._client.hget(self._key, key_name)
        # check the type t, if it is primitive, return it as is
        if value is None:
            return None
        if t in [int, str, float, bool]:
            return t(value)
        elif t == Optional[int]:
            return int(value) if value is not None else None
        elif t == Optional[str]:
            return str(value) if value is not None else None
        elif t == Optional[float]:
            return float(value) if value is not None else None
        elif t == Optional[bool]:
            return bool(value) if value is not None else None
        # Check if t is a dataclass
        elif t and hasattr(t, "__dataclass_fields__"):
            return t(**json.loads(value))
        elif hasattr(t, "__origin__") and t.__origin__ == list or t.__origin__ == dict:
            return json.loads(value)
        elif hasattr(t, "__origin__") and t.__origin__ == set:
            # Assume dict is stored as a JSON string
            return set(json.loads(value))

    def __delattr__(self, item):
        if isinstance(item, str) and item.startswith("_"):
            raise AttributeError(f"Cannot delete attribute {item}")
        _, key_name, _ = self._extract_prop_and_key_names(item)
        self._client.hdel(self._key, key_name)

    def __getitem__(self, name):
        """Alias for __getattr__ to allow dict-like access."""
        return self.__getattr__(name)

    def __setitem__(self, name, value):
        return self.__setattr__(name, value)

    def __delitem__(self, key):
        return self.__delattr__(key)

    def _extract_prop_and_key_names(self, name) -> tuple[str, str, bool]:
        """Extract the property name and the actual key name in Redis from the given name.
        We support having a Dataclass or a Dict as the HashMap object.
        Returns the property name, the key name, and a boolean indicating if it's a dict type.
        E.g., hostname_proxy or hostname:proxy
        """
        prop_name = key_name = name
        is_dict = False
        if hasattr(self._data_class, "__dataclass_fields__"):
            if isinstance(name, str) and name.startswith("_"):
                raise AttributeError(
                    f"{name} is not a valid attribute of {self._data_class.__name__}"
                )
            if hasattr(self._data_class, "__key_map__"):
                if name in self._data_class.__key_map__:
                    # Map the attribute name to the actual key in Redis
                    key_name = self._data_class.__key_map__[prop_name]
                else:
                    inverse = self._data_class.__key_map__.inverse
                    if key_name in inverse:
                        prop_name = inverse[key_name]
            if prop_name not in self._data_class.__annotations__:
                raise AttributeError(
                    f"{name} is not a valid attribute of {self._data_class.__name__}"
                )
        elif (
                hasattr(self._data_class, "__origin__")
                and self._data_class.__origin__ == dict
        ):
            is_dict = True
        else:
            raise TypeError(
                f"{self._data_class} is not a valid data class or dict type"
            )
        return prop_name, key_name, is_dict


class SProxy(Generic[T]):
    """
    A proxy class to access a Redis Set as a Python set.
    Getting and setting values are simply done via attribute operations.
    Note all values are stored as strings under the hood.

    E.g.,
        # To retrieve values
        obj()

        # To add values
        obj.add("value")

        # To remove values
        obj.remove("value")

        # To check if a value exists
        "value" in obj
    """

    def __init__(self, key: str, client: redis.Redis, elem_type: Type[T]):
        self._check_type(elem_type)
        self._elem_type = elem_type
        self._client = client
        self._key = key

    def __call__(self, *args, **kwargs):
        return {self._conv_back(item) for item in self._client.smembers(self._key)}

    def __or__(self, other):
        return self.add(other)

    def __ior__(self, other):
        return self.add(other)

    def __add__(self, other):
        return self.add(other)

    def __iadd__(self, other):
        return self.add(other)

    def __radd__(self, other):
        return NotImplemented

    def __xor__(self, other):
        return self.remove(other)

    def __ixor__(self, other):
        return self.remove(other)

    def __sub__(self, other):
        return self.remove(other)

    def __isub__(self, other):
        return self.remove(other)

    def __rxor__(self, other):
        return NotImplemented

    def add(self, value: Union[int, str, float, bool, frozenset]):
        self._client.sadd(self._key, self._conv_to_str(value))
        return self

    def remove(self, value: Union[int, str, float, bool, frozenset]):
        self._client.srem(self._key, self._conv_to_str(value))
        return self

    def __contains__(self, item):
        return self._client.sismember(self._key, item)

    def _check_type(self, t):
        if t not in [int, str, float, bool, frozenset]:
            raise TypeError(
                f"Set elem_type must be one of the followings: [int, str, float, bool, frozenset]"
            )

    def _conv_back(self, v) -> T:
        if self._elem_type == frozenset:
            return frozenset(json.loads(v))
        else:
            return self._elem_type(v)

    def _conv_to_str(self, v) -> str:
        if self._elem_type == frozenset:
            if v and v[0]:
                if not isinstance(v[0], (int, str, float, bool)):
                    raise TypeError(
                        f"Set elem_type must be one of the followings: [int, str, float, bool, frozenset]"
                    )
            return json.dumps(sorted(list(v))).decode("utf-8")
        else:
            return str(v)


def bind_config_hashmap[T](
        t: Type[T], key_gen: Callable[[], str], redis_client: redis.Redis
) -> HProxy[T]:
    return HProxy[T](key=key_gen(), client=redis_client, data_class=t)


def bind_config_set[T](
        elem_type: Type[T], key_gen: Callable[[], str], redis_client: redis.Redis
) -> SProxy[T]:
    return SProxy[T](key=key_gen(), client=redis_client, elem_type=elem_type)
