import abc
import typing

from lime_etl.domain import exceptions, value_objects

T = typing.TypeVar("T", covariant=True)


class SharedResource(abc.ABC, typing.Generic[T]):
    @property
    @abc.abstractmethod
    def name(self) -> value_objects.ResourceName:
        raise NotImplementedError

    @abc.abstractmethod
    def open(self) -> T:
        raise NotImplementedError

    @abc.abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    def __eq__(self, other: object) -> bool:
        if other.__class__ is self.__class__:
            # noinspection PyTypeChecker
            return self.name == typing.cast(SharedResource[T], other).name
        else:
            return NotImplemented

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return NotImplemented
        else:
            return not result

    def __hash__(self) -> int:
        return hash(self.name)

    def __repr__(self) -> str:
        return f"SharedResource({self.name!r})"


class ResourceManager(typing.Generic[T]):
    def __init__(self, resource: SharedResource[T], /):
        if resource is None:
            raise exceptions.InvalidResource(
                f"Expected a resource with a close and open method, but got None."
            )

        if not hasattr(resource, "open") or not hasattr(resource, "close"):
            raise exceptions.InvalidResource(
                f"Expected a resource with a close and open method, but got {resource!r}."
            )

        self._handle: typing.Optional[T] = None
        self._resource: typing.Optional[SharedResource[T]] = resource

    def close(self) -> bool:
        if self._handle is None:
            return False
        else:
            self.resource.close()
            self._resource = None
            self._handle = None
            return True

    @property
    def is_open(self) -> bool:
        return self._handle is not None

    def open(self) -> T:
        if self._handle is None:
            self._handle = self.resource.open()
        return self._handle

    @property
    def resource(self) -> SharedResource[T]:
        if self._resource is None:
            raise ValueError("The resource has already been disposed.")
        else:
            return self._resource

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._resource!r})"
