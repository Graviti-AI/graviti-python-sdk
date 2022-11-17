#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Basic concepts of Graviti custom exceptions."""

from subprocess import CalledProcessError
from typing import Dict, Optional, Tuple, Type, TypeVar

from requests.models import Response


class GravitiException(Exception):
    """This is the base class for Graviti custom exceptions.

    Arguments:
       message: The error message.

    """

    def __init__(self, message: Optional[str] = None):
        super().__init__()
        self._message = message

    def __str__(self) -> str:
        return self._message if self._message else ""


class UtilityError(GravitiException):
    """This is the base class for custom exceptions in Graviti utility module."""


class ImageDecodeError(UtilityError):
    """This class defines the exception for the image decode errors."""


class PortexError(GravitiException):
    """This is the base class for custom exceptions in Graviti portex module."""


class FieldNameConflictError(PortexError):
    """This class defines the exception for the portex field name error."""


class GitNotFoundError(PortexError):
    """This class defines the exception for the git command not found error.

    Arguments:
       message: The error message.

    """

    _MESSAGE = (
        "'git' command failed, most likely due to the 'git' is not installed.\n"
        "Please install 'git' first: https://git-scm.com/book/en/v2/Getting-Started-Installing-Git"
    )

    def __init__(self, message: str = _MESSAGE):
        super().__init__(message)


class GitCommandError(PortexError):
    """This class defines the exception for the git command related error.

    Arguments:
       message: The error message.
       called_process_error: The CalledProcessError raised from the subprocess.run().

    """

    def __init__(self, message: str, called_process_error: CalledProcessError):
        super().__init__(message)
        self.called_process_error = called_process_error

    def __str__(self) -> str:
        error = self.called_process_error
        return (
            f"{self._message}\n"
            f"  command: {error.cmd}\n"
            f"  returncode: {error.returncode}\n"
            f"  stdout: {error.stdout.decode()}\n"
            f"  stderr: {error.stderr.decode()}"
        )


class OperationError(GravitiException):
    """This is the base class for custom exceptions in Graviti operation module."""


class ObjectCopyError(OperationError):
    """This class defines the exception for object copy error."""


class ManagerError(GravitiException):
    """This is the base class for custom exceptions in Graviti manager module."""


class CriteriaError(ManagerError):
    """This class defines the exception for invalid search criteria."""


class StatusError(ManagerError):
    """This class defines the exception for illegal status."""


class NoCommitsError(StatusError):
    """This class defines the exception for illegal operations on dataset with no commit history."""


class ResourceNameError(ManagerError):
    """This class defines the exception for invalid resource names."""

    def __init__(self, resource: str, name: str) -> None:
        super().__init__()
        self._resource = resource
        self._name = name

    def __str__(self) -> str:
        return f'The {self._resource} name "{self._name}" is invalid'


class ResponseError(ManagerError):
    """This class defines the exception for post response error.

    Arguments:
        response: The response of the request.

    Attributes:
        response: The response of the request.

    """

    # https://github.com/python/mypy/issues/6473
    _INDENT = " " * len(__qualname__)  # type: ignore[name-defined]

    STATUS_CODE: int
    ERROR_CODE: Optional[str]

    def __init__(
        self, message: Optional[str] = None, *, response: Optional[Response] = None
    ) -> None:
        super().__init__(message)
        if response is not None:
            self.response = response

    def __init_subclass__(cls) -> None:
        cls._INDENT = " " * len(cls.__name__)

    def __str__(self) -> str:
        if hasattr(self, "response"):
            return (
                f"Unexpected status code({self.response.status_code})! {self.response.url}!"
                f"\n{self._INDENT}  {self.response.text}"
            )
        return super().__str__()


_R = TypeVar("_R", bound=Type[ResponseError])


class ResponseErrorRegister:
    """A class decorator to register the ResponseError into the distributor.

    Arguments:
        status_code: The http status code of the specific ResponseError.
        error_code: The response error code of the specific ResponseError.

    """

    RESPONSE_ERROR_DISTRIBUTOR: Dict[Tuple[int, Optional[str]], Type[ResponseError]] = {}

    def __init__(self, status_code: int, error_code: Optional[str] = None) -> None:
        self._status_code = status_code
        self._error_code = error_code

    def __call__(self, response_error: _R) -> _R:
        """Register the ResponseError into the distributor.

        Arguments:
            response_error: The response error needs to be registered.

        Returns:
            The input response error class unchanged.

        """
        response_error.STATUS_CODE = self._status_code
        response_error.ERROR_CODE = self._error_code

        self.RESPONSE_ERROR_DISTRIBUTOR[(self._status_code, self._error_code)] = response_error

        return response_error


@ResponseErrorRegister(403, "AccessDenied")
class AccessDeniedError(ResponseError):
    """This class defines the exception for access denied response error."""


@ResponseErrorRegister(403, "Forbidden")
class ForbiddenError(ResponseError):
    """This class defines the exception for illegal operations Graviti forbids."""


@ResponseErrorRegister(400, "InvalidParamsValue")
class InvalidParamsError(ResponseError):
    """This class defines the exception for invalid parameters response error."""


@ResponseErrorRegister(409, "NameConflict")
class NameConflictError(ResponseError):
    """This class defines the exception for name conflict response error."""


@ResponseErrorRegister(400, "RequestParamsMissing")
class RequestParamsMissingError(ResponseError):
    """This class defines the exception for request parameters missing response error."""


@ResponseErrorRegister(404)
class NotFoundError(ResponseError):
    """This class defines the exception for 404 not found response error without error code."""


@ResponseErrorRegister(404, "ResourceNotExist")
class ResourceNotExistError(NotFoundError):
    """This class defines the exception for resource not existing response error."""


@ResponseErrorRegister(500, "InternalServerError")
class InternalServerError(ResponseError):
    """This class defines the exception for internal server error."""


@ResponseErrorRegister(401, "Unauthorized")
class UnauthorizedError(ResponseError):
    """This class defines the exception for unauthorized response error."""


@ResponseErrorRegister(503)
class ServiceUnavailableError(ResponseError):
    """This class defines the exception for 503 service unavailable error without error code."""
