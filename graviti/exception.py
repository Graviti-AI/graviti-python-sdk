#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Basic concepts of Graviti custom exceptions."""

from subprocess import CalledProcessError
from typing import Dict, Optional, Type

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


class PortexError(GravitiException):
    """This is the base class for custom exceptions in Graviti portex module."""


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


class ManagerError(GravitiException):
    """This is the base class for custom exceptions in Graviti manager module."""


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
        return f'The {self._resource} name "{self._name}" is invalid.'


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


class AccessDeniedError(ResponseError):
    """This class defines the exception for access denied response error."""

    STATUS_CODE = 403


class ForbiddenError(ResponseError):
    """This class defines the exception for illegal operations Graviti forbids."""

    STATUS_CODE = 403


class InvalidParamsError(ResponseError):
    """This class defines the exception for invalid parameters response error."""

    STATUS_CODE = 400


class NameConflictError(ResponseError):
    """This class defines the exception for name conflict response error."""

    STATUS_CODE = 409


class RequestParamsMissingError(ResponseError):
    """This class defines the exception for request parameters missing response error."""

    STATUS_CODE = 400


class ResourceNotExistError(ResponseError):
    """This class defines the exception for resource not existing response error."""

    STATUS_CODE = 404


class InternalServerError(ResponseError):
    """This class defines the exception for internal server error."""

    STATUS_CODE = 500


class UnauthorizedError(ResponseError):
    """This class defines the exception for unauthorized response error."""

    STATUS_CODE = 401


RESPONSE_ERROR_DISTRIBUTOR: Dict[str, Type[ResponseError]] = {
    "AccessDenied": AccessDeniedError,
    "Forbidden": ForbiddenError,
    "InvalidParamsValue": InvalidParamsError,
    "NameConflict": NameConflictError,
    "RequestParamsMissing": RequestParamsMissingError,
    "ResourceNotExist": ResourceNotExistError,
    "InternalServerError": InternalServerError,
    "Unauthorized": UnauthorizedError,
}
