#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Basic concepts of Graviti custom exceptions."""

from subprocess import CalledProcessError
from typing import Dict, Optional, Type, Union

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
    """This class defines the exception for illegal status.

    Arguments:
        is_draft: Whether the status is draft.
        message: The error message.

    """

    def __init__(self, message: Optional[str] = None, *, is_draft: Optional[bool] = None) -> None:
        super().__init__()
        if is_draft is None:
            self._message = message
        else:
            required_status = "commit" if is_draft else "draft"
            self._message = f"The status is not {required_status}"


class ResponseError(GravitiException):
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
    """This class defines the exception for invalid parameters response error.

    Arguments:
        response: The response of the request.
        param_name: The name of the invalid parameter.
        param_value: The value of the invalid parameter.

    Attributes:
        response: The response of the request.

    """

    STATUS_CODE = 400

    def __init__(
        self,
        message: Optional[str] = None,
        *,
        response: Optional[Response] = None,
        param_name: Optional[str] = None,
        param_value: Optional[str] = None,
    ) -> None:
        super().__init__(message, response=response)
        self._param_name = param_name
        self._param_value = param_value

    def __str__(self) -> str:
        if self._param_name and self._param_value:
            messages = [f"Invalid {self._param_name}: {self._param_value}."]
            if self._param_name == "path":
                messages.append("Remote path should follow linux style.")

            return f"\n{self._INDENT}".join(messages)
        return super().__str__()


class NameConflictError(ResponseError):
    """This class defines the exception for name conflict response error.

    Arguments:
        response: The response of the request.
        resource: The type of the conflict resource.
        identification: The identification of the conflict resource.

    Attributes:
        response: The response of the request.

    """

    STATUS_CODE = 409

    def __init__(
        self,
        message: Optional[str] = None,
        *,
        response: Optional[Response] = None,
        resource: Optional[str] = None,
        identification: Union[int, str, None] = None,
    ) -> None:
        super().__init__(message, response=response)
        self._resource = resource
        self._identification = identification

    def __str__(self) -> str:
        if self._resource and self._identification:
            return f"The {self._resource}: {self._identification} already exists."
        return super().__str__()


class RequestParamsMissingError(ResponseError):
    """This class defines the exception for request parameters missing response error."""

    STATUS_CODE = 400


class ResourceNotExistError(ResponseError):
    """This class defines the exception for resource not existing response error.

    Arguments:
        response: The response of the request.
        resource: The type of the conflict resource.
        identification: The identification of the conflict resource.

    Arguments:
        response: The response of the request.

    """

    STATUS_CODE = 404

    def __init__(
        self,
        message: Optional[str] = None,
        *,
        response: Optional[Response] = None,
        resource: Optional[str] = None,
        identification: Union[int, str, None] = None,
    ) -> None:
        super().__init__(message, response=response)
        self._resource = resource
        self._identification = identification

    def __str__(self) -> str:
        if self._resource and self._identification:
            return f"The {self._resource}: {self._identification} does not exist."
        return super().__str__()


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
