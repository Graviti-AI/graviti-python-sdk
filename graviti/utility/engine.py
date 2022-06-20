#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Engine control related classes."""

from enum import Enum, auto
from typing import Any


class Mode(Enum):
    """This class defines the engine mode and includes 'LOCAl' and 'ONLINE'."""

    LOCAL = auto()
    ONLINE = auto()


class Online:
    """An engine controller used to start and stop the online mode."""

    _old_mode: Mode

    def __init__(self, _engine: "Engine") -> None:
        self._engine = _engine

    def __enter__(self) -> None:
        self._old_mode = self._engine.mode
        self._engine.mode = Mode.ONLINE

    def __exit__(self, *_: Any) -> None:
        self._engine.mode = self._old_mode


class Engine:
    """This is a base class defining the Engine mode."""

    mode = Mode.LOCAL

    def online(self) -> Online:
        """Init a Online instance.

        Returns:
            the Online instance.

        """
        return Online(self)


engine = Engine()
