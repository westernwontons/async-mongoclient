from typing import Any
from typings import DocumentType
from pymongo.command_cursor import CommandCursor


class AsyncCommandCursor(CommandCursor[DocumentType]):
    def to_list(self, length: int | float | None = None) -> Any:
        pass
