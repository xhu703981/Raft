"""The replicated state machine: a dict. It knows nothing about Raft."""

from typing import Any


class KVStore:
    def __init__(self) -> None:
        self.data: dict[str, str] = {}

    def apply(self, command: Any) -> None:
        if command is None:  # a new leader's no-op entry
            return
        op, key, *args = command
        if op == "put":
            self.data[key] = args[0]
        elif op == "delete":
            self.data.pop(key, None)

    def get(self, key: str) -> str | None:
        return self.data.get(key)

    def snapshot(self) -> dict[str, str]:
        return dict(self.data)

    def restore(self, data: dict[str, str]) -> None:
        self.data = dict(data)
