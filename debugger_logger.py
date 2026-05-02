from datetime import datetime
from typing import Any, Sequence


class DebuggerLogger:
    """
    Small shared logger for debugger services.
    """

    def __init__(self, preview_chars: int = 240) -> None:
        self.preview_chars = preview_chars

    def log(self, tag: str, msg: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] {tag:<9} | {msg}")

    def model_request(
        self,
        source: str,
        model: str,
        messages: Sequence[dict[str, Any]],
        tools_count: int | None = None,
    ) -> None:
        total_chars = sum(len(str(message.get("content", ""))) for message in messages)
        tools_fragment = f" tools={tools_count}" if tools_count is not None else ""
        self.log(
            "MODEL_REQ",
            (
                f"{source} -> model={model} messages={len(messages)} "
                f"chars={total_chars}{tools_fragment}"
            ),
        )

    def model_response(
        self,
        source: str,
        model: str,
        content: str = "",
        tool_calls: Sequence[Any] | None = None,
    ) -> None:
        tool_count = len(tool_calls or [])
        preview = self._preview(content)
        suffix = f" preview={preview!r}" if preview else ""
        self.log(
            "MODEL_RES",
            (
                f"{source} <- model={model} content_chars={len(content)} "
                f"tool_calls={tool_count}{suffix}"
            ),
        )

    def tool_request(self, name: str, args: dict[str, Any]) -> None:
        self.log("TOOL", f"CALLING: {name} || ARGS: {args}")

    def tool_response(self, result: str) -> None:
        self.log("RESULT", result)

    def _preview(self, text: str) -> str:
        if not text:
            return ""
        compact = " ".join(text.split())
        if len(compact) <= self.preview_chars:
            return compact
        return compact[: self.preview_chars - 3] + "..."
