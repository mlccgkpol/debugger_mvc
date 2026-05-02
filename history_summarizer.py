import re
from typing import Sequence

import httpx

from debugger_logger import DebuggerLogger


_TOKEN_PATTERN = re.compile(r"\w+|[^\w\s]", re.UNICODE)


class HistorySummarizer:
    """
    Incrementally condenses older tool history so the main debugger model
    keeps the original prompt plus the freshest raw iterations.
    """

    def __init__(
        self,
        ollama_url: str,
        model: str = "qwen2.5-coder:7b",
        trigger_tokens: int = 3500,
        timeout: float = 120.0,
        logger: DebuggerLogger | None = None,
    ) -> None:
        self.ollama_url = ollama_url
        self.model = model
        self.trigger_tokens = trigger_tokens
        self.timeout = timeout
        self.logger = logger or DebuggerLogger()

    def estimate_tokens(self, text: str) -> int:
        """
        Lightweight token estimate so summarization can be triggered without
        introducing another tokenizer dependency.
        """
        return len(_TOKEN_PATTERN.findall(text))

    def should_summarize(
        self,
        history_entries: Sequence[str],
        preserve_recent: int = 3,
    ) -> bool:
        if len(history_entries) <= preserve_recent:
            return False
        total_history = "\n".join(history_entries)
        return self.estimate_tokens(total_history) >= self.trigger_tokens

    async def summarize(
        self,
        existing_summary: str,
        new_entries: Sequence[str],
    ) -> str:
        """
        Fold newly-eligible history entries into the running middle-history
        summary. The result replaces the previous summary.
        """
        if not new_entries:
            return existing_summary

        summary_context = existing_summary.strip() or "(none yet)"
        tool_history = "\n".join(new_entries)

        messages = [
            {
                "role": "system",
                "content": (
                    "You summarize debugging-session history for another coding model.\n"
                    "Preserve only facts that matter to continue the investigation.\n"
                    "Keep request IDs, timestamps, tool names, file paths, line numbers, "
                    "exceptions, confirmed conclusions, and unresolved leads.\n"
                    "Drop repetition and boilerplate. Never invent facts.\n"
                    "Return compact plain text using short bullet points."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Existing condensed history:\n"
                    f"{summary_context}\n\n"
                    "New tool history to absorb:\n"
                    f"{tool_history}\n\n"
                    "Return an updated condensed history that keeps chronology and "
                    "enough detail for the next debugging step. Keep it concise."
                ),
            },
        ]

        payload = {
            "model": self.model,
            "messages": messages,
            "stream": False,
            "options": {
                "temperature": 0.0,
                "num_ctx": 4096,
            },
        }

        self.logger.model_request(
            source="summarizer",
            model=self.model,
            messages=messages,
        )

        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.ollama_url,
                json=payload,
                timeout=self.timeout,
            )

        if response.status_code != 200:
            raise RuntimeError(
                f"Summarizer returned HTTP {response.status_code}: {response.text}"
            )

        content = response.json().get("message", {}).get("content", "").strip()
        if not content:
            raise RuntimeError("Summarizer returned an empty history summary.")

        self.logger.model_response(
            source="summarizer",
            model=self.model,
            content=content,
            tool_calls=[],
        )

        return content
