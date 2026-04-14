from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT
# Shortened to the absolute essentials. Imperative commands only.
# ─────────────────────────────────────────────────────────────────────────────

_SYSTEM_TEMPLATE = """\
ACT AS A DEBUGGING BOT.

RULES:
1. RESPONSE MUST BE A TOOL CALL.
2. NO CHAT. NO EXPLANATIONS.
3. USE ONLY THESE TOOLS: {tool_list}
4. ONLY USE PATHS FROM get_repository_structure. NEVER GUESS.
5. DATA IS PROVIDED IN 'SOURCE [NAME]' BLOCKS. READ THEM CAREFULLY.
...

SEQUENCE:
1. get_repository_structure (MANDATORY START)
2. search_logs_content(query="500")
3. search_logs_content(query="<timestamp>") to find the error chain.
4. read_file(path="...") for files seen in logs.
5. Write the report only when the bug is confirmed.

REPORT FORMAT:
## FINAL DIAGNOSIS REPORT
- FILE: <path>
- LINE: <N>
- ERROR: <exception_type>
- CAUSE: <one_sentence_reason>
- FIX: <one_line_code_change>
- LOG_EVIDENCE: <exact_log_line>
"""

def build_system_prompt(available_tools: list[str]) -> str:
    return _SYSTEM_TEMPLATE.format(tool_list=", ".join(available_tools))

# ─────────────────────────────────────────────────────────────────────────────
# PROMPT BUILDER
# ─────────────────────────────────────────────────────────────────────────────

class DebugPromptBuilder:
    def __init__(self, available_tools: list[str]) -> None:
        self._tools = available_tools
        self._today = datetime.now().strftime("%Y-%m-%d")
        self.system = build_system_prompt(available_tools)

    def initial(self, query: str) -> str:
        """Forces the first mandatory step."""
        return (
            f"DATE: {self._today}\n"
            f"BUG: {query}\n\n"
            "STEP 1: Call get_repository_structure now."
        )

    def continuation(self, query: str, history_str: str) -> str:
        """Directional prompt to prevent loops."""
        return (
            f"EVIDENCE:\n{history_str}\n\n"
            "INSTRUCTION:\n"
            "1. If you see a file path and an error, call read_file.\n"
            "2. If you see an HTTP 500 without a traceback, search the timestamp.\n"
            "3. If the bug is found, write ## FINAL DIAGNOSIS REPORT.\n"
            "DO NOT REPEAT PREVIOUS TOOL CALLS WITH SAME ARGUMENT."
        )

    def stall(self, stall_count: int) -> str:
        """Aggressive correction for 'talkative' models."""
        messages = {
            1: "STOP TALKING. CALL A TOOL OR WRITE THE REPORT.",
            2: "INVALID OUTPUT. YOU MUST USE A TOOL. Call search_logs_content(query='500') if stuck.",
            3: "FINAL WARNING: Call a tool now or I will terminate the session.",
        }
        return messages.get(stall_count, messages[3])

    def tool_correction(self, bad_tool: str) -> str:
        return (
            f"ERROR: '{bad_tool}' IS NOT A TOOL.\n"
            f"USE ONLY: {', '.join(self._tools)}"
        )

























import asyncio
import json
import hashlib
import httpx
import sys
from datetime import datetime
from typing import Dict, List, Any

from mcp.client.session import ClientSession
from mcp.client.sse import sse_client

class OllamaMCPClient:
    """
    Optimized client for small-parameter local models.
    Prioritizes context window hygiene and loop prevention.
    """
    MAX_TURNS = 15
    MAX_STALLS = 3
    OLLAMA_URL = "http://localhost:11434/api/chat"

    def __init__(self, model: str = "gemma4:e4b"):
        self.model = model
        self.call_history = set()
        self.tool_mapping = {}
        self.all_tools = []

    async def run(self, repo_url: str, log_url: str):
        """Main entry point for the debugging session."""
        self._log("SYS", f"Initializing session with model: {self.model}")
        
        try:
            async with sse_client(repo_url) as (repo_r, repo_w), \
                       sse_client(log_url) as (log_r, log_w):
                
                async with ClientSession(repo_r, repo_w) as repo_session, \
                           ClientSession(log_r, log_w) as log_session:
                    
                    # 1. Initialize Sessions
                    await asyncio.gather(repo_session.initialize(), log_session.initialize())
                    
                    # 2. Aggregate Tools
                    repo_resp = await repo_session.list_tools()
                    log_resp = await log_session.list_tools()
                    
                    self.all_tools = repo_resp.tools + log_resp.tools
                    self.tool_mapping = {t.name: repo_session for t in repo_resp.tools}
                    self.tool_mapping.update({t.name: log_session for t in log_resp.tools})

                    self._log("SYS", f"Debugger Online. {len(self.all_tools)} tools ready.")

                    while True:
                        query = input("\n[?] Describe the issue (or 'q' to quit): ").strip()
                        if query.lower() in ['q', 'quit', 'exit']: break
                        if not query: continue
                        
                        await self.chat_with_tools(query)

        except Exception as e:
            self._log("ERROR", f"Connection failed: {str(e)}")

    async def chat_with_tools(self, user_message: str):
        """Orchestrates the multi-turn debugging logic with raw history retention."""
        prompt_builder = DebugPromptBuilder(available_tools=list(self.tool_mapping.keys()))
        stall_count = 0
        self.call_history.clear()
        
        # This list will store every tool result and model thought for the entire session
        raw_history_log = []

        # Turn 0: Initial setup
        messages = [
            {"role": "system", "content": prompt_builder.system},
            {"role": "user", "content": prompt_builder.initial(user_message)}
        ]

        for turn in range(self.MAX_TURNS):
            self._log("AI", f"Turn {turn + 1}/{self.MAX_TURNS} Reasoning...")
            
            response = await self._call_ollama(messages)
            if not response: break

            content = response.get("content", "")
            tool_calls = response.get("tool_calls") or []

            # A. Check for Final Diagnosis
            if "## FINAL DIAGNOSIS REPORT" in content:
                print(f"\n{content}\n")
                self._log("DONE", "Investigation complete.")
                break

            # B. Handle Stalls
            if not tool_calls:
                stall_count += 1
                if stall_count >= self.MAX_STALLS:
                    self._log("ABORT", "Model stalled too many times.")
                    break
                messages.append({"role": "user", "content": prompt_builder.stall(stall_count)})
                continue

            # C. Execute Tool
            stall_count = 0
            call = tool_calls[0]
            t_name = call["function"]["name"]
            t_args = call["function"]["arguments"]
            if isinstance(t_args, str): t_args = json.loads(t_args)

            # Detect Loops
            call_sig = f"{t_name}:{hashlib.md5(str(t_args).encode()).hexdigest()[:4]}"
            if call_sig in self.call_history:
                self._log("LOOP", f"Redundant call to {t_name}. Nudging model.")
                messages.append({"role": "user", "content": "You already tried that. Check the evidence below or write the report."})
                continue
            
            self.call_history.add(call_sig)

            if t_name in self.tool_mapping:
                result = await self._execute_tool(self.tool_mapping[t_name], t_name, t_args)
                
                # Update the raw history log with the tool's output
                raw_history_log.append(f"--- TOOL CALL: {t_name} ---\nARGS: {t_args}\nRESULT:\n{result}\n")
                
                # D. Updated Context Strategy (Full Raw History)
                # We concatenate all past results so the model sees everything at once.
                history_blob = "\n".join(raw_history_log)
                
                messages = [
                    {"role": "system", "content": prompt_builder.system},
                    {"role": "user", "content": prompt_builder.initial(user_message)},
                    {"role": "user", "content": f"FULL SESSION HISTORY:\n{history_blob}"},
                    {"role": "user", "content": "Based on the FULL HISTORY above, what is the next step?"}
                ]
                
                self._log("HISTORY", f"History size: {len(history_blob)} chars.")
            else:
                messages.append({"role": "user", "content": prompt_builder.tool_correction(t_name)})

    async def _call_ollama(self, messages: List[Dict]) -> Dict:
        """Helper to communicate with local Ollama API with verbose error tracking."""
        payload = {
            "model": self.model,
            "messages": messages,
            "stream": False,
            "tools": self._format_tools(),
            "options": {
                "temperature": 0.0,
                "num_ctx": 8192  # Increased for Turn 5+ depth
            }
        }
        try:
            # Use a longer timeout for Turn 5+ as the prompt is now very large
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    self.OLLAMA_URL, 
                    json=payload, 
                    timeout=90.0  # Increased to 90s
                )
                
                if resp.status_code != 200:
                    self._log("ERR", f"Ollama Status {resp.status_code}: {resp.text}")
                    return {}
                    
                return resp.json().get("message")

        except httpx.TimeoutException:
            self._log("ERR", "Ollama timed out. The context might be too heavy for your GPU/CPU.")
        except Exception as e:
            # This will now show the full traceback info
            self._log("ERR", f"Ollama call failed: {type(e).__name__} - {str(e)}")
            return {}

    async def _execute_tool(self, session: ClientSession, name: str, args: dict) -> str:
        self._log('TOOL', f'CALLING: {name} || ARGS: {args}')
        try:
            result = await session.call_tool(name, args)
            raw_text = "\n".join(item.text for item in result.content if hasattr(item, "text"))
            
            # Minimalist format: Clear name and a simple separator
            formated_result = f"\nSOURCE [{name.upper()}]:\n{raw_text}\n---\n"
            self._log('RESULT', formated_result)
            return formated_result
        except Exception as e:
            return f"\nSOURCE [{name.upper()}] ERROR:\n{str(e)}\n---\n"

    def _format_tools(self) -> List[Dict]:
        """Formats MCP tools for the Ollama tool-call schema."""
        return [
            {
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": (t.description or t.name)[:120],
                    "parameters": t.inputSchema
                }
            } for t in self.all_tools
        ]

    def _log(self, tag: str, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] {tag:<6} | {msg}")

if __name__ == "__main__":
    # Ensure MCP servers are running on these ports
    client = OllamaMCPClient(model="gemma4:e4b")
    asyncio.run(client.run(
        repo_url="http://localhost:8000/sse",
        log_url="http://localhost:8001/sse"
    ))