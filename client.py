import asyncio
import json
import httpx
from mcp.client.session import ClientSession
from mcp.client.sse import sse_client
import time
from datetime import datetime


class OllamaMCPClient:
    def __init__(self, ollama_url="http://localhost:11434", model="gemma4:e4b"):
        self.ollama_url = ollama_url
        self.model = model

    async def run_chat(self):
        """Run the interactive chat with MCP tools."""
        
        async with sse_client("http://localhost:8000/sse") as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                # Get available tools
                tools_response = await session.list_tools()
                tools = tools_response.tools
                print(f"Connected to MCP server with {len(tools)} tools:")
                for tool in tools:
                    print(f"  - {tool.name}: {tool.description}")

                # Interactive chat loop
                print("\n=== Ollama MCP Chat ===")
                print("Type 'quit' to exit")
                print("-" * 50)

                while True:
                    try: 
                        user_input = input("You: ")
                        if user_input.lower() in ['quit', 'exit', 'q']:
                            break

                        await self.chat_with_tools(user_input, session, tools)
                        print("-" * 50)
                    except Exception as e:
                        print('Error occured:', str(e))

    def format_tools_for_ollama(self, tools):
        """Convert MCP tools to Ollama tool format"""

        formatted = []

        for tool in tools:

            description = tool.description or f"Tool {tool.name}"

            formatted.append({
            "type": "function",
            "function": {
                "name": tool.name,
                "description": description,
                "parameters": tool.inputSchema or {
                    "type": "object",
                    "properties": {}
                }
                }
            })

        return formatted

    # --- 1. LOGGING & VISUALS ---
    def _print_banner(self, title):
        print(f"\n{'='*80}\n{title:^80}\n{'='*80}")

    def _log_step(self, status, icon, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {icon} {status:<10} | {message}")

    # --- 2. PROMPT STRATEGY ---
    def _get_debug_instructions(self):
        return (
            "You are a Senior Software Debugger. Your goal is to find the root cause of issues.\n"
            "INVESTIGATION RULES:\n"
            "1. LOCATE: Use 'locate_source_file' or 'list_directory' to find relevant code.\n"
            "2. SEARCH: Use 'grep_repository' to find error strings, variable usages, or function definitions.\n"
            "3. ANALYZE: Read the code logic. Look for edge cases, null pointers, or logic flaws.\n"
            "4. ITERATE: If the first file doesn't explain the bug, follow the imports/calls to the next file.\n"
            "5. CONCLUDE: Once found, explain the bug and the specific file/line responsible."
        )

    def _format_prompt_context(self, query, history, tools_json, is_initial=False):
        if is_initial:
            return f"""
User query: {query}
Message history: Starting new investigation.
tools: {tools_json}
prompt: {self._get_debug_instructions()}
"""
        return f"""
History (Current Progress):
{history}

System instructions:
{self._get_debug_instructions()}

original user query: {query}

tools: {tools_json}
"""

    # --- 3. EXECUTION LOGIC ---
    async def _execute_tool(self, session, name, args):
        self._log_step("TOOL_START", "🔧", f"Invoking: {name} || Args: {args}")
        try:
            result = await session.call_tool(name, args)
            text = "\n".join([item.text for item in result.content if hasattr(item, 'text')])
            
            # Log a small snippet of the result for the user
            preview = text[:150].replace('\n', ' ')
            self._log_step("TOOL_DATA", "📥", f"Received: {preview}...")
            return text
        except Exception as e:
            self._log_step("TOOL_ERROR", "❌", str(e))
            return f"Error: {str(e)}"

    async def chat_with_tools(self, user_message, session, tools):
        formatted_tools = self.format_tools_for_ollama(tools)
        tools_block = json.dumps(formatted_tools, indent=2)
        raw_history = []
        
        # Turn 0 Setup
        prompt = self._format_prompt_context(user_message, "", tools_block, is_initial=True)
        current_messages = [{"role": "user", "content": prompt}]

        self._print_banner("DEBUGGER SESSION: ROOT CAUSE ANALYSIS")

        for i in range(20):
            self._log_step("MODEL", "🧠", f"Reasoning (Turn {i+1})...")
            
            start_time = time.time()
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.ollama_url}/api/chat", 
                    json={
                        "model": self.model,
                        "messages": current_messages,
                        "stream": False,
                        "tools": formatted_tools,
                        "options": {"temperature": 0.1}
                    }, 
                    timeout=60.0
                )
                data = response.json()
            
            assistant_msg = data["message"]
            elapsed = time.time() - start_time

            # Display thoughts
            if assistant_msg.get("content"):
                print(f"\n💭 [THOUGHTS]: {assistant_msg['content'].strip()}\n")

            tool_calls = assistant_msg.get("tool_calls")
            if not tool_calls:
                self._log_step("COMPLETE", "✅", "The model has provided a final answer.")
                break

            # Process Tools
            raw_history.append({"role": "assistant", "content": assistant_msg.get("content") or "Investigating..."})
            
            for tool_call in tool_calls:
                t_name = tool_call["function"]["name"]
                t_args = tool_call["function"]["arguments"]
                if isinstance(t_args, str): t_args = json.loads(t_args)

                result_text = await self._execute_tool(session, t_name, t_args)
                raw_history.append({"role": "tool", "name": t_name, "content": result_text})

            # Rebuild History (Keep only the most relevant recent steps to save tokens)
            history_str = ""
            for msg in raw_history[-6:]: # Sliding window of history
                role = msg['role'].upper()
                name = f" ({msg['name']})" if 'name' in msg else ""
                # Truncate content for the prompt so it doesn't get too bloated
                content = msg['content'][:800] + "..." if len(msg['content']) > 800 else msg['content']
                history_str += f"{role}{name}: {content}\n"

            # Re-inject the structured state
            next_prompt = self._format_prompt_context(user_message, history_str, tools_block)
            current_messages = [{"role": "user", "content": next_prompt}]

        self._print_banner("INVESTIGATION CONCLUDED")




async def main():
    client = OllamaMCPClient()

    try:
        await client.run_chat()
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())