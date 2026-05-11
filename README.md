# log_debugger

`log_debugger` is a small MCP-based debugging harness for log-driven bug diagnosis with a local Ollama model.

The repository has two main parts:

1. The debugger runtime:
   - `client.py`: interactive client that talks to Ollama and MCP servers.
   - `repo_server.py`: repository inspection MCP server.
   - `log_server.py`: log search MCP server.
   - `history_summarizer.py`: condenses older tool history before it is sent back to the main model
2. Synthetic target projects under `test_projects/` used to exercise the debugger against known failure scenarios.

## How It Works

The client connects to two SSE MCP servers:

- a repository server that can map, read, and search code
- a log server that can search `.text` log files

The model is prompted to work in a strict order:

1. map the repository
2. search logs for a request ID or timestamp
3. locate the first warning/error in the request chain
4. read the relevant source file
5. produce a final diagnosis report

## Key Files

- `client.py`: prompt strategy, tool-call loop, loop prevention, Ollama API calls
- `repo_server.py`: `get_repository_structure`, `read_file`, `search_text_in_repository`
- `log_server.py`: `search_log`
- `history_summarizer.py`: rolling middle-history summarization via Ollama
- `requirements.txt`: runtime dependencies
- `test_projects/`: generated fixture projects and logs

## Fixture Projects

- `test_projects/basic_test_project`: first-generation "DataBridge" fixture with simpler failures
- `test_projects/simple_test_project`: second-generation "PulseMetrics" fixture with layered API/service/repository/infrastructure structure
- `test_projects/simple_test_project_production_like`: same general PulseMetrics shape with more production-like naming, config, and logs

See [`docs/fixtures.md`](docs/fixtures.md) for the embedded bug catalog and the differences between fixtures.

## Important Working Directory Rule

Both MCP servers are working-directory sensitive:

- `repo_server.py` uses `os.getcwd()` as the project root it exposes
- `log_server.py` expects a `./logs` directory relative to its current working directory

That means the servers often should not be started from the same directory.

Example for the production-like fixture:

1. Start `repo_server.py` from:
   `test_projects/simple_test_project_production_like/test_project_v2`
2. Start `log_server.py` from:
   `test_projects/simple_test_project_production_like`
3. Start `client.py` from this repo root

If both servers are started from the repo root, the debugger will inspect the debugger repo itself rather than the intended synthetic target code.

## Runtime Assumptions

- Ollama is available at `http://localhost:11434`
- the client defaults to model `gemma4:e4b`
- the history summarizer defaults to model `qwen2.5-coder:7b`
- the MCP servers are expected on:
  - `http://localhost:8002/sse` for the repo server
  - `http://localhost:8001/sse` for the log server

## Known Design Characteristics

- The client currently rebuilds the prompt from full raw tool history on each turn, so context size grows over time.
- Older tool history is now condensed once an estimated token threshold is crossed, while the last 3 raw iterations stay verbatim.
- The client executes only the first tool call returned by the model on each turn.
- The prompt strategy is intentionally strict and optimized for small local models.

## Getting Started

1. Install dependencies from `requirements.txt`.
2. Choose a target fixture under `test_projects/`.
3. Start the repo and log MCP servers from the correct directories.
4. Run `client.py`.
5. Paste a log line, request ID, or bug description into the prompt.

## Related Docs

- [`context.md`](context.md): durable project memory for future work
- [`docs/fixtures.md`](docs/fixtures.md): synthetic project and bug reference
