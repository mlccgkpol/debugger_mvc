# Project Context

This file is meant to be a durable memory aid for future work in `log_debugger`.

## What This Repository Actually Is

This is not a production web service. It is a debugging assistant prototype built around:

- a local LLM client (`client.py`)
- MCP servers that expose code and logs as tools
- synthetic target applications with seeded bugs and realistic logs

The repository is best understood as a mini evaluation harness for log-first debugging behavior.

## Core Mental Model

There are three moving pieces:

1. `client.py`
   - runs the interactive session
   - fetches tool definitions from both MCP servers
   - sends a strict prompt to Ollama
   - executes returned tool calls
   - accumulates tool outputs into session history and condenses older history when it grows too large

2. `repo_server.py`
   - exposes codebase inspection tools
   - assumes its current working directory is the code root to inspect

3. `log_server.py`
   - exposes log search
   - assumes logs live in `./logs` under its current working directory

4. `history_summarizer.py`
   - uses `qwen2.5-coder:7b` through Ollama
   - summarizes only the middle history
   - preserves the last 3 raw iterations for the main model

The target code being debugged is usually not the `log analyser` repo itself. It is typically one of the generated projects in `test_projects/`.

## Most Important Practical Detail

The repo server and log server are cwd-sensitive and may need to be launched from different directories.

Typical fixture setup:

- `repo_server.py` should be run inside the target source tree
- `log_server.py` should be run from the fixture directory that contains `logs/`

Example:

- repo server cwd:
  `test_projects/simple_test_project_production_like/test_project_v2`
- log server cwd:
  `test_projects/simple_test_project_production_like`

If this is forgotten, diagnosis quality will drop because the model will inspect the wrong file tree or fail to find logs.

## Current Repository Layout

- `client.py`
- `repo_server.py`
- `log_server.py`
- `requirements.txt`
- `test_projects/basic_test_project/`
- `test_projects/simple_test_project/`
- `test_projects/simple_test_project_production_like/`

There was no original top-level documentation before this file was added.

## What Each Main File Owns

### `client.py`

Responsibilities:

- define the system prompt and prompt-builder behavior
- connect to both MCP servers via SSE
- list and format tool schemas for Ollama
- run a turn loop with stall handling and loop detection
- print the final diagnosis report

Notable behavior:

- `MAX_TURNS = 15`
- `MAX_STALLS = 3`
- default Ollama endpoint is `http://localhost:11434/api/chat`
- default model is `gemma4:e4b`
- a separate summarizer model is used for history compression once a configurable token threshold is crossed
- only the first tool call from a model response is executed per turn
- the original prompt stays intact, older middle history is summarized, and the last 3 raw iterations are kept verbatim

Implication:

- the system stays simple, but long sessions now trade some raw fidelity for better context efficiency

### `history_summarizer.py`

Responsibilities:

- estimate history size without an external tokenizer dependency
- decide when summarization should start
- incrementally fold older tool results into a running summary
- keep the main debugger model focused on condensed history plus fresh raw evidence

Notable behavior:

- defaults to `qwen2.5-coder:7b`
- only summarizes history older than the last 3 raw iterations
- is triggered by an environment-configurable estimated token threshold
- if summarization fails, the client falls back to unsummarized history instead of losing context

### `repo_server.py`

Exposed tools:

- `get_repository_structure(max_depth=5)`
- `read_file(path)`
- `search_text_in_repository(query)`

Important details:

- `_PROJECT_ROOT = Path(os.getcwd()).resolve()`
- paths are constrained to stay inside `_PROJECT_ROOT`
- large files are refused for safety/context control
- repository search is case-sensitive

Implication:

- this server is really a code-reader rooted at the launch directory, not a general repo manager

### `log_server.py`

Exposed tools:

- `search_log(query, date_pattern="*")`

Important details:

- `LOG_DIR = Path("./logs").resolve()`
- search is case-insensitive
- it returns the matched contiguous block between first and last match offsets per file
- result count is capped

Implication:

- this server assumes a log directory layout and works best when logs are organized by day as `<date>.text`

## Synthetic Fixture Strategy

The fixtures are not random examples; they are the evaluation surface for the debugger.

### `basic_test_project`

- simpler "DataBridge" service
- intentionally direct failures
- good for verifying end-to-end tool use and basic tracing

Embedded bug themes:

- upstream connection failure
- payload schema/type mismatch
- divide-by-zero in computation

### `simple_test_project`

- "PulseMetrics" v2 service
- more realistic layering:
  - API
  - services
  - repositories
  - infrastructure
  - utilities
- bugs are still explicit and didactic

Embedded bug themes:

- DB pool exhaustion
- cache timeout / stampede path
- `None` rows causing aggregation failure
- malformed JWT validation path

### `simple_test_project_production_like`

- same general PulseMetrics shape
- more production-like log messages, env-based config, and naming
- best fixture when evaluating real-world diagnosis quality

This is the fixture behind log chains like:

- `[k7l8m9n0]` cache timeout -> HTTP 503
- `[t5u6v7w8]` summary aggregation crash -> HTTP 500
- `[x9y0z1a2]` DB pool exhaustion -> HTTP 500
- malformed bearer token requests -> HTTP 401

## How To Read a Failure Chain

For the v2 fixtures, the useful mental path is usually:

1. request enters `src/main.py` middleware and gets a request ID
2. API file logs the endpoint intent
3. service layer logs orchestration steps
4. repository or infrastructure layer logs the first warning/error
5. API translates the exception to an HTTP status
6. `main.py` logs the final request line

In many cases the first meaningful cause is not the final exception.

Example:

- repository returns `None` after a DB timeout
- service later crashes with `AttributeError`
- the real diagnosis should mention both the upstream trigger and the local crash site

## Things Worth Checking Before Any Change

1. What directory will each MCP server be launched from?
2. Which fixture is the intended target?
3. Is the user evaluating prompting behavior, server behavior, or fixture realism?
4. Is the worktree already dirty?

The repo often contains local/generated changes, so avoid assuming a clean baseline.

## Useful Future Improvements

- split prompt logic out of `client.py` into a dedicated module
- add a launcher script that starts repo/log servers with the correct cwd for a chosen fixture
- add regression tests for:
  - prompt-builder outputs
  - `read_file` safety behavior
  - `search_log` extraction behavior
- document one or two canonical debugging sessions end-to-end

## If Something Is Unclear

The most useful question to ask is:

"Which fixture and which launch directories do you want the debugger to target?"

That one answer usually removes most ambiguity in this repo.
