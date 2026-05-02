# Fixture Reference

This repo contains synthetic debugging targets used to test the MCP debugger.

## Overview

The fixtures are organized as progressively more realistic debugging scenarios.

- `basic_test_project`
- `simple_test_project`
- `simple_test_project_production_like`

The later fixtures are not separate product ideas; they are evolutions of the same goal: producing believable logs and traceable failure chains for a log-first debugging workflow.

## `basic_test_project`

Service theme:

- "DataBridge" internal aggregation API

Files of interest:

- `test_projects/basic_test_project/test_project/src/api/endpoints.py`
- `test_projects/basic_test_project/test_project/src/services/external_client.py`
- `test_projects/basic_test_project/test_project/src/services/data_processor.py`
- `test_projects/basic_test_project/logs/2026-04-11.text`
- `test_projects/basic_test_project/logs/2026-04-12.text`

Embedded bug set:

1. `ConnectionError`
   - source: `external_client.py`
   - behavior: upstream feed fetch always fails

2. `ValueError`
   - source: `data_processor.py`
   - behavior: malformed `"score"` value cannot be cast to `int`

3. `ZeroDivisionError`
   - source: `data_processor.py`
   - behavior: score computation does not guard `divisor == 0`

Why it exists:

- simple path-following evaluation
- useful for validating the basic prompt/tool loop

## `simple_test_project`

Service theme:

- "PulseMetrics" real-time analytics ingestion/query service

Structure:

- `src/api/`
- `src/services/`
- `src/repositories/`
- `src/infrastructure/`
- `src/utils/`

Embedded bug set:

1. DB pool exhaustion
   - user-visible effect: HTTP 500 on ingest
   - upstream cause: connection acquisition path fails

2. Cache timeout / stampede path
   - user-visible effect: HTTP 503 on summary requests
   - upstream cause: cache `get` waits on refresh/lock and times out

3. Aggregation crash on `None`
   - user-visible effect: HTTP 500 on summary requests
   - upstream cause: repository returns `None`
   - local crash: aggregation assumes iterable rows

4. JWT validation failure
   - user-visible effect: HTTP 401 on ingest
   - cause: malformed or missing bearer token

Why it exists:

- introduces layered tracing across API, service, repository, and infrastructure modules

## `simple_test_project_production_like`

Service theme:

- same PulseMetrics concept, but with more production-like naming and logs

What changes from the simpler v2 fixture:

- env-driven config is used more consistently
- logs read more like a real service
- infrastructure naming is less didactic
- failure chains are more believable while preserving traceability

Representative request IDs from `logs/2026-04-15.text`:

- `k7l8m9n0`
  - path: `GET /api/v2/query/summary/host-prod-03`
  - result: HTTP 503
  - diagnosis shape: cache refresh lock / timeout path

- `t5u6v7w8`
  - path: `GET /api/v2/query/summary/boiler-room-a17`
  - result: HTTP 500
  - diagnosis shape: DB-side read yields `None`, then aggregation crashes on `rows.__iter__()`

- `x9y0z1a2`
  - path: `POST /api/v2/ingest/event`
  - result: HTTP 500
  - diagnosis shape: DB acquire timeout / pool exhaustion

- malformed token requests
  - result: HTTP 401
  - diagnosis shape: JWT validator rejects malformed bearer input

Why this fixture matters most:

- it is the best proxy for realistic tool-driven debugging behavior in this repo

## Choosing a Fixture

Use `basic_test_project` when:

- testing prompt obedience
- testing simple code-to-log linking
- checking whether the model can find obvious causes

Use `simple_test_project` when:

- testing multi-layer tracing
- iterating on prompt wording
- wanting slightly cleaner, more didactic failure paths

Use `simple_test_project_production_like` when:

- testing realistic diagnosis quality
- validating that the debugger can separate upstream trigger from downstream crash
- evaluating how well the system handles believable operational logs

## Launch Reminder

When using these fixtures, remember:

- `repo_server.py` should point at the source tree you want exposed
- `log_server.py` should point at the directory that contains `logs/`

That setup matters more than anything else when the debugger seems confused.
