"""
test_project_generator_v2.py

Generates a mid-size FastAPI project — "PulseMetrics" — a real-time analytics
ingestion service. Designed to stress-test an AI MCP Debugger with:

  • Deep log chains (10–30 lines per error event)
  • Production-realistic failure modes:
      Bug #1 — DB connection pool exhaustion     (OperationalError)
      Bug #2 — Downstream cache stampede          (TimeoutError → stale fallback)
      Bug #3 — Unhandled NoneType in aggregation  (AttributeError)
      Bug #4 — Malformed JWT / auth bypass        (ValueError in token decode)
  • Multi-layer call stacks (router → service → repository → infrastructure)
  • Mixed log levels (DEBUG, INFO, WARNING, ERROR) in every chain
  • Log format: YYYY-MM-DD_HH:MM:SS | LEVEL | filename             | LNN  | message
"""

from pathlib import Path


# ═══════════════════════════════════════════════════════════════════════════════
#  PROJECT SOURCE FILES
# ═══════════════════════════════════════════════════════════════════════════════

# ── src/main.py ────────────────────────────────────────────────────────────────
MAIN_PY = '''\
"""
src/main.py

PulseMetrics — real-time analytics ingestion API.
Initialises the FastAPI application, registers middleware, and mounts routers.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import time

from src.api.ingest import router as ingest_router
from src.api.query import router as query_router
from src.api.auth import router as auth_router
from src.infrastructure.db import Database
from src.infrastructure.cache import CacheClient
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
db     = Database()
cache  = CacheClient()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("PulseMetrics starting up — initialising infrastructure.")
    await db.connect()
    await cache.connect()
    logger.info("Infrastructure ready. Accepting requests.")
    yield
    logger.info("PulseMetrics shutting down.")
    await db.disconnect()
    await cache.disconnect()


app = FastAPI(
    title="PulseMetrics API",
    description="High-throughput real-time analytics ingestion and query service.",
    version="2.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://dashboard.internal"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_lifecycle(request: Request, call_next):
    """Attach a request ID, measure latency, and log every response."""
    import uuid
    request_id = str(uuid.uuid4())[:8]
    request.state.request_id = request_id
    start = time.perf_counter()

    logger.debug(
        f"[{request_id}] Incoming {request.method} {request.url.path} "
        f"from {request.client.host if request.client else \'unknown\'}"
    )

    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000

    level = "INFO" if response.status_code < 400 else "ERROR"
    logger.log(
        level,
        f"[{request_id}] {request.method} {request.url.path} "
        f"→ HTTP {response.status_code} in {duration_ms:.1f}ms"
    )
    return response


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    rid = getattr(request.state, "request_id", "?")
    logger.error(
        f"[{rid}] Unhandled {type(exc).__name__} on {request.url.path}: {exc}"
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error", "request_id": rid},
    )


app.include_router(ingest_router, prefix="/api/v2/ingest", tags=["ingest"])
app.include_router(query_router,  prefix="/api/v2/query",  tags=["query"])
app.include_router(auth_router,   prefix="/api/v2/auth",   tags=["auth"])


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "db": await db.ping(),
        "cache": await cache.ping(),
    }
'''

# ── src/api/ingest.py ──────────────────────────────────────────────────────────
INGEST_PY = '''\
"""
src/api/ingest.py

Ingest endpoints: accept metric events, validate, enrich, and persist.
"""

from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel, Field
from typing import Optional
import time

from src.services.ingest_service import IngestService
from src.services.auth_service import AuthService
from src.utils.logger import AppLogger

logger  = AppLogger(__name__)
router  = APIRouter()
svc     = IngestService()
auth    = AuthService()


class MetricEvent(BaseModel):
    source_id:  str
    metric:     str
    value:      float
    timestamp:  Optional[int] = None
    tags:       dict          = Field(default_factory=dict)


@router.post("/event")
async def ingest_event(
    event: MetricEvent,
    authorization: Optional[str] = Header(default=None),
) -> dict:
    """
    Accept a single metric event.

    Auth   → validate JWT bearer token.
    Enrich → resolve source_id to tenant context.
    Persist → write to time-series DB.
    """
    rid = "n/a"

    logger.info(f"[{rid}] Ingest request for metric=\'{event.metric}\' source=\'{event.source_id}\'.")

    # Auth check
    try:
        token = (authorization or "").removeprefix("Bearer ").strip()
        tenant = auth.validate_token(token)          # BUG #4 lurks here
        logger.info(f"[{rid}] Auth OK — tenant=\'{tenant}\'.")
    except ValueError as exc:
        logger.warning(f"[{rid}] Auth rejected for source=\'{event.source_id}\': {exc}")
        raise HTTPException(status_code=401, detail=str(exc))

    # Ingest
    try:
        result = await svc.ingest(tenant=tenant, event=event.model_dump())
        logger.info(f"[{rid}] Event accepted — event_id=\'{result[\'event_id\']}\'.")
        return result
    except Exception as exc:
        logger.error(f"[{rid}] Ingest pipeline failed for metric=\'{event.metric}\': {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/batch")
async def ingest_batch(
    events: list[MetricEvent],
    authorization: Optional[str] = Header(default=None),
) -> dict:
    """Accept a batch of metric events (max 500)."""
    rid = "n/a"
    logger.info(f"[{rid}] Batch ingest — {len(events)} events.")

    if len(events) > 500:
        raise HTTPException(status_code=422, detail="Batch size exceeds 500 events.")

    token  = (authorization or "").removeprefix("Bearer ").strip()
    tenant = auth.validate_token(token)

    results = []
    for ev in events:
        try:
            r = await svc.ingest(tenant=tenant, event=ev.model_dump())
            results.append(r)
        except Exception as exc:
            logger.error(f"[{rid}] Batch item failed — metric=\'{ev.metric}\': {exc}")
            results.append({"error": str(exc)})

    ok    = sum(1 for r in results if "event_id" in r)
    failed = len(results) - ok
    logger.info(f"[{rid}] Batch complete — ok={ok} failed={failed}.")
    return {"ok": ok, "failed": failed, "results": results}
'''

# ── src/api/query.py ───────────────────────────────────────────────────────────
QUERY_PY = '''\
"""
src/api/query.py

Query endpoints: aggregate metrics, return summaries and time-series data.
"""

from fastapi import APIRouter, HTTPException, Query as QParam
from typing import Optional

from src.services.query_service import QueryService
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc    = QueryService()


@router.get("/summary/{source_id}")
async def get_summary(
    source_id: str,
    metric:    str,
    window:    int  = QParam(default=3600, ge=60, le=86400),
    tenant_id: Optional[str] = None,
) -> dict:
    """
    Return an aggregated summary for a metric over a rolling time window.
    Results are served from cache when available.
    """
    logger.info(
        f"Summary query — source=\'{source_id}\' metric=\'{metric}\' window={window}s."
    )

    try:
        result = await svc.get_summary(
            source_id=source_id,
            metric=metric,
            window=window,
            tenant_id=tenant_id,
        )
        logger.info(
            f"Summary OK — source=\'{source_id}\' metric=\'{metric}\' "
            f"cached={result.get(\'from_cache\', False)}."
        )
        return result
    except TimeoutError as exc:
        logger.error(f"Summary timed out — source=\'{source_id}\': {exc}")
        raise HTTPException(status_code=503, detail=str(exc))
    except AttributeError as exc:
        logger.error(f"Aggregation error — source=\'{source_id}\': {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        logger.error(f"Unexpected error in summary — source=\'{source_id}\': {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/timeseries/{source_id}")
async def get_timeseries(
    source_id: str,
    metric:    str,
    start:     int,
    end:       int,
) -> dict:
    """Return raw time-series data between two Unix timestamps."""
    logger.info(
        f"Timeseries query — source=\'{source_id}\' metric=\'{metric}\' "
        f"range=[{start},{end}]."
    )
    try:
        result = await svc.get_timeseries(source_id, metric, start, end)
        return result
    except Exception as exc:
        logger.error(f"Timeseries failed — source=\'{source_id}\': {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
'''

# ── src/api/auth.py ────────────────────────────────────────────────────────────
AUTH_PY = '''\
"""
src/api/auth.py

Authentication endpoints: token issuance and validation.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.services.auth_service import AuthService
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc    = AuthService()


class TokenRequest(BaseModel):
    client_id:     str
    client_secret: str
    tenant_id:     str


@router.post("/token")
async def issue_token(body: TokenRequest) -> dict:
    """Issue a short-lived JWT for a verified client."""
    logger.info(f"Token request — client=\'{body.client_id}\' tenant=\'{body.tenant_id}\'.")
    try:
        token = svc.issue_token(body.client_id, body.client_secret, body.tenant_id)
        logger.info(f"Token issued — client=\'{body.client_id}\'.")
        return {"access_token": token, "token_type": "bearer", "expires_in": 3600}
    except PermissionError as exc:
        logger.warning(f"Token denied — client=\'{body.client_id}\': {exc}")
        raise HTTPException(status_code=403, detail=str(exc))


@router.post("/validate")
async def validate_token(token: str) -> dict:
    """Validate a token and return its claims."""
    try:
        tenant = svc.validate_token(token)
        return {"valid": True, "tenant": tenant}
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
'''

# ── src/services/ingest_service.py ────────────────────────────────────────────
INGEST_SERVICE_PY = '''\
"""
src/services/ingest_service.py

Orchestrates the metric ingestion pipeline:
  validate → enrich → write to DB → invalidate cache
"""

import uuid
from typing import Any

from src.repositories.metric_repo import MetricRepository
from src.repositories.source_repo import SourceRepository
from src.infrastructure.cache import CacheClient
from src.utils.logger import AppLogger

logger      = AppLogger(__name__)
metric_repo = MetricRepository()
source_repo = SourceRepository()
cache       = CacheClient()


class IngestService:
    """Coordinates validation, enrichment, and persistence of metric events."""

    async def ingest(self, tenant: str, event: dict[str, Any]) -> dict[str, Any]:
        """
        Run the full ingest pipeline for one event.

        Steps:
          1. Validate the event schema.
          2. Resolve the source to its configuration.
          3. Write the metric to the time-series store.
          4. Invalidate any cached summaries for this source.

        Returns:
            dict containing event_id and storage metadata.

        Raises:
            ValueError:        On schema validation failure.
            RuntimeError:      If the source cannot be resolved.
            OperationalError:  If the DB write fails.            ← BUG #1 path
        """
        event_id = str(uuid.uuid4())[:12]
        logger.debug(
            f"[ingest] Entering pipeline — event_id={event_id} "
            f"tenant={tenant} metric={event.get(\'metric\')}."
        )

        # Step 1 — validate
        self._validate(event)
        logger.debug(f"[ingest] Schema validation passed — event_id={event_id}.")

        # Step 2 — resolve source
        logger.debug(f"[ingest] Resolving source_id={event.get(\'source_id\')}.")
        source_cfg = await source_repo.get(event["source_id"], tenant)
        if source_cfg is None:
            raise RuntimeError(
                f"Source \'{event[\'source_id\']}\' not found for tenant \'{tenant}\'."
            )
        logger.debug(
            f"[ingest] Source resolved — retention={source_cfg.get(\'retention_days\')}d."
        )

        # Step 3 — persist
        logger.info(
            f"[ingest] Writing metric to DB — event_id={event_id} "
            f"source={event[\'source_id\']}."
        )
        meta = await metric_repo.write(tenant, event, source_cfg)   # raises on pool exhaustion
        logger.info(f"[ingest] DB write OK — event_id={event_id} rows={meta.get(\'rows\')}.")

        # Step 4 — cache invalidation
        cache_key = f"summary:{tenant}:{event[\'source_id\']}:{event[\'metric\']}"
        await cache.delete(cache_key)
        logger.debug(f"[ingest] Cache key invalidated — key={cache_key}.")

        return {"event_id": event_id, "tenant": tenant, "meta": meta}

    @staticmethod
    def _validate(event: dict[str, Any]) -> None:
        required = {"source_id", "metric", "value"}
        missing  = required - set(event.keys())
        if missing:
            raise ValueError(f"Missing required fields: {missing}")
        if not isinstance(event["value"], (int, float)):
            raise ValueError(f"Field \'value\' must be numeric, got {type(event[\'value\']).__name__}.")
'''

# ── src/services/query_service.py ────────────────────────────────────────────
QUERY_SERVICE_PY = '''\
"""
src/services/query_service.py

Orchestrates metric queries: cache-first read with DB fallback and aggregation.
"""

from typing import Any, Optional

from src.repositories.metric_repo import MetricRepository
from src.infrastructure.cache import CacheClient
from src.utils.logger import AppLogger

logger      = AppLogger(__name__)
metric_repo = MetricRepository()
cache       = CacheClient()


class QueryService:
    """Serves metric summaries and time-series data."""

    async def get_summary(
        self,
        source_id: str,
        metric:    str,
        window:    int,
        tenant_id: Optional[str],
    ) -> dict[str, Any]:
        """
        Return an aggregated summary (min/max/avg/count) over a rolling window.

        Cache-first strategy:
          1. Check cache for pre-computed result.
          2. On cache miss: query DB, aggregate, write back to cache.

        Raises:
            TimeoutError:   If the cache read exceeds its deadline.   ← BUG #2 path
            AttributeError: If the DB result set is unexpectedly None. ← BUG #3 path
        """
        tenant = tenant_id or "default"
        cache_key = f"summary:{tenant}:{source_id}:{metric}:{window}"

        logger.debug(
            f"[query] Summary requested — source={source_id} "
            f"metric={metric} window={window}s tenant={tenant}."
        )

        # ── Cache read ────────────────────────────────────────────────────────
        logger.debug(f"[query] Checking cache — key={cache_key}.")
        cached = await cache.get(cache_key)           # raises TimeoutError on stampede

        if cached is not None:
            logger.info(f"[query] Cache hit — key={cache_key}.")
            return {**cached, "from_cache": True}

        logger.info(
            f"[query] Cache miss — falling back to DB "
            f"source={source_id} metric={metric}."
        )

        # ── DB read ───────────────────────────────────────────────────────────
        logger.debug(
            f"[query] Querying metric_repo — source={source_id} window={window}s."
        )
        rows = await metric_repo.read_window(source_id, metric, window, tenant)
        logger.debug(f"[query] DB returned {len(rows) if rows is not None else \'None\'} rows.")

        # ── Aggregation ───────────────────────────────────────────────────────
        logger.debug(f"[query] Beginning aggregation — source={source_id}.")
        summary = self._aggregate(rows)              # raises AttributeError when rows is None
        logger.info(
            f"[query] Aggregation complete — "
            f"count={summary[\'count\']} avg={summary[\'avg\']:.4f}."
        )

        # ── Write back to cache ───────────────────────────────────────────────
        await cache.set(cache_key, summary, ttl=window)
        logger.debug(f"[query] Cache populated — key={cache_key} ttl={window}s.")

        return {**summary, "from_cache": False}

    async def get_timeseries(
        self,
        source_id: str,
        metric:    str,
        start:     int,
        end:       int,
    ) -> dict[str, Any]:
        """Return raw time-series rows for a given range."""
        tenant = "default"
        logger.debug(
            f"[query] Timeseries fetch — source={source_id} "
            f"metric={metric} range=[{start},{end}]."
        )
        rows = await metric_repo.read_range(source_id, metric, start, end, tenant)
        return {"source_id": source_id, "metric": metric, "rows": rows}

    @staticmethod
    def _aggregate(rows) -> dict[str, Any]:         # line 74
        """
        Compute min/max/avg/p95 over a list of metric row dicts.

        Raises:
            AttributeError: When rows is None (DB returned nothing, not []).   ← BUG #3
        """
        values = [r["value"] for r in rows]          # line 79 — AttributeError if rows is None
        if not values:
            return {"count": 0, "min": None, "max": None, "avg": 0.0, "p95": None}

        sorted_vals = sorted(values)
        p95_idx     = int(len(sorted_vals) * 0.95)
        return {
            "count": len(values),
            "min":   sorted_vals[0],
            "max":   sorted_vals[-1],
            "avg":   sum(values) / len(values),
            "p95":   sorted_vals[p95_idx],
        }
'''

# ── src/services/auth_service.py ─────────────────────────────────────────────
AUTH_SERVICE_PY = '''\
"""
src/services/auth_service.py

JWT token issuance and validation for the PulseMetrics API.
Uses a simple HMAC-SHA256 scheme (production would use RS256 + JWKS).
"""

import hmac
import hashlib
import base64
import json
import time

from src.utils.logger import AppLogger

logger = AppLogger(__name__)

_SECRET    = "pulse-secret-do-not-ship"
_ALGORITHM = "HS256"


class AuthService:
    """Issues and validates short-lived JWT bearer tokens."""

    def issue_token(self, client_id: str, client_secret: str, tenant_id: str) -> str:
        """
        Issue a signed token for a verified client.

        Raises:
            PermissionError: If client_secret is incorrect.
        """
        if client_secret != "valid-secret":
            raise PermissionError(f"Invalid credentials for client \'{client_id}\'.")

        header  = base64.urlsafe_b64encode(
            json.dumps({"alg": _ALGORITHM, "typ": "JWT"}).encode()
        ).decode().rstrip("=")
        payload = base64.urlsafe_b64encode(
            json.dumps({
                "sub":    client_id,
                "tenant": tenant_id,
                "iat":    int(time.time()),
                "exp":    int(time.time()) + 3600,
            }).encode()
        ).decode().rstrip("=")

        sig = hmac.new(
            _SECRET.encode(), f"{header}.{payload}".encode(), hashlib.sha256
        ).digest()
        sig_b64 = base64.urlsafe_b64encode(sig).decode().rstrip("=")

        return f"{header}.{payload}.{sig_b64}"

    def validate_token(self, token: str) -> str:
        """
        Validate a bearer token and return the tenant_id.

        Raises:
            ValueError: On malformed token, bad signature, or expiry.   ← BUG #4
        """
        logger.debug(f"Validating token (length={len(token)}).")

        parts = token.split(".")
        if len(parts) != 3:                          # line 57
            raise ValueError(                        # line 58 — raises on malformed/empty token
                f"Malformed token: expected 3 parts, got {len(parts)}. "
                "Ensure the Authorization header is \'Bearer <token>\'."
            )

        header_b64, payload_b64, sig_b64 = parts

        # Verify signature
        expected_sig = hmac.new(
            _SECRET.encode(),
            f"{header_b64}.{payload_b64}".encode(),
            hashlib.sha256,
        ).digest()
        expected_b64 = base64.urlsafe_b64encode(expected_sig).decode().rstrip("=")

        if not hmac.compare_digest(sig_b64, expected_b64):
            raise ValueError("Token signature verification failed.")

        # Decode payload
        padding = "=" * (4 - len(payload_b64) % 4)
        try:
            claims = json.loads(base64.urlsafe_b64decode(payload_b64 + padding))
        except Exception as exc:
            raise ValueError(f"Cannot decode token payload: {exc}")

        # Expiry check
        if claims.get("exp", 0) < time.time():
            raise ValueError(
                f"Token expired at {claims[\'exp\']}. Issue a new token via /api/v2/auth/token."
            )

        tenant = claims.get("tenant")
        logger.debug(f"Token valid — tenant={tenant} sub={claims.get(\'sub\')}.")
        return tenant
'''

# ── src/repositories/metric_repo.py ──────────────────────────────────────────
METRIC_REPO_PY = '''\
"""
src/repositories/metric_repo.py

Data-access layer for time-series metric storage.
Wraps the DB connection pool; all queries go through here.
"""

from typing import Any, Optional

from src.infrastructure.db import Database
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
_db    = Database()


class MetricRepository:
    """CRUD operations for metric events in the time-series store."""

    async def write(
        self,
        tenant:     str,
        event:      dict[str, Any],
        source_cfg: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Persist a metric event to the time-series table.

        Raises:
            OperationalError: When the DB connection pool is exhausted.  ← BUG #1
        """
        table = f"metrics_{tenant}"
        logger.debug(
            f"[metric_repo] Acquiring DB connection — table={table} "
            f"metric={event.get(\'metric\')}."
        )

        conn = await _db.acquire()                   # line 33 — raises OperationalError
        logger.debug(f"[metric_repo] Connection acquired from pool.")

        try:
            sql = (
                f"INSERT INTO {table} "
                "(source_id, metric, value, timestamp, tags) "
                "VALUES ($1, $2, $3, $4, $5)"
            )
            logger.debug(f"[metric_repo] Executing INSERT — metric={event.get(\'metric\')}.")
            result = await conn.execute(
                sql,
                event["source_id"],
                event["metric"],
                event["value"],
                event.get("timestamp"),
                event.get("tags", {}),
            )
            rows = int(result.split()[-1])
            logger.debug(f"[metric_repo] INSERT complete — rows={rows}.")
            return {"rows": rows, "table": table}
        finally:
            await _db.release(conn)
            logger.debug("[metric_repo] Connection returned to pool.")

    async def read_window(
        self,
        source_id: str,
        metric:    str,
        window:    int,
        tenant:    str,
    ) -> Optional[list[dict[str, Any]]]:
        """
        Read metric rows within a rolling time window.

        Returns None when the DB query times out (simulated).   ← BUG #3 root cause
        """
        table = f"metrics_{tenant}"
        logger.debug(
            f"[metric_repo] read_window — source={source_id} "
            f"metric={metric} window={window}s table={table}."
        )

        conn = await _db.acquire()
        try:
            # Simulated: for specific combination, DB query returns None
            # (e.g. statement timeout reached, driver returns None instead of [])
            if source_id == "sensor-404" and metric == "temperature":
                logger.warning(
                    f"[metric_repo] Query returned None for source={source_id} "
                    f"metric={metric} — possible statement timeout."
                )
                return None                          # ← triggers BUG #3 in query_service

            sql = (
                f"SELECT source_id, metric, value, timestamp "
                f"FROM {table} "
                f"WHERE source_id=$1 AND metric=$2 "
                f"AND timestamp > extract(epoch from now()) - $3"
            )
            rows = await conn.fetch(sql, source_id, metric, window)
            return [dict(r) for r in rows]
        finally:
            await _db.release(conn)

    async def read_range(
        self,
        source_id: str,
        metric:    str,
        start:     int,
        end:       int,
        tenant:    str,
    ) -> list[dict[str, Any]]:
        """Read raw rows between two Unix timestamps."""
        table = f"metrics_{tenant}"
        conn  = await _db.acquire()
        try:
            sql = (
                f"SELECT source_id, metric, value, timestamp "
                f"FROM {table} WHERE source_id=$1 AND metric=$2 "
                f"AND timestamp BETWEEN $3 AND $4"
            )
            rows = await conn.fetch(sql, source_id, metric, start, end)
            return [dict(r) for r in rows]
        finally:
            await _db.release(conn)
'''

# ── src/repositories/source_repo.py ──────────────────────────────────────────
SOURCE_REPO_PY = '''\
"""
src/repositories/source_repo.py

Data-access layer for source/sensor configuration records.
"""

from typing import Any, Optional

from src.infrastructure.db import Database
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
_db    = Database()


class SourceRepository:
    """Reads source configuration from the relational store."""

    async def get(self, source_id: str, tenant: str) -> Optional[dict[str, Any]]:
        """
        Fetch the configuration record for a source/sensor.

        Returns None if the source does not exist.
        """
        logger.debug(
            f"[source_repo] Fetching config — source_id={source_id} tenant={tenant}."
        )
        conn = await _db.acquire()
        try:
            sql = (
                "SELECT source_id, tenant, retention_days, sampling_rate, active "
                "FROM sources WHERE source_id=$1 AND tenant=$2"
            )
            row = await conn.fetchrow(sql, source_id, tenant)
            if row is None:
                logger.warning(
                    f"[source_repo] Source not found — "
                    f"source_id={source_id} tenant={tenant}."
                )
                return None
            return dict(row)
        finally:
            await _db.release(conn)
'''

# ── src/infrastructure/db.py ──────────────────────────────────────────────────
DB_PY = '''\
"""
src/infrastructure/db.py

Database connection pool wrapper (simulated asyncpg interface).

BUG #1 lives here: the pool size is hard-capped at 5 connections.
Under load, acquire() raises OperationalError when all connections
are checked out and the wait exceeds the timeout.
"""

import asyncio
from src.utils.logger import AppLogger

logger = AppLogger(__name__)

_POOL_SIZE    = 5
_ACQUIRE_TIMEOUT = 2.0   # seconds


class OperationalError(Exception):
    """Raised when a database operation cannot be completed."""


class _FakeConn:
    """Simulated asyncpg connection."""
    async def execute(self, sql, *args):
        await asyncio.sleep(0.01)
        return "INSERT 0 1"

    async def fetch(self, sql, *args):
        await asyncio.sleep(0.01)
        return []

    async def fetchrow(self, sql, *args):
        await asyncio.sleep(0.01)
        return {"source_id": args[0], "tenant": args[1],
                "retention_days": 30, "sampling_rate": 1.0, "active": True}


class Database:
    """Singleton-style connection pool manager."""

    _pool:     list   = []
    _checked:  int    = 0
    _max_conns: int   = _POOL_SIZE

    async def connect(self):
        logger.info(f"[db] Connecting — pool_size={self._max_conns}.")
        self._pool = [_FakeConn() for _ in range(self._max_conns)]
        self._checked = 0
        logger.info("[db] Connection pool ready.")

    async def disconnect(self):
        logger.info("[db] Closing connection pool.")
        self._pool.clear()

    async def acquire(self):
        """
        Check out a connection from the pool.

        Raises:
            OperationalError: When pool is exhausted and timeout is exceeded.  ← BUG #1
        """
        logger.debug(
            f"[db] acquire() called — "
            f"available={len(self._pool) - self._checked}/{len(self._pool)}."
        )

        if self._checked >= len(self._pool):          # line 68
            # Pool exhausted — simulate timeout wait
            logger.warning(
                f"[db] Pool exhausted ({self._checked}/{len(self._pool)} in use). "
                f"Waiting up to {_ACQUIRE_TIMEOUT}s for a free connection."
            )
            await asyncio.sleep(_ACQUIRE_TIMEOUT)
            raise OperationalError(                   # line 75 — BUG #1 raise site
                f"Connection pool exhausted: all {len(self._pool)} connections "
                f"are checked out. Increase pool_size or reduce query concurrency."
            )

        self._checked += 1
        conn = self._pool[self._checked - 1]
        logger.debug(f"[db] Connection acquired — checked_out={self._checked}.")
        return conn

    async def release(self, conn):
        if self._checked > 0:
            self._checked -= 1
        logger.debug(f"[db] Connection released — checked_out={self._checked}.")

    async def ping(self) -> str:
        return "ok" if self._pool else "disconnected"
'''

# ── src/infrastructure/cache.py ───────────────────────────────────────────────
CACHE_PY = '''\
"""
src/infrastructure/cache.py

Redis cache client (simulated).

BUG #2 lives here: under high concurrency the cache layer enters a
"stampede" state where multiple callers all miss simultaneously and
the get() call times out waiting for a lock that is never released.
"""

import asyncio
from typing import Any, Optional
from src.utils.logger import AppLogger

logger = AppLogger(__name__)

_DEFAULT_TTL = 300   # seconds
_GET_TIMEOUT = 1.5   # seconds before raising TimeoutError


class CacheClient:
    """Simulated async Redis client with in-process dict storage."""

    _store:    dict = {}
    _stampede: bool = False   # toggled by the test harness to trigger BUG #2

    async def connect(self):
        logger.info("[cache] Connecting to Redis.")
        self._store = {}
        logger.info("[cache] Redis connection established.")

    async def disconnect(self):
        logger.info("[cache] Closing Redis connection.")
        self._store.clear()

    async def get(self, key: str) -> Optional[Any]:
        """
        Fetch a cached value.

        Raises:
            TimeoutError: On cache stampede / lock contention.   ← BUG #2
        """
        logger.debug(f"[cache] GET {key}.")

        if self._stampede:                            # line 40
            logger.warning(
                f"[cache] Stampede detected on key={key}. "
                f"Waiting {_GET_TIMEOUT}s for distributed lock."
            )
            await asyncio.sleep(_GET_TIMEOUT)
            raise TimeoutError(                       # line 46 — BUG #2 raise site
                f"Cache GET timed out after {_GET_TIMEOUT}s waiting for lock on key=\'{key}\'. "
                "Possible cache stampede — consider probabilistic early expiry."
            )

        value = self._store.get(key)
        logger.debug(f"[cache] GET {key} -> {'HIT' if value else 'MISS'}.")
        return value

    async def set(self, key: str, value: Any, ttl: int = _DEFAULT_TTL):
        logger.debug(f"[cache] SET {key} ttl={ttl}s.")
        self._store[key] = value

    async def delete(self, key: str):
        removed = self._store.pop(key, None)
        logger.debug(f"[cache] DEL {key} -> {'removed' if removed else 'not found'}.")

    async def ping(self) -> str:
        return "ok"
'''

# ── src/utils/logger.py ───────────────────────────────────────────────────────
LOGGER_PY = '''\
"""
src/utils/logger.py

Structured application logger.

Format : YYYY-MM-DD_HH:MM:SS | LEVEL | filename             | LNN  | message
Levels : DEBUG, INFO, WARNING, ERROR

A chain-divider line is appended after every HTTP response entry so that
request chains are visually separated in the log file.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

_FMT      = "%(asctime)s | %(levelname)-7s | %(filename)-22s | L%(lineno)-4d | %(message)s"
_DATE_FMT = "%Y-%m-%d_%H:%M:%S"

_CHAIN_DIVIDER = "--- " * 22


def _log_path() -> Path:
    return LOG_DIR / f"{datetime.now().strftime(\'%Y-%m-%d\')}.text"


class AppLogger:
    """Thin facade over stdlib logging with file + stderr sinks."""

    _LEVELS = {
        "DEBUG":   logging.DEBUG,
        "INFO":    logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR":   logging.ERROR,
    }

    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)
        if not self._logger.handlers:
            self._logger.setLevel(logging.DEBUG)
            fmt = logging.Formatter(_FMT, datefmt=_DATE_FMT)
            fh  = logging.FileHandler(_log_path(), encoding="utf-8")
            fh.setFormatter(fmt)
            sh  = logging.StreamHandler(sys.stderr)
            sh.setFormatter(fmt)
            self._logger.addHandler(fh)
            self._logger.addHandler(sh)

    def debug(self, msg: str)   -> None: self._logger.debug(msg)
    def info(self, msg: str)    -> None: self._logger.info(msg);    self._divider(msg)
    def warning(self, msg: str) -> None: self._logger.warning(msg)
    def error(self, msg: str)   -> None: self._logger.error(msg);   self._divider(msg)

    def log(self, level: str, msg: str) -> None:
        lvl = self._LEVELS.get(level.upper(), logging.INFO)
        self._logger.log(lvl, msg)
        if "→ HTTP" in msg:
            self._write_divider()

    def _divider(self, msg: str) -> None:
        if "→ HTTP" in msg:
            self._write_divider()

    def _write_divider(self) -> None:
        try:
            with open(_log_path(), "a", encoding="utf-8") as fh:
                fh.write(_CHAIN_DIVIDER + "\\n")
        except OSError:
            pass
'''


# ═══════════════════════════════════════════════════════════════════════════════
#  LOG FILE CONTENTS
#  Format: YYYY-MM-DD_HH:MM:SS | LEVEL   | filename               | LNN  | message
#  Each ERROR chain: 10–30 lines including DEBUG/INFO/WARNING context
# ═══════════════════════════════════════════════════════════════════════════════

LOG_APR_14 = """\
2026-04-14_06:00:01 | INFO    | main.py               | L62  | PulseMetrics starting up — initialising infrastructure.
2026-04-14_06:00:01 | INFO    | db.py                 | L43  | [db] Connecting — pool_size=5.
2026-04-14_06:00:02 | INFO    | db.py                 | L45  | [db] Connection pool ready.
2026-04-14_06:00:02 | INFO    | cache.py              | L30  | [cache] Connecting to Redis.
2026-04-14_06:00:02 | INFO    | cache.py              | L32  | [cache] Redis connection established.
2026-04-14_06:00:02 | INFO    | main.py               | L64  | Infrastructure ready. Accepting requests.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-14_08:14:05 | DEBUG   | main.py               | L75  | [a1b2c3d4] Incoming POST /api/v2/ingest/event from 10.0.1.44
2026-04-14_08:14:05 | INFO    | ingest.py             | L42  | [a1b2c3d4] Ingest request for metric='cpu_usage' source='host-prod-01'.
2026-04-14_08:14:05 | DEBUG   | auth_service.py       | L48  | Validating token (length=172).
2026-04-14_08:14:05 | DEBUG   | auth_service.py       | L76  | Token valid — tenant=acme sub=pipeline-agent.
2026-04-14_08:14:05 | INFO    | ingest.py             | L48  | [a1b2c3d4] Auth OK — tenant='acme'.
2026-04-14_08:14:05 | DEBUG   | ingest_service.py     | L46  | [ingest] Entering pipeline — event_id=f3a8b21c9d01 tenant=acme metric=cpu_usage.
2026-04-14_08:14:05 | DEBUG   | ingest_service.py     | L52  | [ingest] Schema validation passed — event_id=f3a8b21c9d01.
2026-04-14_08:14:05 | DEBUG   | ingest_service.py     | L55  | [ingest] Resolving source_id=host-prod-01.
2026-04-14_08:14:05 | DEBUG   | source_repo.py        | L27  | [source_repo] Fetching config — source_id=host-prod-01 tenant=acme.
2026-04-14_08:14:05 | DEBUG   | db.py                 | L62  | [db] acquire() called — available=5/5.
2026-04-14_08:14:05 | DEBUG   | db.py                 | L82  | [db] Connection acquired — checked_out=1.
2026-04-14_08:14:05 | DEBUG   | db.py                 | L88  | [db] Connection released — checked_out=0.
2026-04-14_08:14:05 | DEBUG   | ingest_service.py     | L59  | [ingest] Source resolved — retention=30d.
2026-04-14_08:14:05 | INFO    | ingest_service.py     | L63  | [ingest] Writing metric to DB — event_id=f3a8b21c9d01 source=host-prod-01.
2026-04-14_08:14:05 | DEBUG   | metric_repo.py        | L31  | [metric_repo] Acquiring DB connection — table=metrics_acme metric=cpu_usage.
2026-04-14_08:14:05 | DEBUG   | db.py                 | L62  | [db] acquire() called — available=5/5.
2026-04-14_08:14:05 | DEBUG   | db.py                 | L82  | [db] Connection acquired — checked_out=1.
2026-04-14_08:14:05 | DEBUG   | metric_repo.py        | L35  | [metric_repo] Connection acquired from pool.
2026-04-14_08:14:05 | DEBUG   | metric_repo.py        | L45  | [metric_repo] Executing INSERT — metric=cpu_usage.
2026-04-14_08:14:05 | DEBUG   | metric_repo.py        | L54  | [metric_repo] INSERT complete — rows=1.
2026-04-14_08:14:05 | DEBUG   | db.py                 | L88  | [db] Connection released — checked_out=0.
2026-04-14_08:14:05 | DEBUG   | metric_repo.py        | L57  | [metric_repo] Connection returned to pool.
2026-04-14_08:14:05 | INFO    | ingest_service.py     | L66  | [ingest] DB write OK — event_id=f3a8b21c9d01 rows=1.
2026-04-14_08:14:05 | DEBUG   | ingest_service.py     | L70  | [ingest] Cache key invalidated — key=summary:acme:host-prod-01:cpu_usage.
2026-04-14_08:14:05 | INFO    | ingest.py             | L55  | [a1b2c3d4] Event accepted — event_id='f3a8b21c9d01'.
2026-04-14_08:14:05 | INFO    | main.py               | L83  | [a1b2c3d4] POST /api/v2/ingest/event → HTTP 200 in 11.2ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-14_09:30:00 | DEBUG   | main.py               | L75  | [e5f6a7b8] Incoming POST /api/v2/ingest/event from 10.0.2.11
2026-04-14_09:30:00 | INFO    | ingest.py             | L42  | [e5f6a7b8] Ingest request for metric='memory_rss' source='host-prod-02'.
2026-04-14_09:30:00 | DEBUG   | auth_service.py       | L48  | Validating token (length=0).
2026-04-14_09:30:00 | WARNING | auth_service.py       | L57  | Malformed token: expected 3 parts, got 1.
2026-04-14_09:30:00 | ERROR   | auth_service.py       | L58  | ValueError: Malformed token: expected 3 parts, got 1. Ensure the Authorization header is 'Bearer <token>'.
2026-04-14_09:30:00 | WARNING | ingest.py             | L50  | [e5f6a7b8] Auth rejected for source='host-prod-02': Malformed token: expected 3 parts, got 1.
2026-04-14_09:30:00 | INFO    | main.py               | L83  | [e5f6a7b8] POST /api/v2/ingest/event → HTTP 401 in 3.1ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-14_10:45:12 | DEBUG   | main.py               | L75  | [c9d0e1f2] Incoming GET /api/v2/query/summary/sensor-404 from 10.0.3.55
2026-04-14_10:45:12 | INFO    | query.py              | L35  | Summary query — source='sensor-404' metric='temperature' window=3600s.
2026-04-14_10:45:12 | DEBUG   | query_service.py      | L45  | [query] Summary requested — source=sensor-404 metric=temperature window=3600s tenant=default.
2026-04-14_10:45:12 | DEBUG   | query_service.py      | L51  | [query] Checking cache — key=summary:default:sensor-404:temperature:3600.
2026-04-14_10:45:12 | DEBUG   | cache.py              | L34  | [cache] GET summary:default:sensor-404:temperature:3600.
2026-04-14_10:45:12 | DEBUG   | cache.py              | L50  | [cache] GET summary:default:sensor-404:temperature:3600 -> MISS.
2026-04-14_10:45:12 | INFO    | query_service.py      | L57  | [query] Cache miss — falling back to DB source=sensor-404 metric=temperature.
2026-04-14_10:45:12 | DEBUG   | query_service.py      | L61  | [query] Querying metric_repo — source=sensor-404 window=3600s.
2026-04-14_10:45:12 | DEBUG   | metric_repo.py        | L73  | [metric_repo] read_window — source=sensor-404 metric=temperature window=3600s table=metrics_default.
2026-04-14_10:45:12 | DEBUG   | db.py                 | L62  | [db] acquire() called — available=5/5.
2026-04-14_10:45:12 | DEBUG   | db.py                 | L82  | [db] Connection acquired — checked_out=1.
2026-04-14_10:45:12 | WARNING | metric_repo.py        | L83  | [metric_repo] Query returned None for source=sensor-404 metric=temperature — possible statement timeout.
2026-04-14_10:45:12 | DEBUG   | db.py                 | L88  | [db] Connection released — checked_out=1.
2026-04-14_10:45:12 | DEBUG   | query_service.py      | L63  | [query] DB returned None rows.
2026-04-14_10:45:12 | DEBUG   | query_service.py      | L66  | [query] Beginning aggregation — source=sensor-404.
2026-04-14_10:45:12 | ERROR   | query_service.py      | L79  | AttributeError: 'NoneType' object has no attribute '__iter__'
2026-04-14_10:45:12 | ERROR   | query.py              | L50  | Aggregation error — source='sensor-404': 'NoneType' object has no attribute '__iter__'
2026-04-14_10:45:12 | ERROR   | main.py               | L83  | [c9d0e1f2] GET /api/v2/query/summary/sensor-404 → HTTP 500 in 28.4ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-14_11:00:00 | DEBUG   | main.py               | L75  | [g3h4i5j6] Incoming POST /api/v2/ingest/event from 10.0.1.44
2026-04-14_11:00:00 | INFO    | ingest.py             | L42  | [g3h4i5j6] Ingest request for metric='disk_io' source='host-prod-01'.
2026-04-14_11:00:00 | DEBUG   | auth_service.py       | L48  | Validating token (length=172).
2026-04-14_11:00:00 | DEBUG   | auth_service.py       | L76  | Token valid — tenant=acme sub=pipeline-agent.
2026-04-14_11:00:00 | INFO    | ingest.py             | L48  | [g3h4i5j6] Auth OK — tenant='acme'.
2026-04-14_11:00:00 | DEBUG   | ingest_service.py     | L46  | [ingest] Entering pipeline — event_id=7c3d912f4e55 tenant=acme metric=disk_io.
2026-04-14_11:00:00 | DEBUG   | ingest_service.py     | L52  | [ingest] Schema validation passed — event_id=7c3d912f4e55.
2026-04-14_11:00:00 | DEBUG   | ingest_service.py     | L55  | [ingest] Resolving source_id=host-prod-01.
2026-04-14_11:00:00 | DEBUG   | source_repo.py        | L27  | [source_repo] Fetching config — source_id=host-prod-01 tenant=acme.
2026-04-14_11:00:00 | DEBUG   | db.py                 | L62  | [db] acquire() called — available=5/5.
2026-04-14_11:00:00 | DEBUG   | db.py                 | L82  | [db] Connection acquired — checked_out=1.
2026-04-14_11:00:00 | DEBUG   | db.py                 | L88  | [db] Connection released — checked_out=0.
2026-04-14_11:00:00 | DEBUG   | ingest_service.py     | L59  | [ingest] Source resolved — retention=30d.
2026-04-14_11:00:00 | INFO    | ingest_service.py     | L63  | [ingest] Writing metric to DB — event_id=7c3d912f4e55 source=host-prod-01.
2026-04-14_11:00:00 | DEBUG   | metric_repo.py        | L31  | [metric_repo] Acquiring DB connection — table=metrics_acme metric=disk_io.
2026-04-14_11:00:00 | DEBUG   | db.py                 | L62  | [db] acquire() called — available=0/5.
2026-04-14_11:00:00 | WARNING | db.py                 | L71  | [db] Pool exhausted (5/5 in use). Waiting up to 2.0s for a free connection.
2026-04-14_11:00:02 | ERROR   | db.py                 | L75  | OperationalError: Connection pool exhausted: all 5 connections are checked out. Increase pool_size or reduce query concurrency.
2026-04-14_11:00:02 | ERROR   | metric_repo.py        | L33  | [metric_repo] Failed to acquire DB connection — OperationalError raised.
2026-04-14_11:00:02 | ERROR   | ingest_service.py     | L63  | [ingest] DB write failed for event_id=7c3d912f4e55: Connection pool exhausted.
2026-04-14_11:00:02 | ERROR   | ingest.py             | L59  | [g3h4i5j6] Ingest pipeline failed for metric='disk_io': Connection pool exhausted.
2026-04-14_11:00:02 | ERROR   | main.py               | L83  | [g3h4i5j6] POST /api/v2/ingest/event → HTTP 500 in 2041.3ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
"""

LOG_APR_15 = """\
2026-04-15_07:00:00 | INFO    | main.py               | L62  | PulseMetrics starting up — initialising infrastructure.
2026-04-15_07:00:00 | INFO    | db.py                 | L43  | [db] Connecting — pool_size=5.
2026-04-15_07:00:01 | INFO    | db.py                 | L45  | [db] Connection pool ready.
2026-04-15_07:00:01 | INFO    | cache.py              | L30  | [cache] Connecting to Redis.
2026-04-15_07:00:01 | INFO    | cache.py              | L32  | [cache] Redis connection established.
2026-04-15_07:00:01 | INFO    | main.py               | L64  | Infrastructure ready. Accepting requests.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-15_08:05:30 | DEBUG   | main.py               | L75  | [k7l8m9n0] Incoming GET /api/v2/query/summary/host-prod-03 from 10.0.5.21
2026-04-15_08:05:30 | INFO    | query.py              | L35  | Summary query — source='host-prod-03' metric='net_rx_bytes' window=1800s.
2026-04-15_08:05:30 | DEBUG   | query_service.py      | L45  | [query] Summary requested — source=host-prod-03 metric=net_rx_bytes window=1800s tenant=default.
2026-04-15_08:05:30 | DEBUG   | query_service.py      | L51  | [query] Checking cache — key=summary:default:host-prod-03:net_rx_bytes:1800.
2026-04-15_08:05:30 | DEBUG   | cache.py              | L34  | [cache] GET summary:default:host-prod-03:net_rx_bytes:1800.
2026-04-15_08:05:30 | WARNING | cache.py              | L42  | [cache] Stampede detected on key=summary:default:host-prod-03:net_rx_bytes:1800. Waiting 1.5s for distributed lock.
2026-04-15_08:05:31 | ERROR   | cache.py              | L46  | TimeoutError: Cache GET timed out after 1.5s waiting for lock on key='summary:default:host-prod-03:net_rx_bytes:1800'. Possible cache stampede — consider probabilistic early expiry.
2026-04-15_08:05:31 | ERROR   | query_service.py      | L51  | [query] Cache read failed — key=summary:default:host-prod-03:net_rx_bytes:1800: Cache GET timed out.
2026-04-15_08:05:31 | ERROR   | query.py              | L42  | Summary timed out — source='host-prod-03': Cache GET timed out after 1.5s.
2026-04-15_08:05:31 | ERROR   | main.py               | L83  | [k7l8m9n0] GET /api/v2/query/summary/host-prod-03 → HTTP 503 in 1521.8ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-15_09:12:44 | DEBUG   | main.py               | L75  | [p1q2r3s4] Incoming POST /api/v2/ingest/event from 10.0.1.77
2026-04-15_09:12:44 | INFO    | ingest.py             | L42  | [p1q2r3s4] Ingest request for metric='latency_p99' source='api-gateway'.
2026-04-15_09:12:44 | DEBUG   | auth_service.py       | L48  | Validating token (length=0).
2026-04-15_09:12:44 | WARNING | auth_service.py       | L57  | Malformed token: expected 3 parts, got 1.
2026-04-15_09:12:44 | ERROR   | auth_service.py       | L58  | ValueError: Malformed token: expected 3 parts, got 1. Ensure the Authorization header is 'Bearer <token>'.
2026-04-15_09:12:44 | WARNING | ingest.py             | L50  | [p1q2r3s4] Auth rejected for source='api-gateway': Malformed token: expected 3 parts, got 1.
2026-04-15_09:12:44 | INFO    | main.py               | L83  | [p1q2r3s4] POST /api/v2/ingest/event → HTTP 401 in 2.8ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-15_10:00:00 | DEBUG   | main.py               | L75  | [t5u6v7w8] Incoming GET /api/v2/query/summary/sensor-404 from 10.0.3.55
2026-04-15_10:00:00 | INFO    | query.py              | L35  | Summary query — source='sensor-404' metric='temperature' window=3600s.
2026-04-15_10:00:00 | DEBUG   | query_service.py      | L45  | [query] Summary requested — source=sensor-404 metric=temperature window=3600s tenant=default.
2026-04-15_10:00:00 | DEBUG   | query_service.py      | L51  | [query] Checking cache — key=summary:default:sensor-404:temperature:3600.
2026-04-15_10:00:00 | DEBUG   | cache.py              | L34  | [cache] GET summary:default:sensor-404:temperature:3600.
2026-04-15_10:00:00 | DEBUG   | cache.py              | L50  | [cache] GET summary:default:sensor-404:temperature:3600 -> MISS.
2026-04-15_10:00:00 | INFO    | query_service.py      | L57  | [query] Cache miss — falling back to DB source=sensor-404 metric=temperature.
2026-04-15_10:00:00 | DEBUG   | query_service.py      | L61  | [query] Querying metric_repo — source=sensor-404 window=3600s.
2026-04-15_10:00:00 | DEBUG   | metric_repo.py        | L73  | [metric_repo] read_window — source=sensor-404 metric=temperature window=3600s table=metrics_default.
2026-04-15_10:00:00 | DEBUG   | db.py                 | L62  | [db] acquire() called — available=5/5.
2026-04-15_10:00:00 | DEBUG   | db.py                 | L82  | [db] Connection acquired — checked_out=1.
2026-04-15_10:00:00 | WARNING | metric_repo.py        | L83  | [metric_repo] Query returned None for source=sensor-404 metric=temperature — possible statement timeout.
2026-04-15_10:00:00 | DEBUG   | db.py                 | L88  | [db] Connection released — checked_out=0.
2026-04-15_10:00:00 | DEBUG   | query_service.py      | L63  | [query] DB returned None rows.
2026-04-15_10:00:00 | DEBUG   | query_service.py      | L66  | [query] Beginning aggregation — source=sensor-404.
2026-04-15_10:00:00 | ERROR   | query_service.py      | L79  | AttributeError: 'NoneType' object has no attribute '__iter__'
2026-04-15_10:00:00 | ERROR   | query.py              | L50  | Aggregation error — source='sensor-404': 'NoneType' object has no attribute '__iter__'
2026-04-15_10:00:00 | ERROR   | main.py               | L83  | [t5u6v7w8] GET /api/v2/query/summary/sensor-404 → HTTP 500 in 31.7ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-15_11:30:00 | DEBUG   | main.py               | L75  | [x9y0z1a2] Incoming POST /api/v2/ingest/event from 10.0.1.44
2026-04-15_11:30:00 | INFO    | ingest.py             | L42  | [x9y0z1a2] Ingest request for metric='cpu_usage' source='host-prod-05'.
2026-04-15_11:30:00 | DEBUG   | auth_service.py       | L48  | Validating token (length=172).
2026-04-15_11:30:00 | DEBUG   | auth_service.py       | L76  | Token valid — tenant=acme sub=pipeline-agent.
2026-04-15_11:30:00 | INFO    | ingest.py             | L48  | [x9y0z1a2] Auth OK — tenant='acme'.
2026-04-15_11:30:00 | DEBUG   | ingest_service.py     | L46  | [ingest] Entering pipeline — event_id=9e1f234a5b67 tenant=acme metric=cpu_usage.
2026-04-15_11:30:00 | DEBUG   | ingest_service.py     | L52  | [ingest] Schema validation passed — event_id=9e1f234a5b67.
2026-04-15_11:30:00 | DEBUG   | ingest_service.py     | L55  | [ingest] Resolving source_id=host-prod-05.
2026-04-15_11:30:00 | DEBUG   | source_repo.py        | L27  | [source_repo] Fetching config — source_id=host-prod-05 tenant=acme.
2026-04-15_11:30:00 | DEBUG   | db.py                 | L62  | [db] acquire() called — available=5/5.
2026-04-15_11:30:00 | DEBUG   | db.py                 | L82  | [db] Connection acquired — checked_out=1.
2026-04-15_11:30:00 | DEBUG   | db.py                 | L88  | [db] Connection released — checked_out=0.
2026-04-15_11:30:00 | DEBUG   | ingest_service.py     | L59  | [ingest] Source resolved — retention=30d.
2026-04-15_11:30:00 | INFO    | ingest_service.py     | L63  | [ingest] Writing metric to DB — event_id=9e1f234a5b67 source=host-prod-05.
2026-04-15_11:30:00 | DEBUG   | metric_repo.py        | L31  | [metric_repo] Acquiring DB connection — table=metrics_acme metric=cpu_usage.
2026-04-15_11:30:00 | DEBUG   | db.py                 | L62  | [db] acquire() called — available=0/5.
2026-04-15_11:30:00 | WARNING | db.py                 | L71  | [db] Pool exhausted (5/5 in use). Waiting up to 2.0s for a free connection.
2026-04-15_11:30:02 | ERROR   | db.py                 | L75  | OperationalError: Connection pool exhausted: all 5 connections are checked out. Increase pool_size or reduce query concurrency.
2026-04-15_11:30:02 | ERROR   | metric_repo.py        | L33  | [metric_repo] Failed to acquire DB connection — OperationalError raised.
2026-04-15_11:30:02 | ERROR   | ingest_service.py     | L63  | [ingest] DB write failed for event_id=9e1f234a5b67: Connection pool exhausted.
2026-04-15_11:30:02 | ERROR   | ingest.py             | L59  | [x9y0z1a2] Ingest pipeline failed for metric='cpu_usage': Connection pool exhausted.
2026-04-15_11:30:02 | ERROR   | main.py               | L83  | [x9y0z1a2] POST /api/v2/ingest/event → HTTP 500 in 2038.1ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-15_13:15:00 | DEBUG   | main.py               | L75  | [b3c4d5e6] Incoming GET /api/v2/query/summary/host-prod-07 from 10.0.5.21
2026-04-15_13:15:00 | INFO    | query.py              | L35  | Summary query — source='host-prod-07' metric='net_tx_bytes' window=1800s.
2026-04-15_13:15:00 | DEBUG   | query_service.py      | L45  | [query] Summary requested — source=host-prod-07 metric=net_tx_bytes window=1800s tenant=default.
2026-04-15_13:15:00 | DEBUG   | query_service.py      | L51  | [query] Checking cache — key=summary:default:host-prod-07:net_tx_bytes:1800.
2026-04-15_13:15:00 | DEBUG   | cache.py              | L34  | [cache] GET summary:default:host-prod-07:net_tx_bytes:1800.
2026-04-15_13:15:00 | WARNING | cache.py              | L42  | [cache] Stampede detected on key=summary:default:host-prod-07:net_tx_bytes:1800. Waiting 1.5s for distributed lock.
2026-04-15_13:15:01 | ERROR   | cache.py              | L46  | TimeoutError: Cache GET timed out after 1.5s waiting for lock on key='summary:default:host-prod-07:net_tx_bytes:1800'. Possible cache stampede — consider probabilistic early expiry.
2026-04-15_13:15:01 | ERROR   | query_service.py      | L51  | [query] Cache read failed — key=summary:default:host-prod-07:net_tx_bytes:1800: Cache GET timed out.
2026-04-15_13:15:01 | ERROR   | query.py              | L42  | Summary timed out — source='host-prod-07': Cache GET timed out after 1.5s.
2026-04-15_13:15:01 | ERROR   | main.py               | L83  | [b3c4d5e6] GET /api/v2/query/summary/host-prod-07 → HTTP 503 in 1519.3ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

def generate() -> Path:
    root = Path("test_project_v2")

    files: dict[Path, str] = {
        # App core
        root / "src" / "__init__.py":                              "",
        root / "src" / "main.py":                                  MAIN_PY,
        # API layer
        root / "src" / "api" / "__init__.py":                      "",
        root / "src" / "api" / "ingest.py":                        INGEST_PY,
        root / "src" / "api" / "query.py":                         QUERY_PY,
        root / "src" / "api" / "auth.py":                          AUTH_PY,
        # Service layer
        root / "src" / "services" / "__init__.py":                 "",
        root / "src" / "services" / "ingest_service.py":           INGEST_SERVICE_PY,
        root / "src" / "services" / "query_service.py":            QUERY_SERVICE_PY,
        root / "src" / "services" / "auth_service.py":             AUTH_SERVICE_PY,
        # Repository layer
        root / "src" / "repositories" / "__init__.py":             "",
        root / "src" / "repositories" / "metric_repo.py":          METRIC_REPO_PY,
        root / "src" / "repositories" / "source_repo.py":          SOURCE_REPO_PY,
        # Infrastructure layer
        root / "src" / "infrastructure" / "__init__.py":           "",
        root / "src" / "infrastructure" / "db.py":                 DB_PY,
        root / "src" / "infrastructure" / "cache.py":              CACHE_PY,
        # Utilities
        root / "src" / "utils" / "__init__.py":                    "",
        root / "src" / "utils" / "logger.py":                      LOGGER_PY,
        # Logs
        root / "logs" / "2026-04-14.text":                         LOG_APR_14,
        root / "logs" / "2026-04-15.text":                         LOG_APR_15,
    }

    for path, content in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    return root


def print_tree(root: Path) -> None:
    print(f"\n{'─' * 64}")
    print("  Generated project structure")
    print(f"{'─' * 64}")

    def _walk(path: Path, prefix: str = "") -> None:
        if path.is_dir():
            print(f"{prefix}DIRECTORY: {path.name}/")
            for child in sorted(path.iterdir(), key=lambda p: (p.is_file(), p.name)):
                _walk(child, prefix + "  ")
        else:
            print(f"{prefix}FILE: {path.name}")

    _walk(root)
    print(f"{'─' * 64}\n")


def main() -> None:
    root = generate()

    # Syntax-check all generated Python files
    import ast
    errors = []
    for f in root.rglob("*.py"):
        src = f.read_text(encoding="utf-8")
        if not src.strip():
            continue
        try:
            ast.parse(src)
        except SyntaxError as e:
            errors.append(f"{f.relative_to(root)}: {e}")

    print(f"\n{'═' * 64}")
    print("  PulseMetrics test_project_v2 generated")
    print(f"{'═' * 64}\n")

    if errors:
        print("  ⚠  Syntax errors detected:")
        for e in errors:
            print(f"     {e}")
    else:
        print("  ✔  All Python files pass syntax check.\n")

    print("  Bugs embedded")
    print("  ┌─ Bug #1  OperationalError   → db.py:75             (pool exhausted)")
    print("  ├─ Bug #2  TimeoutError       → cache.py:46          (stampede)")
    print("  ├─ Bug #3  AttributeError     → query_service.py:79  (None rows)")
    print("  └─ Bug #4  ValueError         → auth_service.py:58   (malformed JWT)\n")

    print("  Log chains per bug")
    print("  ┌─ Bug #1  22 log lines  (DEBUG → WARNING → ERROR × 4 → HTTP 500)")
    print("  ├─ Bug #2  10 log lines  (DEBUG × 5 → WARNING → ERROR × 3 → HTTP 503)")
    print("  ├─ Bug #3  17 log lines  (DEBUG × 9 → WARNING → ERROR × 3 → HTTP 500)")
    print("  └─ Bug #4   7 log lines  (DEBUG → WARNING → ERROR × 2 → HTTP 401)\n")

    print("  Log files")
    print("  ┌─ logs/2026-04-14.text  (4 chains: healthy, auth-fail, none-rows, pool-exhaustion)")
    print("  └─ logs/2026-04-15.text  (5 chains: healthy start, stampede, auth-fail, none-rows, pool-exhaustion)\n")

    print_tree(root)


if __name__ == "__main__":
    main()