"""
project_generator.py

Generates the PulseMetrics analytics ingestion service source tree.

Layout produced
---------------
  project_generator.py
  test_project_v2/
    src/
      main.py
      api/         ingest.py  query.py  auth.py
      services/    ingest_service.py  query_service.py  auth_service.py
      repositories/metric_repo.py  source_repo.py
      infrastructure/db.py  cache.py  telemetry.py
      utils/       logger.py
  logs/
    2026-04-14.text
    2026-04-15.text
"""

from pathlib import Path


# ============================================================================
#  SOURCE FILES
# ============================================================================

MAIN_PY = '''\
"""
src/main.py

PulseMetrics real-time analytics ingestion API.
Initialises the FastAPI application, middleware, and routers.
"""

from contextlib import asynccontextmanager
import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.api.auth import router as auth_router
from src.api.ingest import router as ingest_router
from src.api.query import router as query_router
from src.infrastructure.cache import CacheClient
from src.infrastructure.db import Database
from src.infrastructure.telemetry import TelemetryClient
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
db = Database()
cache = CacheClient()
telemetry = TelemetryClient()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("PulseMetrics starting up - initialising infrastructure.")
    await db.connect()
    await cache.connect()
    await telemetry.connect()
    logger.info("Infrastructure ready. Accepting requests.")
    yield
    logger.info("PulseMetrics shutting down.")
    await db.disconnect()
    await cache.disconnect()
    await telemetry.disconnect()


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
    import uuid

    request_id = str(uuid.uuid4())[:8]
    request.state.request_id = request_id
    started_at = time.perf_counter()

    logger.debug(
        f"[{request_id}] Incoming {request.method} {request.url.path} "
        f"from {request.client.host if request.client else 'unknown'}"
    )

    response = await call_next(request)
    duration_ms = (time.perf_counter() - started_at) * 1000

    level = "INFO" if response.status_code < 400 else "ERROR"
    logger.log(
        level,
        f"[{request_id}] {request.method} {request.url.path} "
        f"-> HTTP {response.status_code} in {duration_ms:.1f}ms",
    )
    return response


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    request_id = getattr(request.state, "request_id", "?")
    logger.error(
        f"[{request_id}] Unhandled {type(exc).__name__} on {request.url.path}: {exc}"
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error", "request_id": request_id},
    )


app.include_router(ingest_router, prefix="/api/v2/ingest", tags=["ingest"])
app.include_router(query_router, prefix="/api/v2/query", tags=["query"])
app.include_router(auth_router, prefix="/api/v2/auth", tags=["auth"])


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "db": await db.ping(),
        "cache": await cache.ping(),
        "telemetry": await telemetry.ping(),
    }
'''

INGEST_PY = '''\
"""
src/api/ingest.py

Ingest endpoints: accept metric events, validate, enrich, and persist.
"""

from typing import Optional

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from src.services.auth_service import AuthService
from src.services.ingest_service import IngestService
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = IngestService()
auth = AuthService()


class MetricEvent(BaseModel):
    source_id: str
    metric: str
    value: float
    timestamp: Optional[int] = None
    tags: dict = Field(default_factory=dict)


@router.post("/event")
async def ingest_event(
    event: MetricEvent,
    authorization: Optional[str] = Header(default=None),
) -> dict:
    rid = "n/a"

    logger.info(
        f"[{rid}] Ingest request for metric='{event.metric}' source='{event.source_id}'."
    )

    try:
        token = (authorization or "").removeprefix("Bearer ").strip()
        tenant = auth.validate_token(token)
        logger.info(f"[{rid}] Auth OK - tenant='{tenant}'.")
    except ValueError as exc:
        logger.warning(
            f"[{rid}] Auth rejected for source='{event.source_id}': {exc}"
        )
        raise HTTPException(status_code=401, detail=str(exc))

    try:
        result = await svc.ingest(tenant=tenant, event=event.model_dump())
        logger.info(f"[{rid}] Event accepted - event_id='{result['event_id']}'.")
        return result
    except Exception as exc:
        logger.error(
            f"[{rid}] Ingest pipeline failed for metric='{event.metric}': {exc}"
        )
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/batch")
async def ingest_batch(
    events: list[MetricEvent],
    authorization: Optional[str] = Header(default=None),
) -> dict:
    rid = "n/a"
    logger.info(f"[{rid}] Batch ingest - {len(events)} events.")

    if len(events) > 500:
        raise HTTPException(status_code=422, detail="Batch size exceeds 500 events.")

    token = (authorization or "").removeprefix("Bearer ").strip()
    tenant = auth.validate_token(token)

    results = []
    for ev in events:
        try:
            result = await svc.ingest(tenant=tenant, event=ev.model_dump())
            results.append(result)
        except Exception as exc:
            logger.error(f"[{rid}] Batch item failed - metric='{ev.metric}': {exc}")
            results.append({"error": str(exc)})

    ok = sum(1 for result in results if "event_id" in result)
    failed = len(results) - ok
    logger.info(f"[{rid}] Batch complete - ok={ok} failed={failed}.")
    return {"ok": ok, "failed": failed, "results": results}
'''

QUERY_PY = '''\
"""
src/api/query.py

Query endpoints: aggregate metrics, return summaries and time-series data.
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query as QParam

from src.services.query_service import QueryService
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
router = APIRouter()
svc = QueryService()


@router.get("/summary/{source_id}")
async def get_summary(
    source_id: str,
    metric: str,
    window: int = QParam(default=3600, ge=60, le=86400),
    tenant_id: Optional[str] = None,
) -> dict:
    logger.info(
        f"Summary query - source='{source_id}' metric='{metric}' window={window}s."
    )

    try:
        result = await svc.get_summary(
            source_id=source_id,
            metric=metric,
            window=window,
            tenant_id=tenant_id,
        )
        logger.info(
            f"Summary OK - source='{source_id}' metric='{metric}' "
            f"cached={result.get('from_cache', False)}."
        )
        return result
    except TimeoutError as exc:
        logger.error(f"Summary timed out - source='{source_id}': {exc}")
        raise HTTPException(status_code=503, detail=str(exc))
    except AttributeError as exc:
        logger.error(f"Aggregation error - source='{source_id}': {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        logger.error(f"Unexpected error in summary - source='{source_id}': {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/timeseries/{source_id}")
async def get_timeseries(
    source_id: str,
    metric: str,
    start: int,
    end: int,
) -> dict:
    logger.info(
        f"Timeseries query - source='{source_id}' metric='{metric}' range=[{start},{end}]."
    )
    try:
        return await svc.get_timeseries(source_id, metric, start, end)
    except Exception as exc:
        logger.error(f"Timeseries failed - source='{source_id}': {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
'''

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
svc = AuthService()


class TokenRequest(BaseModel):
    client_id: str
    client_secret: str
    tenant_id: str


@router.post("/token")
async def issue_token(body: TokenRequest) -> dict:
    logger.info(
        f"Token request - client='{body.client_id}' tenant='{body.tenant_id}'."
    )
    try:
        token = svc.issue_token(body.client_id, body.client_secret, body.tenant_id)
        logger.info(f"Token issued - client='{body.client_id}'.")
        return {"access_token": token, "token_type": "bearer", "expires_in": 3600}
    except PermissionError as exc:
        logger.warning(f"Token denied - client='{body.client_id}': {exc}")
        raise HTTPException(status_code=403, detail=str(exc))


@router.post("/validate")
async def validate_token(token: str) -> dict:
    try:
        tenant = svc.validate_token(token)
        return {"valid": True, "tenant": tenant}
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
'''

INGEST_SERVICE_PY = '''\
"""
src/services/ingest_service.py

Orchestrates the metric ingestion pipeline:
  validate -> enrich -> write to DB -> forward to telemetry -> invalidate cache
"""

import uuid
from typing import Any

from src.infrastructure.cache import CacheClient
from src.infrastructure.telemetry import TelemetryClient
from src.repositories.metric_repo import MetricRepository
from src.repositories.source_repo import SourceRepository
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
metric_repo = MetricRepository()
source_repo = SourceRepository()
cache = CacheClient()
telemetry = TelemetryClient()


class IngestService:
    async def ingest(self, tenant: str, event: dict[str, Any]) -> dict[str, Any]:
        event_id = str(uuid.uuid4())[:12]
        logger.debug(
            f"[ingest] Entering pipeline - event_id={event_id} "
            f"tenant={tenant} metric={event.get('metric')}."
        )

        self._validate(event)
        logger.debug(f"[ingest] Schema validation passed - event_id={event_id}.")

        logger.debug(f"[ingest] Resolving source_id={event.get('source_id')}.")
        source_cfg = await source_repo.get(event["source_id"], tenant)
        if source_cfg is None:
            raise RuntimeError(
                f"Source '{event['source_id']}' not found for tenant '{tenant}'."
            )
        logger.debug(
            f"[ingest] Source resolved - retention={source_cfg.get('retention_days')}d."
        )

        logger.info(
            f"[ingest] Writing metric to TimescaleDB - event_id={event_id} "
            f"source={event['source_id']}."
        )
        try:
            meta = await metric_repo.write(tenant, event, source_cfg)
        except Exception as exc:
            logger.error(f"[ingest] DB write failed for event_id={event_id}: {exc}")
            raise

        logger.info(f"[ingest] DB write OK - event_id={event_id} rows={meta.get('rows')}.")

        await telemetry.emit(tenant, event_id, event)
        logger.debug(f"[ingest] Relay emit acknowledged - event_id={event_id}.")

        cache_key = f"summary:{tenant}:{event['source_id']}:{event['metric']}"
        await cache.delete(cache_key)
        logger.debug(f"[ingest] Cache key invalidated - key={cache_key}.")

        return {"event_id": event_id, "tenant": tenant, "meta": meta}

    @staticmethod
    def _validate(event: dict[str, Any]) -> None:
        required = {"source_id", "metric", "value"}
        missing = required - set(event.keys())
        if missing:
            raise ValueError(f"Missing required fields: {missing}")
        if not isinstance(event["value"], (int, float)):
            raise ValueError(
                f"Field 'value' must be numeric, got {type(event['value']).__name__}."
            )
'''

QUERY_SERVICE_PY = '''\
"""
src/services/query_service.py

Orchestrates metric queries: Redis cache-first read with TimescaleDB fallback.
"""

from typing import Any, Optional

from src.infrastructure.cache import CacheClient
from src.repositories.metric_repo import MetricRepository
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
metric_repo = MetricRepository()
cache = CacheClient()


class QueryService:
    async def get_summary(
        self,
        source_id: str,
        metric: str,
        window: int,
        tenant_id: Optional[str],
    ) -> dict[str, Any]:
        tenant = tenant_id or "default"
        cache_key = f"summary:{tenant}:{source_id}:{metric}:{window}"

        logger.debug(
            f"[query] Summary requested - source={source_id} "
            f"metric={metric} window={window}s tenant={tenant}."
        )

        logger.debug(f"[query] Checking Redis - key={cache_key}.")
        try:
            cached = await cache.get(cache_key)
        except TimeoutError as exc:
            logger.error(f"[query] Cache read failed - key={cache_key}: {exc}")
            raise

        if cached is not None:
            logger.info(f"[query] Cache hit - key={cache_key}.")
            return {**cached, "from_cache": True}

        logger.info(
            f"[query] Cache miss - falling back to TimescaleDB "
            f"source={source_id} metric={metric}."
        )

        logger.debug(
            f"[query] Querying metric_repo - source={source_id} window={window}s."
        )
        rows = await metric_repo.read_window(source_id, metric, window, tenant)
        logger.debug(
            f"[query] DB returned {len(rows) if rows is not None else 'None'} rows."
        )

        logger.debug(f"[query] Beginning aggregation - source={source_id}.")
        try:
            summary = self._aggregate(rows)
        except AttributeError as exc:
            logger.error(f"[query] Aggregation failed - source={source_id}: {exc}")
            raise

        logger.info(
            f"[query] Aggregation complete - "
            f"count={summary['count']} avg={summary['avg']:.4f}."
        )

        await cache.set(cache_key, summary, ttl=window)
        logger.debug(f"[query] Redis populated - key={cache_key} ttl={window}s.")

        return {**summary, "from_cache": False}

    async def get_timeseries(
        self,
        source_id: str,
        metric: str,
        start: int,
        end: int,
    ) -> dict[str, Any]:
        tenant = "default"
        logger.debug(
            f"[query] Timeseries fetch - source={source_id} "
            f"metric={metric} range=[{start},{end}]."
        )
        rows = await metric_repo.read_range(source_id, metric, start, end, tenant)
        return {"source_id": source_id, "metric": metric, "rows": rows}

    @staticmethod
    def _aggregate(rows) -> dict[str, Any]:
        iterator = rows.__iter__()
        values = [row["value"] for row in iterator]
        if not values:
            return {"count": 0, "min": None, "max": None, "avg": 0.0, "p95": None}

        sorted_vals = sorted(values)
        p95_idx = int(len(sorted_vals) * 0.95)
        return {
            "count": len(values),
            "min": sorted_vals[0],
            "max": sorted_vals[-1],
            "avg": sum(values) / len(values),
            "p95": sorted_vals[p95_idx],
        }
'''

AUTH_SERVICE_PY = '''\
"""
src/services/auth_service.py

JWT token issuance and validation for the PulseMetrics API.
HMAC-SHA256 symmetric signing; tokens expire after 3600 seconds.
"""

import base64
import hashlib
import hmac
import json
import os
import time

from src.utils.logger import AppLogger

logger = AppLogger(__name__)

_SECRET = os.getenv("PULSEMETRICS_JWT_SECRET", "pm-prod-hs256-fallback")
_ALGORITHM = "HS256"

_CLIENTS: dict[str, str] = {
    "pipeline-agent": "valid-secret",
    "dashboard-reader": "valid-secret",
    "ops-exporter": "valid-secret",
}


class AuthService:
    def issue_token(self, client_id: str, client_secret: str, tenant_id: str) -> str:
        expected = _CLIENTS.get(client_id)
        if expected is None or client_secret != expected:
            raise PermissionError(f"Invalid credentials for client '{client_id}'.")

        header = base64.urlsafe_b64encode(
            json.dumps({"alg": _ALGORITHM, "typ": "JWT"}).encode()
        ).decode().rstrip("=")
        payload = base64.urlsafe_b64encode(
            json.dumps(
                {
                    "sub": client_id,
                    "tenant": tenant_id,
                    "iat": int(time.time()),
                    "exp": int(time.time()) + 3600,
                }
            ).encode()
        ).decode().rstrip("=")

        sig = hmac.new(
            _SECRET.encode(),
            f"{header}.{payload}".encode(),
            hashlib.sha256,
        ).digest()
        sig_b64 = base64.urlsafe_b64encode(sig).decode().rstrip("=")

        return f"{header}.{payload}.{sig_b64}"

    def validate_token(self, token: str) -> str:
        logger.debug(f"Validating token (length={len(token)}).")

        parts = token.split(".")
        if len(parts) != 3:
            logger.warning(
                f"Rejecting token - expected 3 JWT segments, got {len(parts)}."
            )
            raise ValueError(
                f"Malformed token: expected 3 parts, got {len(parts)}. "
                "Ensure the Authorization header is 'Bearer <token>'."
            )

        header_b64, payload_b64, sig_b64 = parts

        expected_sig = hmac.new(
            _SECRET.encode(),
            f"{header_b64}.{payload_b64}".encode(),
            hashlib.sha256,
        ).digest()
        expected_b64 = base64.urlsafe_b64encode(expected_sig).decode().rstrip("=")

        if not hmac.compare_digest(sig_b64, expected_b64):
            logger.warning("Rejecting token - signature verification failed.")
            raise ValueError("Token signature verification failed.")

        padding = "=" * (-len(payload_b64) % 4)
        try:
            claims = json.loads(base64.urlsafe_b64decode(payload_b64 + padding))
        except Exception as exc:
            logger.warning(f"Rejecting token - payload decode failed: {exc}.")
            raise ValueError(f"Cannot decode token payload: {exc}")

        if claims.get("exp", 0) < time.time():
            logger.warning(f"Rejecting token - expired at {claims.get('exp')}.")
            raise ValueError(
                f"Token expired at {claims['exp']}. "
                "Issue a new token via /api/v2/auth/token."
            )

        tenant = claims.get("tenant")
        if not tenant:
            logger.warning("Rejecting token - tenant claim missing.")
            raise ValueError("Token payload missing tenant claim.")

        logger.debug(f"Token valid - tenant={tenant} sub={claims.get('sub')}.")
        return tenant
'''

METRIC_REPO_PY = '''\
"""
src/repositories/metric_repo.py

Data-access layer for time-series metric storage backed by TimescaleDB.
All queries are routed through the shared asyncpg connection pool in db.py.
"""

from typing import Any, Optional

import asyncpg

from src.infrastructure.db import Database, OperationalError
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
_db = Database()


class MetricRepository:
    async def write(
        self,
        tenant: str,
        event: dict[str, Any],
        source_cfg: dict[str, Any],
    ) -> dict[str, Any]:
        table = f"metrics_{tenant}"
        logger.debug(
            f"[metric_repo] Acquiring DB connection - table={table} "
            f"metric={event.get('metric')}."
        )

        try:
            conn = await _db.acquire()
        except OperationalError as exc:
            logger.error(
                f"[metric_repo] Failed to acquire DB connection - table={table}: {exc}"
            )
            raise

        logger.debug("[metric_repo] Connection acquired from pool.")

        try:
            sql = (
                f"INSERT INTO {table} "
                "(source_id, metric, value, timestamp, tags) "
                "VALUES ($1, $2, $3, $4, $5)"
            )
            logger.debug(
                f"[metric_repo] Executing INSERT - metric={event.get('metric')}."
            )
            result = await conn.execute(
                sql,
                event["source_id"],
                event["metric"],
                event["value"],
                event.get("timestamp"),
                event.get("tags", {}),
            )
            rows = int(result.split()[-1])
            logger.debug(f"[metric_repo] INSERT complete - rows={rows}.")
            return {"rows": rows, "table": table}
        finally:
            await _db.release(conn)
            logger.debug("[metric_repo] Connection returned to pool.")

    async def read_window(
        self,
        source_id: str,
        metric: str,
        window: int,
        tenant: str,
    ) -> Optional[list[dict[str, Any]]]:
        table = f"metrics_{tenant}"
        logger.debug(
            f"[metric_repo] read_window - source={source_id} "
            f"metric={metric} window={window}s table={table}."
        )

        conn = await _db.acquire()
        try:
            sql = (
                f"SELECT source_id, metric, value, timestamp "
                f"FROM {table} "
                f"WHERE source_id=$1 AND metric=$2 "
                f"AND timestamp > extract(epoch from now()) - $3"
            )
            try:
                rows = await conn.fetch(sql, source_id, metric, window)
            except asyncpg.exceptions.QueryCanceledError:
                logger.warning(
                    f"[metric_repo] Statement timeout reading source={source_id} "
                    f"metric={metric} window={window}s."
                )
                return None
            return [dict(row) for row in rows]
        finally:
            await _db.release(conn)

    async def read_range(
        self,
        source_id: str,
        metric: str,
        start: int,
        end: int,
        tenant: str,
    ) -> list[dict[str, Any]]:
        table = f"metrics_{tenant}"
        conn = await _db.acquire()
        try:
            sql = (
                f"SELECT source_id, metric, value, timestamp "
                f"FROM {table} WHERE source_id=$1 AND metric=$2 "
                f"AND timestamp BETWEEN $3 AND $4"
            )
            rows = await conn.fetch(sql, source_id, metric, start, end)
            return [dict(row) for row in rows]
        finally:
            await _db.release(conn)
'''

SOURCE_REPO_PY = '''\
"""
src/repositories/source_repo.py

Data-access layer for source configuration records stored in PostgreSQL.
"""

from typing import Any, Optional

from src.infrastructure.db import Database
from src.utils.logger import AppLogger

logger = AppLogger(__name__)
_db = Database()


class SourceRepository:
    async def get(self, source_id: str, tenant: str) -> Optional[dict[str, Any]]:
        logger.debug(
            f"[source_repo] Fetching config - source_id={source_id} tenant={tenant}."
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
                    f"[source_repo] Source not found - source_id={source_id} tenant={tenant}."
                )
                return None
            return dict(row)
        finally:
            await _db.release(conn)
'''

DB_PY = '''\
"""
src/infrastructure/db.py

asyncpg connection pool for the primary TimescaleDB cluster.
Configured from the runtime environment for production deployment.
"""

import asyncio
import os

import asyncpg

from src.utils.logger import AppLogger

logger = AppLogger(__name__)

_DSN = os.getenv(
    "PULSEMETRICS_DB_DSN",
    "postgresql://pulse_rw@ts-db-primary.pulsemetrics.svc.cluster.local:5432/pulsemetrics",
)
_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
_ACQUIRE_TIMEOUT = float(os.getenv("DB_ACQUIRE_TIMEOUT", "2.0"))


class OperationalError(Exception):
    """Raised when a database operation cannot be completed."""


class Database:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            instance = super().__new__(cls)
            instance._pool = None
            instance._checked = 0
            instance._max_conns = _POOL_SIZE
            cls._instance = instance
        return cls._instance

    async def connect(self):
        if self._pool is not None:
            return

        logger.info(
            f"[db] Opening asyncpg pool - dsn={_DSN!r} pool_size={self._max_conns}."
        )
        self._pool = await asyncpg.create_pool(
            dsn=_DSN,
            min_size=min(2, self._max_conns),
            max_size=self._max_conns,
            command_timeout=30,
        )
        self._checked = 0
        logger.info("[db] asyncpg pool ready.")

    async def disconnect(self):
        logger.info("[db] Closing asyncpg pool.")
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
        self._checked = 0

    async def acquire(self) -> asyncpg.Connection:
        if self._pool is None:
            raise OperationalError("Database pool has not been initialised.")

        logger.debug(
            f"[db] acquire() called - checked_out={self._checked}/{self._max_conns}."
        )

        try:
            conn = await self._pool.acquire(timeout=_ACQUIRE_TIMEOUT)
        except asyncio.TimeoutError as exc:
            logger.warning(
                f"[db] Pool acquire timed out after {_ACQUIRE_TIMEOUT}s "
                f"(checked_out={self._checked}/{self._max_conns})."
            )
            raise OperationalError(
                f"Timed out after {_ACQUIRE_TIMEOUT}s waiting for a database "
                f"connection from a pool of {self._max_conns}."
            ) from exc

        self._checked += 1
        logger.debug(f"[db] Connection acquired - checked_out={self._checked}.")
        return conn

    async def release(self, conn: asyncpg.Connection):
        if self._pool is not None and conn is not None:
            await self._pool.release(conn)
        if self._checked > 0:
            self._checked -= 1
        logger.debug(f"[db] Connection released - checked_out={self._checked}.")

    async def ping(self) -> str:
        if self._pool is None:
            return "disconnected"
        try:
            await self._pool.fetchval("SELECT 1")
            return "ok"
        except Exception:
            return "error"
'''

CACHE_PY = '''\
"""
src/infrastructure/cache.py

Redis 7 client via redis-py asyncio interface.
Summary requests use Redis as a read-through cache.
"""

import asyncio
import json
import os
from typing import Any, Optional

import redis.asyncio as aioredis

from src.utils.logger import AppLogger

logger = AppLogger(__name__)

_HOST = os.getenv("REDIS_HOST", "redis-primary.pulsemetrics.svc.cluster.local")
_PORT = int(os.getenv("REDIS_PORT", "6379"))
_DB = int(os.getenv("REDIS_DB", "0"))
_DEFAULT_TTL = int(os.getenv("REDIS_DEFAULT_TTL", "300"))
_GET_TIMEOUT = float(os.getenv("REDIS_GET_TIMEOUT", "1.5"))
_LOCK_POLL_INTERVAL = 0.05


class CacheClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            instance = super().__new__(cls)
            instance._client = None
            cls._instance = instance
        return cls._instance

    async def connect(self):
        if self._client is not None:
            return

        logger.info(f"[cache] Connecting to Redis - {_HOST}:{_PORT}/{_DB}.")
        self._client = aioredis.Redis(
            host=_HOST,
            port=_PORT,
            db=_DB,
            decode_responses=False,
            socket_connect_timeout=3,
            socket_timeout=2,
        )
        await self._client.ping()
        logger.info("[cache] Redis connection established.")

    async def disconnect(self):
        logger.info("[cache] Closing Redis connection.")
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get(self, key: str) -> Optional[Any]:
        if self._client is None:
            raise RuntimeError("Redis client has not been initialised.")

        logger.debug(f"[cache] GET {key}.")

        raw = await self._client.get(key)
        if raw is not None:
            logger.debug(f"[cache] GET {key} -> HIT.")
            return json.loads(raw)

        lock_key = f"{key}:refresh-lock"
        if not await self._client.exists(lock_key):
            logger.debug(f"[cache] GET {key} -> MISS.")
            return None

        logger.warning(
            f"[cache] Refresh lock present for key={key}. "
            f"Waiting up to {_GET_TIMEOUT}s for a hydrated value."
        )

        loop = asyncio.get_running_loop()
        deadline = loop.time() + _GET_TIMEOUT
        while loop.time() < deadline:
            await asyncio.sleep(_LOCK_POLL_INTERVAL)
            raw = await self._client.get(key)
            if raw is not None:
                logger.debug(f"[cache] GET {key} -> HIT after refresh wait.")
                return json.loads(raw)
            if not await self._client.exists(lock_key):
                logger.debug(f"[cache] GET {key} -> MISS after refresh wait.")
                return None

        raise TimeoutError(
            f"Cache GET timed out after {_GET_TIMEOUT}s waiting for refresh lock "
            f"on key='{key}'."
        )

    async def set(self, key: str, value: Any, ttl: int = _DEFAULT_TTL):
        if self._client is None:
            raise RuntimeError("Redis client has not been initialised.")
        logger.debug(f"[cache] SET {key} ttl={ttl}s.")
        await self._client.set(key, json.dumps(value), ex=ttl)

    async def delete(self, key: str):
        if self._client is None:
            raise RuntimeError("Redis client has not been initialised.")
        removed = await self._client.delete(key)
        logger.debug(
            f"[cache] DEL {key} -> {'removed' if removed else 'not found'}."
        )

    async def ping(self) -> str:
        if self._client is None:
            return "disconnected"
        try:
            await self._client.ping()
            return "ok"
        except Exception:
            return "error"
'''

TELEMETRY_PY = '''\
"""
src/infrastructure/telemetry.py

PulseMetrics telemetry relay client.
Forwards raw metric events to downstream consumers.
"""

import json
import os
from typing import Any

import httpx

from src.utils.logger import AppLogger

logger = AppLogger(__name__)

_TELEMETRY_ENDPOINT = os.getenv(
    "PULSEMETRICS_TELEMETRY_URL",
    "https://telemetry-gateway.pulsemetrics.internal/v1/streams/pulse-prod",
)
_TELEMETRY_API_KEY = os.getenv(
    "PULSEMETRICS_TELEMETRY_API_KEY",
    "pulsemetrics-relay-local",
)
_EMIT_TIMEOUT = float(os.getenv("PULSEMETRICS_TELEMETRY_TIMEOUT", "3.0"))


class TelemetryClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            instance = super().__new__(cls)
            instance._http = None
            cls._instance = instance
        return cls._instance

    async def connect(self):
        if self._http is not None:
            return

        logger.info(
            f"[telemetry] Initialising relay client - endpoint={_TELEMETRY_ENDPOINT}."
        )
        self._http = httpx.AsyncClient(
            base_url=_TELEMETRY_ENDPOINT,
            headers={
                "X-PulseMetrics-Key": _TELEMETRY_API_KEY,
                "Content-Type": "application/json",
            },
            timeout=_EMIT_TIMEOUT,
        )
        logger.info("[telemetry] Relay client ready.")

    async def disconnect(self):
        logger.info("[telemetry] Closing relay client.")
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    async def emit(self, tenant: str, event_id: str, event: dict[str, Any]) -> None:
        if self._http is None:
            raise RuntimeError("Telemetry client has not been initialised.")

        payload = {
            "tenant": tenant,
            "event_id": event_id,
            "source_id": event.get("source_id"),
            "metric": event.get("metric"),
            "value": event.get("value"),
            "timestamp": event.get("timestamp"),
        }
        try:
            response = await self._http.post("", content=json.dumps(payload))
            response.raise_for_status()
            logger.debug(
                f"[telemetry] Relay ack - event_id={event_id} status={response.status_code}."
            )
        except httpx.HTTPStatusError as exc:
            logger.warning(
                f"[telemetry] Relay rejected event_id={event_id}: "
                f"HTTP {exc.response.status_code}."
            )
        except Exception as exc:
            logger.warning(
                f"[telemetry] Relay emit failed for event_id={event_id}: {exc}."
            )

    async def ping(self) -> str:
        if self._http is None:
            return "disconnected"
        try:
            response = await self._http.get("/healthz")
            return "ok" if response.status_code == 200 else "degraded"
        except Exception:
            return "error"
'''

LOGGER_PY = '''\
"""
src/utils/logger.py

Structured application logger.

Format : YYYY-MM-DD_HH:MM:SS | LEVEL   | filename               | LNN  | message
Levels : DEBUG, INFO, WARNING, ERROR
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parents[3] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

_FMT = "%(asctime)s | %(levelname)-7s | %(filename)-22s | L%(lineno)-4d | %(message)s"
_DATE_FMT = "%Y-%m-%d_%H:%M:%S"
_CHAIN_DIVIDER = "--- " * 22


def _log_path() -> Path:
    return LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}.text"


class AppLogger:
    _LEVELS = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
    }

    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)
        if not self._logger.handlers:
            self._logger.setLevel(logging.DEBUG)
            fmt = logging.Formatter(_FMT, datefmt=_DATE_FMT)

            file_handler = logging.FileHandler(_log_path(), encoding="utf-8")
            file_handler.setFormatter(fmt)

            stderr_handler = logging.StreamHandler(sys.stderr)
            stderr_handler.setFormatter(fmt)

            self._logger.addHandler(file_handler)
            self._logger.addHandler(stderr_handler)

    def debug(self, msg: str) -> None:
        self._logger.debug(msg)

    def info(self, msg: str) -> None:
        self._logger.info(msg)
        self._divider(msg)

    def warning(self, msg: str) -> None:
        self._logger.warning(msg)

    def error(self, msg: str) -> None:
        self._logger.error(msg)
        self._divider(msg)

    def log(self, level: str, msg: str) -> None:
        lvl = self._LEVELS.get(level.upper(), logging.INFO)
        self._logger.log(lvl, msg)
        if "-> HTTP" in msg:
            self._write_divider()

    def _divider(self, msg: str) -> None:
        if "-> HTTP" in msg:
            self._write_divider()

    def _write_divider(self) -> None:
        try:
            with open(_log_path(), "a", encoding="utf-8") as fh:
                fh.write(_CHAIN_DIVIDER + "\\n")
        except OSError:
            pass
'''


# ============================================================================
#  LOG FILE CONTENTS
# ============================================================================

LOG_APR_14 = """\
2026-04-14_06:00:01 | INFO    | main.py               | L27  | PulseMetrics starting up - initialising infrastructure.
2026-04-14_06:00:01 | INFO    | db.py                 | L39  | [db] Opening asyncpg pool - dsn='postgresql://pulse_rw@ts-db-primary.pulsemetrics.svc.cluster.local:5432/pulsemetrics' pool_size=5.
2026-04-14_06:00:02 | INFO    | db.py                 | L47  | [db] asyncpg pool ready.
2026-04-14_06:00:02 | INFO    | cache.py              | L35  | [cache] Connecting to Redis - redis-primary.pulsemetrics.svc.cluster.local:6379/0.
2026-04-14_06:00:02 | INFO    | cache.py              | L45  | [cache] Redis connection established.
2026-04-14_06:00:02 | INFO    | telemetry.py          | L37  | [telemetry] Initialising relay client - endpoint=https://telemetry-gateway.pulsemetrics.internal/v1/streams/pulse-prod.
2026-04-14_06:00:02 | INFO    | telemetry.py          | L47  | [telemetry] Relay client ready.
2026-04-14_06:00:02 | INFO    | main.py               | L31  | Infrastructure ready. Accepting requests.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-14_08:14:05 | DEBUG   | main.py               | L55  | [a1b2c3d4] Incoming POST /api/v2/ingest/event from 10.0.1.44
2026-04-14_08:14:05 | INFO    | ingest.py             | L34  | [n/a] Ingest request for metric='cpu_usage' source='host-prod-01'.
2026-04-14_08:14:05 | DEBUG   | auth_service.py       | L46  | Validating token (length=172).
2026-04-14_08:14:05 | DEBUG   | auth_service.py       | L85  | Token valid - tenant=acme sub=pipeline-agent.
2026-04-14_08:14:05 | INFO    | ingest.py             | L40  | [n/a] Auth OK - tenant='acme'.
2026-04-14_08:14:05 | DEBUG   | ingest_service.py     | L25  | [ingest] Entering pipeline - event_id=f3a8b21c9d01 tenant=acme metric=cpu_usage.
2026-04-14_08:14:05 | DEBUG   | ingest_service.py     | L31  | [ingest] Schema validation passed - event_id=f3a8b21c9d01.
2026-04-14_08:14:05 | DEBUG   | ingest_service.py     | L33  | [ingest] Resolving source_id=host-prod-01.
2026-04-14_08:14:05 | DEBUG   | source_repo.py        | L19  | [source_repo] Fetching config - source_id=host-prod-01 tenant=acme.
2026-04-14_08:14:05 | DEBUG   | db.py                 | L58  | [db] acquire() called - checked_out=0/5.
2026-04-14_08:14:05 | DEBUG   | db.py                 | L73  | [db] Connection acquired - checked_out=1.
2026-04-14_08:14:05 | DEBUG   | db.py                 | L80  | [db] Connection released - checked_out=0.
2026-04-14_08:14:05 | DEBUG   | ingest_service.py     | L38  | [ingest] Source resolved - retention=30d.
2026-04-14_08:14:05 | INFO    | ingest_service.py     | L42  | [ingest] Writing metric to TimescaleDB - event_id=f3a8b21c9d01 source=host-prod-01.
2026-04-14_08:14:05 | DEBUG   | metric_repo.py        | L22  | [metric_repo] Acquiring DB connection - table=metrics_acme metric=cpu_usage.
2026-04-14_08:14:05 | DEBUG   | db.py                 | L58  | [db] acquire() called - checked_out=0/5.
2026-04-14_08:14:05 | DEBUG   | db.py                 | L73  | [db] Connection acquired - checked_out=1.
2026-04-14_08:14:05 | DEBUG   | metric_repo.py        | L34  | [metric_repo] Connection acquired from pool.
2026-04-14_08:14:05 | DEBUG   | metric_repo.py        | L42  | [metric_repo] Executing INSERT - metric=cpu_usage.
2026-04-14_08:14:05 | DEBUG   | metric_repo.py        | L53  | [metric_repo] INSERT complete - rows=1.
2026-04-14_08:14:05 | DEBUG   | db.py                 | L80  | [db] Connection released - checked_out=0.
2026-04-14_08:14:05 | DEBUG   | metric_repo.py        | L57  | [metric_repo] Connection returned to pool.
2026-04-14_08:14:05 | INFO    | ingest_service.py     | L50  | [ingest] DB write OK - event_id=f3a8b21c9d01 rows=1.
2026-04-14_08:14:05 | DEBUG   | ingest_service.py     | L53  | [ingest] Relay emit acknowledged - event_id=f3a8b21c9d01.
2026-04-14_08:14:05 | DEBUG   | ingest_service.py     | L57  | [ingest] Cache key invalidated - key=summary:acme:host-prod-01:cpu_usage.
2026-04-14_08:14:05 | INFO    | ingest.py             | L48  | [n/a] Event accepted - event_id='f3a8b21c9d01'.
2026-04-14_08:14:05 | INFO    | main.py               | L67  | [a1b2c3d4] POST /api/v2/ingest/event -> HTTP 200 in 11.2ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-14_09:30:00 | DEBUG   | main.py               | L55  | [e5f6a7b8] Incoming POST /api/v2/ingest/event from 10.0.2.11
2026-04-14_09:30:00 | INFO    | ingest.py             | L34  | [n/a] Ingest request for metric='memory_rss' source='host-prod-02'.
2026-04-14_09:30:00 | DEBUG   | auth_service.py       | L46  | Validating token (length=0).
2026-04-14_09:30:00 | WARNING | auth_service.py       | L50  | Rejecting token - expected 3 JWT segments, got 1.
2026-04-14_09:30:00 | WARNING | ingest.py             | L42  | [n/a] Auth rejected for source='host-prod-02': Malformed token: expected 3 parts, got 1. Ensure the Authorization header is 'Bearer <token>'.
2026-04-14_09:30:00 | ERROR   | main.py               | L67  | [e5f6a7b8] POST /api/v2/ingest/event -> HTTP 401 in 3.1ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-14_10:45:12 | DEBUG   | main.py               | L55  | [c9d0e1f2] Incoming GET /api/v2/query/summary/boiler-room-a17 from 10.0.3.55
2026-04-14_10:45:12 | INFO    | query.py              | L29  | Summary query - source='boiler-room-a17' metric='temperature_c' window=3600s.
2026-04-14_10:45:12 | DEBUG   | query_service.py      | L28  | [query] Summary requested - source=boiler-room-a17 metric=temperature_c window=3600s tenant=default.
2026-04-14_10:45:12 | DEBUG   | query_service.py      | L33  | [query] Checking Redis - key=summary:default:boiler-room-a17:temperature_c:3600.
2026-04-14_10:45:12 | DEBUG   | cache.py              | L52  | [cache] GET summary:default:boiler-room-a17:temperature_c:3600.
2026-04-14_10:45:12 | DEBUG   | cache.py              | L60  | [cache] GET summary:default:boiler-room-a17:temperature_c:3600 -> MISS.
2026-04-14_10:45:12 | INFO    | query_service.py      | L42  | [query] Cache miss - falling back to TimescaleDB source=boiler-room-a17 metric=temperature_c.
2026-04-14_10:45:12 | DEBUG   | query_service.py      | L47  | [query] Querying metric_repo - source=boiler-room-a17 window=3600s.
2026-04-14_10:45:12 | DEBUG   | metric_repo.py        | L69  | [metric_repo] read_window - source=boiler-room-a17 metric=temperature_c window=3600s table=metrics_default.
2026-04-14_10:45:12 | DEBUG   | db.py                 | L58  | [db] acquire() called - checked_out=0/5.
2026-04-14_10:45:12 | DEBUG   | db.py                 | L73  | [db] Connection acquired - checked_out=1.
2026-04-14_10:45:12 | WARNING | metric_repo.py        | L83  | [metric_repo] Statement timeout reading source=boiler-room-a17 metric=temperature_c window=3600s.
2026-04-14_10:45:12 | DEBUG   | db.py                 | L80  | [db] Connection released - checked_out=0.
2026-04-14_10:45:12 | DEBUG   | query_service.py      | L51  | [query] DB returned None rows.
2026-04-14_10:45:12 | DEBUG   | query_service.py      | L55  | [query] Beginning aggregation - source=boiler-room-a17.
2026-04-14_10:45:12 | ERROR   | query_service.py      | L59  | [query] Aggregation failed - source=boiler-room-a17: 'NoneType' object has no attribute '__iter__'
2026-04-14_10:45:12 | ERROR   | query.py              | L44  | Aggregation error - source='boiler-room-a17': 'NoneType' object has no attribute '__iter__'
2026-04-14_10:45:12 | ERROR   | main.py               | L67  | [c9d0e1f2] GET /api/v2/query/summary/boiler-room-a17 -> HTTP 500 in 28.4ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-14_11:00:00 | DEBUG   | main.py               | L55  | [g3h4i5j6] Incoming POST /api/v2/ingest/event from 10.0.1.44
2026-04-14_11:00:00 | INFO    | ingest.py             | L34  | [n/a] Ingest request for metric='disk_io' source='host-prod-01'.
2026-04-14_11:00:00 | DEBUG   | auth_service.py       | L46  | Validating token (length=172).
2026-04-14_11:00:00 | DEBUG   | auth_service.py       | L85  | Token valid - tenant=acme sub=pipeline-agent.
2026-04-14_11:00:00 | INFO    | ingest.py             | L40  | [n/a] Auth OK - tenant='acme'.
2026-04-14_11:00:00 | DEBUG   | ingest_service.py     | L25  | [ingest] Entering pipeline - event_id=7c3d912f4e55 tenant=acme metric=disk_io.
2026-04-14_11:00:00 | DEBUG   | ingest_service.py     | L31  | [ingest] Schema validation passed - event_id=7c3d912f4e55.
2026-04-14_11:00:00 | DEBUG   | ingest_service.py     | L33  | [ingest] Resolving source_id=host-prod-01.
2026-04-14_11:00:00 | DEBUG   | source_repo.py        | L19  | [source_repo] Fetching config - source_id=host-prod-01 tenant=acme.
2026-04-14_11:00:00 | DEBUG   | db.py                 | L58  | [db] acquire() called - checked_out=0/5.
2026-04-14_11:00:00 | DEBUG   | db.py                 | L73  | [db] Connection acquired - checked_out=1.
2026-04-14_11:00:00 | DEBUG   | db.py                 | L80  | [db] Connection released - checked_out=0.
2026-04-14_11:00:00 | DEBUG   | ingest_service.py     | L38  | [ingest] Source resolved - retention=30d.
2026-04-14_11:00:00 | INFO    | ingest_service.py     | L42  | [ingest] Writing metric to TimescaleDB - event_id=7c3d912f4e55 source=host-prod-01.
2026-04-14_11:00:00 | DEBUG   | metric_repo.py        | L22  | [metric_repo] Acquiring DB connection - table=metrics_acme metric=disk_io.
2026-04-14_11:00:00 | DEBUG   | db.py                 | L58  | [db] acquire() called - checked_out=5/5.
2026-04-14_11:00:02 | WARNING | db.py                 | L65  | [db] Pool acquire timed out after 2.0s (checked_out=5/5).
2026-04-14_11:00:02 | ERROR   | metric_repo.py        | L29  | [metric_repo] Failed to acquire DB connection - table=metrics_acme: Timed out after 2.0s waiting for a database connection from a pool of 5.
2026-04-14_11:00:02 | ERROR   | ingest_service.py     | L47  | [ingest] DB write failed for event_id=7c3d912f4e55: Timed out after 2.0s waiting for a database connection from a pool of 5.
2026-04-14_11:00:02 | ERROR   | ingest.py             | L51  | [n/a] Ingest pipeline failed for metric='disk_io': Timed out after 2.0s waiting for a database connection from a pool of 5.
2026-04-14_11:00:02 | ERROR   | main.py               | L67  | [g3h4i5j6] POST /api/v2/ingest/event -> HTTP 500 in 2041.3ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
"""

LOG_APR_15 = """\
2026-04-15_07:00:00 | INFO    | main.py               | L27  | PulseMetrics starting up - initialising infrastructure.
2026-04-15_07:00:00 | INFO    | db.py                 | L39  | [db] Opening asyncpg pool - dsn='postgresql://pulse_rw@ts-db-primary.pulsemetrics.svc.cluster.local:5432/pulsemetrics' pool_size=5.
2026-04-15_07:00:01 | INFO    | db.py                 | L47  | [db] asyncpg pool ready.
2026-04-15_07:00:01 | INFO    | cache.py              | L35  | [cache] Connecting to Redis - redis-primary.pulsemetrics.svc.cluster.local:6379/0.
2026-04-15_07:00:01 | INFO    | cache.py              | L45  | [cache] Redis connection established.
2026-04-15_07:00:01 | INFO    | telemetry.py          | L37  | [telemetry] Initialising relay client - endpoint=https://telemetry-gateway.pulsemetrics.internal/v1/streams/pulse-prod.
2026-04-15_07:00:01 | INFO    | telemetry.py          | L47  | [telemetry] Relay client ready.
2026-04-15_07:00:01 | INFO    | main.py               | L31  | Infrastructure ready. Accepting requests.
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-15_08:05:30 | DEBUG   | main.py               | L55  | [k7l8m9n0] Incoming GET /api/v2/query/summary/host-prod-03 from 10.0.5.21
2026-04-15_08:05:30 | INFO    | query.py              | L29  | Summary query - source='host-prod-03' metric='net_rx_bytes' window=1800s.
2026-04-15_08:05:30 | DEBUG   | query_service.py      | L28  | [query] Summary requested - source=host-prod-03 metric=net_rx_bytes window=1800s tenant=default.
2026-04-15_08:05:30 | DEBUG   | query_service.py      | L33  | [query] Checking Redis - key=summary:default:host-prod-03:net_rx_bytes:1800.
2026-04-15_08:05:30 | DEBUG   | cache.py              | L52  | [cache] GET summary:default:host-prod-03:net_rx_bytes:1800.
2026-04-15_08:05:30 | WARNING | cache.py              | L64  | [cache] Refresh lock present for key=summary:default:host-prod-03:net_rx_bytes:1800. Waiting up to 1.5s for a hydrated value.
2026-04-15_08:05:31 | ERROR   | query_service.py      | L37  | [query] Cache read failed - key=summary:default:host-prod-03:net_rx_bytes:1800: Cache GET timed out after 1.5s waiting for refresh lock on key='summary:default:host-prod-03:net_rx_bytes:1800'.
2026-04-15_08:05:31 | ERROR   | query.py              | L41  | Summary timed out - source='host-prod-03': Cache GET timed out after 1.5s waiting for refresh lock on key='summary:default:host-prod-03:net_rx_bytes:1800'.
2026-04-15_08:05:31 | ERROR   | main.py               | L67  | [k7l8m9n0] GET /api/v2/query/summary/host-prod-03 -> HTTP 503 in 1521.8ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-15_09:12:44 | DEBUG   | main.py               | L55  | [p1q2r3s4] Incoming POST /api/v2/ingest/event from 10.0.1.77
2026-04-15_09:12:44 | INFO    | ingest.py             | L34  | [n/a] Ingest request for metric='latency_p99' source='api-gateway'.
2026-04-15_09:12:44 | DEBUG   | auth_service.py       | L46  | Validating token (length=0).
2026-04-15_09:12:44 | WARNING | auth_service.py       | L50  | Rejecting token - expected 3 JWT segments, got 1.
2026-04-15_09:12:44 | WARNING | ingest.py             | L42  | [n/a] Auth rejected for source='api-gateway': Malformed token: expected 3 parts, got 1. Ensure the Authorization header is 'Bearer <token>'.
2026-04-15_09:12:44 | ERROR   | main.py               | L67  | [p1q2r3s4] POST /api/v2/ingest/event -> HTTP 401 in 2.8ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-15_10:00:00 | DEBUG   | main.py               | L55  | [t5u6v7w8] Incoming GET /api/v2/query/summary/boiler-room-a17 from 10.0.3.55
2026-04-15_10:00:00 | INFO    | query.py              | L29  | Summary query - source='boiler-room-a17' metric='temperature_c' window=3600s.
2026-04-15_10:00:00 | DEBUG   | query_service.py      | L28  | [query] Summary requested - source=boiler-room-a17 metric=temperature_c window=3600s tenant=default.
2026-04-15_10:00:00 | DEBUG   | query_service.py      | L33  | [query] Checking Redis - key=summary:default:boiler-room-a17:temperature_c:3600.
2026-04-15_10:00:00 | DEBUG   | cache.py              | L52  | [cache] GET summary:default:boiler-room-a17:temperature_c:3600.
2026-04-15_10:00:00 | DEBUG   | cache.py              | L60  | [cache] GET summary:default:boiler-room-a17:temperature_c:3600 -> MISS.
2026-04-15_10:00:00 | INFO    | query_service.py      | L42  | [query] Cache miss - falling back to TimescaleDB source=boiler-room-a17 metric=temperature_c.
2026-04-15_10:00:00 | DEBUG   | query_service.py      | L47  | [query] Querying metric_repo - source=boiler-room-a17 window=3600s.
2026-04-15_10:00:00 | DEBUG   | metric_repo.py        | L69  | [metric_repo] read_window - source=boiler-room-a17 metric=temperature_c window=3600s table=metrics_default.
2026-04-15_10:00:00 | DEBUG   | db.py                 | L58  | [db] acquire() called - checked_out=0/5.
2026-04-15_10:00:00 | DEBUG   | db.py                 | L73  | [db] Connection acquired - checked_out=1.
2026-04-15_10:00:00 | WARNING | metric_repo.py        | L83  | [metric_repo] Statement timeout reading source=boiler-room-a17 metric=temperature_c window=3600s.
2026-04-15_10:00:00 | DEBUG   | db.py                 | L80  | [db] Connection released - checked_out=0.
2026-04-15_10:00:00 | DEBUG   | query_service.py      | L51  | [query] DB returned None rows.
2026-04-15_10:00:00 | DEBUG   | query_service.py      | L55  | [query] Beginning aggregation - source=boiler-room-a17.
2026-04-15_10:00:00 | ERROR   | query_service.py      | L59  | [query] Aggregation failed - source=boiler-room-a17: 'NoneType' object has no attribute '__iter__'
2026-04-15_10:00:00 | ERROR   | query.py              | L44  | Aggregation error - source='boiler-room-a17': 'NoneType' object has no attribute '__iter__'
2026-04-15_10:00:00 | ERROR   | main.py               | L67  | [t5u6v7w8] GET /api/v2/query/summary/boiler-room-a17 -> HTTP 500 in 31.7ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-15_11:30:00 | DEBUG   | main.py               | L55  | [x9y0z1a2] Incoming POST /api/v2/ingest/event from 10.0.1.44
2026-04-15_11:30:00 | INFO    | ingest.py             | L34  | [n/a] Ingest request for metric='cpu_usage' source='host-prod-05'.
2026-04-15_11:30:00 | DEBUG   | auth_service.py       | L46  | Validating token (length=172).
2026-04-15_11:30:00 | DEBUG   | auth_service.py       | L85  | Token valid - tenant=acme sub=pipeline-agent.
2026-04-15_11:30:00 | INFO    | ingest.py             | L40  | [n/a] Auth OK - tenant='acme'.
2026-04-15_11:30:00 | DEBUG   | ingest_service.py     | L25  | [ingest] Entering pipeline - event_id=9e1f234a5b67 tenant=acme metric=cpu_usage.
2026-04-15_11:30:00 | DEBUG   | ingest_service.py     | L31  | [ingest] Schema validation passed - event_id=9e1f234a5b67.
2026-04-15_11:30:00 | DEBUG   | ingest_service.py     | L33  | [ingest] Resolving source_id=host-prod-05.
2026-04-15_11:30:00 | DEBUG   | source_repo.py        | L19  | [source_repo] Fetching config - source_id=host-prod-05 tenant=acme.
2026-04-15_11:30:00 | DEBUG   | db.py                 | L58  | [db] acquire() called - checked_out=0/5.
2026-04-15_11:30:00 | DEBUG   | db.py                 | L73  | [db] Connection acquired - checked_out=1.
2026-04-15_11:30:00 | DEBUG   | db.py                 | L80  | [db] Connection released - checked_out=0.
2026-04-15_11:30:00 | DEBUG   | ingest_service.py     | L38  | [ingest] Source resolved - retention=30d.
2026-04-15_11:30:00 | INFO    | ingest_service.py     | L42  | [ingest] Writing metric to TimescaleDB - event_id=9e1f234a5b67 source=host-prod-05.
2026-04-15_11:30:00 | DEBUG   | metric_repo.py        | L22  | [metric_repo] Acquiring DB connection - table=metrics_acme metric=cpu_usage.
2026-04-15_11:30:00 | DEBUG   | db.py                 | L58  | [db] acquire() called - checked_out=5/5.
2026-04-15_11:30:02 | WARNING | db.py                 | L65  | [db] Pool acquire timed out after 2.0s (checked_out=5/5).
2026-04-15_11:30:02 | ERROR   | metric_repo.py        | L29  | [metric_repo] Failed to acquire DB connection - table=metrics_acme: Timed out after 2.0s waiting for a database connection from a pool of 5.
2026-04-15_11:30:02 | ERROR   | ingest_service.py     | L47  | [ingest] DB write failed for event_id=9e1f234a5b67: Timed out after 2.0s waiting for a database connection from a pool of 5.
2026-04-15_11:30:02 | ERROR   | ingest.py             | L51  | [n/a] Ingest pipeline failed for metric='cpu_usage': Timed out after 2.0s waiting for a database connection from a pool of 5.
2026-04-15_11:30:02 | ERROR   | main.py               | L67  | [x9y0z1a2] POST /api/v2/ingest/event -> HTTP 500 in 2038.1ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
2026-04-15_13:15:00 | DEBUG   | main.py               | L55  | [b3c4d5e6] Incoming GET /api/v2/query/summary/host-prod-07 from 10.0.5.21
2026-04-15_13:15:00 | INFO    | query.py              | L29  | Summary query - source='host-prod-07' metric='net_tx_bytes' window=1800s.
2026-04-15_13:15:00 | DEBUG   | query_service.py      | L28  | [query] Summary requested - source=host-prod-07 metric=net_tx_bytes window=1800s tenant=default.
2026-04-15_13:15:00 | DEBUG   | query_service.py      | L33  | [query] Checking Redis - key=summary:default:host-prod-07:net_tx_bytes:1800.
2026-04-15_13:15:00 | DEBUG   | cache.py              | L52  | [cache] GET summary:default:host-prod-07:net_tx_bytes:1800.
2026-04-15_13:15:00 | WARNING | cache.py              | L64  | [cache] Refresh lock present for key=summary:default:host-prod-07:net_tx_bytes:1800. Waiting up to 1.5s for a hydrated value.
2026-04-15_13:15:01 | ERROR   | query_service.py      | L37  | [query] Cache read failed - key=summary:default:host-prod-07:net_tx_bytes:1800: Cache GET timed out after 1.5s waiting for refresh lock on key='summary:default:host-prod-07:net_tx_bytes:1800'.
2026-04-15_13:15:01 | ERROR   | query.py              | L41  | Summary timed out - source='host-prod-07': Cache GET timed out after 1.5s waiting for refresh lock on key='summary:default:host-prod-07:net_tx_bytes:1800'.
2026-04-15_13:15:01 | ERROR   | main.py               | L67  | [b3c4d5e6] GET /api/v2/query/summary/host-prod-07 -> HTTP 503 in 1519.3ms
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
"""


# ============================================================================
#  GENERATOR
# ============================================================================

def generate() -> tuple[Path, Path]:
    """
    Write source files under test_project_v2/ and log files under logs/.
    Both directories sit alongside this script.
    """
    base = Path(__file__).resolve().parent
    src_root = base / "test_project_v2"
    log_root = base / "logs"

    source_files: dict[Path, str] = {
        src_root / "src" / "__init__.py": "",
        src_root / "src" / "main.py": MAIN_PY,
        src_root / "src" / "api" / "__init__.py": "",
        src_root / "src" / "api" / "ingest.py": INGEST_PY,
        src_root / "src" / "api" / "query.py": QUERY_PY,
        src_root / "src" / "api" / "auth.py": AUTH_PY,
        src_root / "src" / "services" / "__init__.py": "",
        src_root / "src" / "services" / "ingest_service.py": INGEST_SERVICE_PY,
        src_root / "src" / "services" / "query_service.py": QUERY_SERVICE_PY,
        src_root / "src" / "services" / "auth_service.py": AUTH_SERVICE_PY,
        src_root / "src" / "repositories" / "__init__.py": "",
        src_root / "src" / "repositories" / "metric_repo.py": METRIC_REPO_PY,
        src_root / "src" / "repositories" / "source_repo.py": SOURCE_REPO_PY,
        src_root / "src" / "infrastructure" / "__init__.py": "",
        src_root / "src" / "infrastructure" / "db.py": DB_PY,
        src_root / "src" / "infrastructure" / "cache.py": CACHE_PY,
        src_root / "src" / "infrastructure" / "telemetry.py": TELEMETRY_PY,
        src_root / "src" / "utils" / "__init__.py": "",
        src_root / "src" / "utils" / "logger.py": LOGGER_PY,
    }

    log_files: dict[Path, str] = {
        log_root / "2026-04-14.text": LOG_APR_14,
        log_root / "2026-04-15.text": LOG_APR_15,
    }

    for path, content in {**source_files, **log_files}.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    return src_root, log_root


def print_tree(root: Path, label: str) -> None:
    print(f"\\n{'-' * 64}")
    print(f"  {label}")
    print(f"{'-' * 64}")

    def _walk(path: Path, prefix: str = "") -> None:
        if path.is_dir():
            print(f"{prefix}{path.name}/")
            children = sorted(path.iterdir(), key=lambda p: (p.is_file(), p.name))
            for child in children:
                _walk(child, prefix + "  ")
        else:
            print(f"{prefix}{path.name}")

    _walk(root)


def main() -> None:
    src_root, log_root = generate()

    import ast

    errors: list[str] = []
    for file_path in src_root.rglob("*.py"):
        source = file_path.read_text(encoding="utf-8")
        if not source.strip():
            continue
        try:
            ast.parse(source)
        except SyntaxError as exc:
            errors.append(f"{file_path.relative_to(src_root)}: {exc}")

    print(f"\\n{'=' * 64}")
    print("  PulseMetrics - service tree generated")
    print(f"{'=' * 64}\\n")

    if errors:
        print("  Syntax errors detected:")
        for err in errors:
            print(f"     {err}")
        print()
    else:
        print("  All Python files pass syntax check.\\n")

    print("  Infrastructure")
    print("  - TimescaleDB  ts-db-primary.pulsemetrics.svc.cluster.local:5432")
    print("  - Redis        redis-primary.pulsemetrics.svc.cluster.local:6379")
    print("  - Telemetry    telemetry-gateway.pulsemetrics.internal/v1/streams/pulse-prod\\n")

    print_tree(src_root, "test_project_v2/")
    print_tree(log_root, "logs/")
    print()


if __name__ == "__main__":
    main()