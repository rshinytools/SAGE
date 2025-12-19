# Factory 4: Enterprise Architecture Design

## Current vs Target State

| Aspect | Current | Target | Priority |
|--------|---------|--------|----------|
| Retry Logic | None | 3x with exponential backoff | **CRITICAL** |
| Circuit Breaker | None | Per-provider with auto-recovery | **CRITICAL** |
| Request Tracing | None | UUID propagated through all steps | **CRITICAL** |
| Error Recovery | Fail-fast | Graceful degradation | **HIGH** |
| Observability | Basic logging | Structured logs + metrics | **HIGH** |
| Rate Limiting | None | Token bucket per user/endpoint | **HIGH** |
| Connection Pool | None | DuckDB connection pool | **MEDIUM** |
| Config Management | Hardcoded | Externalized YAML + hot reload | **MEDIUM** |
| Health Checks | Basic | Deep health with dependencies | **MEDIUM** |

---

## 1. Resilience Layer

### 1.1 Retry Policy

```python
@dataclass
class RetryPolicy:
    """Configurable retry policy for external calls."""
    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0
    exponential_base: float = 2.0
    jitter: bool = True  # Prevent thundering herd

    # Retryable conditions
    retry_on_timeout: bool = True
    retry_on_connection_error: bool = True
    retry_on_rate_limit: bool = True
    retry_on_server_error: bool = True  # 5xx responses

    def get_delay(self, attempt: int) -> float:
        """Calculate delay with exponential backoff + jitter."""
        delay = min(
            self.base_delay_seconds * (self.exponential_base ** attempt),
            self.max_delay_seconds
        )
        if self.jitter:
            delay *= (0.5 + random.random())  # 50-150% of calculated delay
        return delay
```

### 1.2 Circuit Breaker

```python
@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5        # Failures before opening
    success_threshold: int = 3        # Successes before closing
    timeout_seconds: float = 30.0     # Time in open state before half-open

class CircuitBreaker:
    """
    Circuit breaker pattern for external services.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Failing fast, no requests pass through
    - HALF_OPEN: Testing if service recovered
    """

    def __init__(self, name: str, config: CircuitBreakerConfig):
        self.name = name
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self._lock = threading.Lock()

    def can_execute(self) -> bool:
        """Check if request can proceed."""
        with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            elif self.state == CircuitState.OPEN:
                # Check if timeout elapsed
                if time.time() - self.last_failure_time > self.config.timeout_seconds:
                    self.state = CircuitState.HALF_OPEN
                    return True
                return False
            else:  # HALF_OPEN
                return True

    def record_success(self):
        """Record successful call."""
        with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self._close()
            self.failure_count = 0

    def record_failure(self):
        """Record failed call."""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitState.HALF_OPEN:
                self._open()
            elif self.failure_count >= self.config.failure_threshold:
                self._open()
```

### 1.3 Resilient LLM Client

```python
class ResilientLLMClient:
    """
    LLM client with retry, circuit breaker, and fallback.
    """

    def __init__(
        self,
        primary_provider: LLMProvider,
        fallback_provider: Optional[LLMProvider] = None,
        retry_policy: RetryPolicy = None,
        circuit_breaker: CircuitBreaker = None
    ):
        self.primary = primary_provider
        self.fallback = fallback_provider
        self.retry = retry_policy or RetryPolicy()
        self.circuit = circuit_breaker or CircuitBreaker("llm", CircuitBreakerConfig())
        self.metrics = LLMMetrics()

    def generate(self, request: LLMRequest, context: RequestContext) -> LLMResponse:
        """Generate with full resilience."""

        # Check circuit breaker
        if not self.circuit.can_execute():
            self.metrics.record_circuit_open()
            if self.fallback:
                return self._try_fallback(request, context)
            raise CircuitOpenError(f"Circuit open for {self.circuit.name}")

        # Try primary with retries
        last_error = None
        for attempt in range(self.retry.max_attempts):
            try:
                with self.metrics.timer("llm_request"):
                    response = self.primary.generate(request)

                self.circuit.record_success()
                self.metrics.record_success(attempt)
                return response

            except RetryableError as e:
                last_error = e
                self.metrics.record_retry(attempt, str(e))

                if attempt < self.retry.max_attempts - 1:
                    delay = self.retry.get_delay(attempt)
                    logger.warning(
                        f"LLM request failed (attempt {attempt + 1}), "
                        f"retrying in {delay:.1f}s: {e}",
                        extra={"request_id": context.request_id}
                    )
                    time.sleep(delay)

        # All retries exhausted
        self.circuit.record_failure()
        self.metrics.record_failure(str(last_error))

        # Try fallback
        if self.fallback:
            return self._try_fallback(request, context)

        raise LLMError(f"All {self.retry.max_attempts} attempts failed: {last_error}")
```

---

## 2. Observability Layer

### 2.1 Request Context

```python
@dataclass
class RequestContext:
    """
    Context propagated through entire pipeline.
    Enables tracing, logging, and metrics correlation.
    """
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    start_time: float = field(default_factory=time.time)

    # Trace data
    parent_span_id: Optional[str] = None
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Stage tracking
    current_stage: str = ""
    stage_timings: Dict[str, float] = field(default_factory=dict)
    stage_metadata: Dict[str, Any] = field(default_factory=dict)

    def start_stage(self, stage: str):
        """Mark stage start."""
        self.current_stage = stage
        self.stage_timings[f"{stage}_start"] = time.time()

    def end_stage(self, stage: str, metadata: Dict = None):
        """Mark stage end with optional metadata."""
        end_time = time.time()
        start_time = self.stage_timings.get(f"{stage}_start", self.start_time)
        self.stage_timings[f"{stage}_duration_ms"] = (end_time - start_time) * 1000
        if metadata:
            self.stage_metadata[stage] = metadata

    def to_log_context(self) -> Dict[str, Any]:
        """Get context for structured logging."""
        return {
            "request_id": self.request_id,
            "trace_id": self.trace_id,
            "user_id": self.user_id,
            "stage": self.current_stage,
            "elapsed_ms": (time.time() - self.start_time) * 1000
        }
```

### 2.2 Structured Logging

```python
class StructuredLogger:
    """
    JSON structured logger with request context.
    """

    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self._context: Optional[RequestContext] = None

    def with_context(self, context: RequestContext) -> 'StructuredLogger':
        """Return logger bound to request context."""
        new_logger = StructuredLogger(self.logger.name)
        new_logger._context = context
        return new_logger

    def info(self, message: str, **extra):
        """Log info with context."""
        self._log(logging.INFO, message, extra)

    def warning(self, message: str, **extra):
        """Log warning with context."""
        self._log(logging.WARNING, message, extra)

    def error(self, message: str, error: Exception = None, **extra):
        """Log error with context and exception details."""
        if error:
            extra["error_type"] = type(error).__name__
            extra["error_message"] = str(error)
            extra["error_traceback"] = traceback.format_exc()
        self._log(logging.ERROR, message, extra)

    def _log(self, level: int, message: str, extra: Dict):
        """Internal log with structured data."""
        log_data = {
            "message": message,
            "timestamp": datetime.utcnow().isoformat(),
            **extra
        }
        if self._context:
            log_data.update(self._context.to_log_context())

        self.logger.log(level, json.dumps(log_data))
```

### 2.3 Metrics Collection

```python
class PipelineMetrics:
    """
    Metrics collector for pipeline observability.
    Exports to Prometheus format.
    """

    def __init__(self):
        # Counters
        self.requests_total = Counter("sage_requests_total", ["status", "stage"])
        self.errors_total = Counter("sage_errors_total", ["stage", "error_type"])
        self.cache_operations = Counter("sage_cache_ops", ["operation", "result"])

        # Histograms
        self.request_duration = Histogram(
            "sage_request_duration_seconds",
            buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60, 120]
        )
        self.stage_duration = Histogram(
            "sage_stage_duration_seconds",
            ["stage"],
            buckets=[0.01, 0.05, 0.1, 0.5, 1, 5, 10]
        )
        self.llm_tokens = Histogram(
            "sage_llm_tokens",
            ["direction"],  # input/output
            buckets=[100, 500, 1000, 2000, 5000, 10000]
        )

        # Gauges
        self.active_requests = Gauge("sage_active_requests")
        self.circuit_breaker_state = Gauge("sage_circuit_state", ["service"])
        self.cache_size = Gauge("sage_cache_size")
        self.cache_hit_rate = Gauge("sage_cache_hit_rate")

    def record_request(self, context: RequestContext, result: PipelineResult):
        """Record completed request metrics."""
        status = "success" if result.success else "error"
        failed_stage = result.metadata.get("failed_stage", "none")

        self.requests_total.inc(status=status, stage=failed_stage)
        self.request_duration.observe(context.total_elapsed_seconds())

        # Record per-stage timings
        for stage, duration_ms in context.stage_timings.items():
            if stage.endswith("_duration_ms"):
                stage_name = stage.replace("_duration_ms", "")
                self.stage_duration.observe(duration_ms / 1000, stage=stage_name)
```

---

## 3. Graceful Degradation

### 3.1 Degradation Levels

```python
class DegradationLevel(Enum):
    """Service degradation levels."""
    FULL = "full"           # All features available
    LIMITED = "limited"     # Some features disabled
    MINIMAL = "minimal"     # Core functionality only
    MAINTENANCE = "maintenance"  # Read-only / cached responses only

@dataclass
class DegradationConfig:
    """Configuration for graceful degradation."""

    # Feature flags per level
    features: Dict[DegradationLevel, Set[str]] = field(default_factory=lambda: {
        DegradationLevel.FULL: {
            "llm_intent_classification",
            "llm_sql_generation",
            "llm_explanation",
            "entity_extraction",
            "confidence_scoring",
            "caching"
        },
        DegradationLevel.LIMITED: {
            "llm_sql_generation",  # Core - always try
            "entity_extraction",
            "confidence_scoring",
            "caching"
        },
        DegradationLevel.MINIMAL: {
            "caching"  # Only return cached results
        },
        DegradationLevel.MAINTENANCE: set()  # Nothing works
    })
```

### 3.2 Fallback Strategies

```python
class FallbackStrategy:
    """
    Fallback strategies when components fail.
    """

    @staticmethod
    def intent_classification_fallback(query: str) -> str:
        """
        When LLM intent classification fails:
        - Use keyword-based heuristics
        - Default to CLINICAL_DATA if uncertain
        """
        query_lower = query.lower().strip()

        # Simple heuristics
        greeting_words = {"hi", "hello", "hey", "good morning", "good afternoon"}
        help_words = {"help", "what can you", "how do i", "how does"}

        if any(query_lower.startswith(w) for w in greeting_words):
            return "GREETING"
        if any(w in query_lower for w in help_words):
            return "HELP"

        # Default: assume clinical data query
        return "CLINICAL_DATA"

    @staticmethod
    def sql_generation_fallback(
        query: str,
        table_resolution: TableResolution,
        cache: QueryCache
    ) -> Optional[str]:
        """
        When LLM SQL generation fails:
        - Check cache for similar queries
        - Use template-based generation for simple patterns
        """
        # Try fuzzy cache match
        similar = cache.find_similar(query, threshold=0.85)
        if similar:
            return similar.sql

        # Template-based generation for common patterns
        templates = SQLTemplates()
        return templates.try_generate(query, table_resolution)

    @staticmethod
    def explanation_fallback(
        result: ExecutionResult,
        table_resolution: TableResolution
    ) -> str:
        """
        When LLM explanation generation fails:
        - Use template-based explanation
        """
        if result.row_count == 1 and len(result.data[0]) == 1:
            # Single value result (e.g., COUNT)
            value = list(result.data[0].values())[0]
            return f"**{value}** records found in {table_resolution.selected_table}."

        return f"Query returned {result.row_count} records from {table_resolution.selected_table}."
```

---

## 4. Configuration Management

### 4.1 Externalized Configuration

```yaml
# config/factory4.yaml

pipeline:
  timeout_seconds: 240
  enable_cache: true
  cache_ttl_seconds: 3600
  cache_max_size: 1000

resilience:
  retry:
    max_attempts: 3
    base_delay_seconds: 1.0
    max_delay_seconds: 30.0
    exponential_base: 2.0
    jitter: true

  circuit_breaker:
    failure_threshold: 5
    success_threshold: 3
    timeout_seconds: 30.0

llm:
  provider: claude
  model: claude-sonnet-4-20250514
  temperature: 0.1
  max_tokens: 2000
  timeout_seconds: 60

  safety:
    enable_audit: true
    block_pii: true
    audit_log_path: /app/logs/llm_audit

confidence:
  thresholds:
    high: 90.0
    medium: 70.0
    low: 50.0

  weights:
    dictionary_match: 0.40
    metadata_coverage: 0.30
    execution_success: 0.20
    result_sanity: 0.10

  sanity_limits:
    max_reasonable_count: 100000
    min_reasonable_count: 0

clinical_rules:
  # Externalized from clinical_config.py
  table_priorities:
    adverse_events: [ADAE, AE]
    demographics: [ADSL, DM]
    labs: [ADLB, LB]
    vitals: [ADVS, VS]
    concomitant_meds: [ADCM, CM]
    exposure: [ADEX, EX]

  population_defaults:
    adverse_events: safety
    demographics: intent_to_treat
    labs: safety

  domain_keywords:
    adverse_events:
      - adverse
      - ae
      - event
      - teae
      - toxicity
      - side effect
    demographics:
      - age
      - sex
      - gender
      - race
      - ethnicity
```

### 4.2 Configuration Loader

```python
class ConfigLoader:
    """
    Load and validate configuration with hot-reload support.
    """

    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self._config: Dict = {}
        self._last_modified: float = 0
        self._lock = threading.Lock()
        self._watchers: List[Callable] = []

    def load(self) -> Dict:
        """Load configuration from YAML."""
        with self._lock:
            if not self.config_path.exists():
                raise ConfigurationError(f"Config not found: {self.config_path}")

            current_mtime = self.config_path.stat().st_mtime
            if current_mtime != self._last_modified:
                with open(self.config_path) as f:
                    self._config = yaml.safe_load(f)
                self._last_modified = current_mtime
                self._validate()
                self._notify_watchers()

            return self._config

    def watch(self, callback: Callable[[Dict], None]):
        """Register callback for config changes."""
        self._watchers.append(callback)

    def _validate(self):
        """Validate configuration against schema."""
        schema = self._load_schema()
        jsonschema.validate(self._config, schema)

    def _notify_watchers(self):
        """Notify all watchers of config change."""
        for callback in self._watchers:
            try:
                callback(self._config)
            except Exception as e:
                logger.error(f"Config watcher failed: {e}")
```

---

## 5. Health Checks

### 5.1 Deep Health Check

```python
@dataclass
class HealthStatus:
    """Health status for a component."""
    healthy: bool
    message: str
    latency_ms: float
    details: Dict[str, Any] = field(default_factory=dict)

class HealthChecker:
    """
    Deep health checks for all dependencies.
    """

    async def check_all(self) -> Dict[str, HealthStatus]:
        """Run all health checks concurrently."""
        checks = {
            "database": self._check_database(),
            "llm_provider": self._check_llm(),
            "cache": self._check_cache(),
            "metadata": self._check_metadata(),
        }

        results = await asyncio.gather(
            *[self._timed_check(name, check) for name, check in checks.items()],
            return_exceptions=True
        )

        return {name: result for name, result in zip(checks.keys(), results)}

    async def _check_database(self) -> HealthStatus:
        """Check DuckDB connectivity."""
        try:
            conn = duckdb.connect(self.db_path, read_only=True)
            result = conn.execute("SELECT 1").fetchone()
            conn.close()

            # Check table count
            tables = self._get_table_count()

            return HealthStatus(
                healthy=True,
                message="Database operational",
                latency_ms=0,
                details={"tables": tables}
            )
        except Exception as e:
            return HealthStatus(
                healthy=False,
                message=f"Database error: {e}",
                latency_ms=0
            )

    async def _check_llm(self) -> HealthStatus:
        """Check LLM provider connectivity."""
        try:
            # Simple test request
            response = await self.llm_client.generate(
                LLMRequest(prompt="ping", max_tokens=5)
            )
            return HealthStatus(
                healthy=True,
                message="LLM provider operational",
                latency_ms=response.latency_ms,
                details={"model": response.model}
            )
        except Exception as e:
            return HealthStatus(
                healthy=False,
                message=f"LLM error: {e}",
                latency_ms=0
            )
```

---

## 6. Rate Limiting

```python
class TokenBucketRateLimiter:
    """
    Token bucket rate limiter for API protection.
    """

    def __init__(
        self,
        rate: float,           # Tokens per second
        capacity: float,       # Maximum tokens
        key_func: Callable     # Function to extract rate limit key
    ):
        self.rate = rate
        self.capacity = capacity
        self.key_func = key_func
        self._buckets: Dict[str, Tuple[float, float]] = {}  # key -> (tokens, last_update)
        self._lock = threading.Lock()

    def acquire(self, key: str, tokens: float = 1.0) -> bool:
        """
        Try to acquire tokens. Returns True if successful.
        """
        with self._lock:
            now = time.time()
            current_tokens, last_update = self._buckets.get(key, (self.capacity, now))

            # Refill tokens based on time elapsed
            elapsed = now - last_update
            current_tokens = min(self.capacity, current_tokens + elapsed * self.rate)

            if current_tokens >= tokens:
                self._buckets[key] = (current_tokens - tokens, now)
                return True
            else:
                self._buckets[key] = (current_tokens, now)
                return False

    def get_wait_time(self, key: str, tokens: float = 1.0) -> float:
        """Get seconds to wait before tokens available."""
        with self._lock:
            current_tokens, _ = self._buckets.get(key, (self.capacity, time.time()))
            if current_tokens >= tokens:
                return 0.0
            return (tokens - current_tokens) / self.rate
```

---

## 7. Implementation Priority

### Phase 1: Critical (Week 1-2)
1. **Request Context & Tracing** - Foundation for all observability
2. **Retry Logic for LLM** - Most common failure point
3. **Circuit Breaker** - Prevent cascading failures
4. **Structured Logging** - Debug production issues

### Phase 2: High (Week 3-4)
5. **Metrics Collection** - Operational visibility
6. **Graceful Degradation** - Partial results vs. failures
7. **Rate Limiting** - Protect from abuse
8. **Health Checks** - Deployment readiness

### Phase 3: Medium (Week 5-6)
9. **Configuration Externalization** - Clinical rules to YAML
10. **Connection Pooling** - DuckDB performance
11. **Cache Improvements** - Fuzzy matching, repair
12. **Hot Reload** - Config changes without restart

---

## 8. Success Metrics

| Metric | Current | Target |
|--------|---------|--------|
| Request Success Rate | ~90% | 99.5% |
| P50 Latency | 5-10s | 3-5s |
| P99 Latency | 60s+ | 15s |
| Mean Time to Recovery | Manual | < 30s auto |
| Error Traceability | None | 100% |
| Cache Hit Rate | Unknown | > 40% |

---

## 9. Testing Strategy

### Chaos Testing
- Kill LLM provider mid-request
- Inject network latency
- Corrupt cache entries
- Exhaust database connections

### Load Testing
- Sustained 100 QPS for 1 hour
- Burst to 500 QPS for 5 minutes
- Concurrent users: 50+

### Failure Injection
- LLM returns garbage SQL
- Database locks
- Cache corruption
- Config file missing
