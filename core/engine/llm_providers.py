# SAGE - LLM Provider Abstraction
# ================================
"""
LLM Provider System
==================
Supports multiple LLM backends with a unified interface:
- Claude (Anthropic API) - Primary
- Mock (testing)

Includes safety layer to audit/sanitize data before external API calls.
"""

import re
import os
import time
import json
import logging
import hashlib
import threading
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

class LLMProvider(str, Enum):
    """Supported LLM providers."""
    CLAUDE = "claude"  # Primary - Cloud LLM via Anthropic API
    MOCK = "mock"      # For testing


@dataclass
class LLMConfig:
    """Configuration for Claude LLM provider."""
    provider: LLMProvider = LLMProvider.CLAUDE
    temperature: float = 0.1
    max_tokens: int = 2000
    timeout: int = 60

    # Claude/Anthropic settings
    anthropic_api_key: Optional[str] = None
    claude_model: str = "claude-sonnet-4-20250514"

    # Safety settings
    enable_safety_audit: bool = True
    audit_log_path: Optional[str] = None
    block_potential_pii: bool = True

    @classmethod
    def from_env(cls) -> "LLMConfig":
        """Create config from environment variables."""
        provider_str = os.getenv("LLM_PROVIDER", "claude").lower()
        try:
            provider = LLMProvider(provider_str)
        except ValueError:
            logger.warning(f"Unknown LLM_PROVIDER '{provider_str}', defaulting to claude")
            provider = LLMProvider.CLAUDE

        return cls(
            provider=provider,
            temperature=float(os.getenv("LLM_TEMPERATURE", "0.1")),
            max_tokens=int(os.getenv("LLM_MAX_TOKENS", "2000")),
            timeout=int(os.getenv("LLM_TIMEOUT_SECONDS", "60")),
            # Claude
            anthropic_api_key=os.getenv("ANTHROPIC_API_KEY"),
            claude_model=os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514"),
            # Safety
            enable_safety_audit=os.getenv("LLM_SAFETY_AUDIT", "true").lower() == "true",
            audit_log_path=os.getenv("LLM_AUDIT_LOG_PATH"),
            block_potential_pii=os.getenv("LLM_BLOCK_PII", "true").lower() == "true",
        )


@dataclass
class LLMRequest:
    """Request to send to LLM."""
    prompt: str
    system_prompt: Optional[str] = None
    max_tokens: int = 2000
    temperature: float = 0.1


@dataclass
class LLMResponse:
    """Response from LLM."""
    content: str
    model: str
    provider: str
    generation_time_ms: float
    tokens_used: Optional[int] = None
    raw_response: Optional[Any] = None


# =============================================================================
# SAFETY & AUDIT LAYER
# =============================================================================

@dataclass
class SafetyAuditRecord:
    """Record of what was sent to an external LLM."""
    timestamp: str
    provider: str
    model: str
    prompt_hash: str  # Hash of prompt (for deduplication)
    prompt_length: int
    contains_schema_only: bool
    potential_pii_detected: bool
    pii_patterns_found: List[str]
    blocked: bool
    block_reason: Optional[str] = None


class SafetyAuditor:
    """
    Safety layer that audits and sanitizes data before external API calls.

    Key responsibilities:
    1. Detect potential PII/PHI in prompts
    2. Log what's being sent for audit trail
    3. Optionally block requests with suspicious content
    """

    # Patterns that might indicate PII/PHI
    PII_PATTERNS = [
        (r'\b\d{3}-\d{2}-\d{4}\b', 'SSN'),  # SSN
        (r'\b\d{9}\b', 'SSN_NO_DASH'),  # SSN without dashes
        (r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', 'EMAIL'),  # Email
        (r'\b\d{10}\b', 'PHONE'),  # Phone number
        (r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', 'PHONE_FORMATTED'),  # Formatted phone
        (r'\b(?:19|20)\d{2}[-/]\d{1,2}[-/]\d{1,2}\b', 'DATE'),  # Date of birth
        (r'\b\d{1,2}[-/]\d{1,2}[-/](?:19|20)\d{2}\b', 'DATE'),  # Date of birth (alt)
        (r'\bMRN[:\s]?\d+\b', 'MRN'),  # Medical Record Number
        (r'\bPatient ID[:\s]?\w+\b', 'PATIENT_ID'),  # Patient ID
        (r'\b(?:Mr\.|Mrs\.|Ms\.|Dr\.)\s+[A-Z][a-z]+\s+[A-Z][a-z]+\b', 'FULL_NAME'),  # Name with title
    ]

    # Safe patterns (schema-only content)
    SAFE_PATTERNS = [
        r'^TABLE:\s*\w+$',
        r'^COLUMNS?\s*\(',
        r'^\w+:\s*(?:VARCHAR|INTEGER|DOUBLE|DATE|BOOLEAN)',
        r'^(?:SELECT|FROM|WHERE|COUNT|DISTINCT)\b',
    ]

    def __init__(self, config: LLMConfig, audit_log_dir: Optional[str] = None):
        """
        Initialize safety auditor.

        Args:
            config: LLM configuration
            audit_log_dir: Directory to write audit logs
        """
        self.config = config
        self.audit_log_dir = Path(audit_log_dir) if audit_log_dir else None
        self._ensure_audit_dir()

    def _ensure_audit_dir(self):
        """Ensure audit log directory exists."""
        if self.audit_log_dir:
            self.audit_log_dir.mkdir(parents=True, exist_ok=True)

    def audit_request(self, request: LLMRequest, provider: str, model: str) -> SafetyAuditRecord:
        """
        Audit a request before sending to external LLM.

        Args:
            request: The LLM request
            provider: Provider name
            model: Model name

        Returns:
            SafetyAuditRecord with audit results
        """
        full_prompt = f"{request.system_prompt or ''}\n{request.prompt}"

        # Check for PII patterns
        pii_found = []
        for pattern, pii_type in self.PII_PATTERNS:
            if re.search(pattern, full_prompt, re.IGNORECASE):
                pii_found.append(pii_type)

        # Check if content looks like schema-only
        schema_only = self._is_schema_only(full_prompt)

        # Determine if we should block
        blocked = False
        block_reason = None
        if pii_found and self.config.block_potential_pii:
            blocked = True
            block_reason = f"Potential PII detected: {', '.join(pii_found)}"

        # Create audit record
        record = SafetyAuditRecord(
            timestamp=datetime.now().isoformat(),
            provider=provider,
            model=model,
            prompt_hash=hashlib.sha256(full_prompt.encode()).hexdigest()[:16],
            prompt_length=len(full_prompt),
            contains_schema_only=schema_only,
            potential_pii_detected=bool(pii_found),
            pii_patterns_found=pii_found,
            blocked=blocked,
            block_reason=block_reason
        )

        # Log the audit
        self._log_audit(record, full_prompt if not blocked else "[BLOCKED]")

        return record

    def _is_schema_only(self, prompt: str) -> bool:
        """Check if prompt contains only schema/metadata (no actual data)."""
        lines = prompt.split('\n')

        # Look for data-like patterns (actual values)
        data_patterns = [
            r"'[A-Z0-9]{6,}'",  # Subject IDs
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}',  # ISO timestamps
            r"VALUES\s*\(",  # INSERT VALUES
        ]

        for line in lines:
            for pattern in data_patterns:
                if re.search(pattern, line):
                    return False

        return True

    def _log_audit(self, record: SafetyAuditRecord, prompt_preview: str):
        """Log audit record."""
        # Always log to standard logger
        log_msg = (
            f"LLM Audit: provider={record.provider}, model={record.model}, "
            f"prompt_len={record.prompt_length}, schema_only={record.contains_schema_only}, "
            f"pii_detected={record.potential_pii_detected}, blocked={record.blocked}"
        )

        if record.blocked:
            logger.warning(log_msg)
        else:
            logger.info(log_msg)

        # Write to audit log file if configured
        if self.audit_log_dir:
            log_file = self.audit_log_dir / f"llm_audit_{datetime.now().strftime('%Y%m%d')}.jsonl"
            with open(log_file, 'a') as f:
                audit_entry = {
                    **record.__dict__,
                    'prompt_preview': prompt_preview[:500] if len(prompt_preview) > 500 else prompt_preview
                }
                f.write(json.dumps(audit_entry) + '\n')

    def sanitize_prompt(self, prompt: str) -> str:
        """
        Sanitize prompt by removing potential PII.

        Note: This is a best-effort sanitization. The primary protection
        is the architecture design that only sends schema/metadata.

        Args:
            prompt: Original prompt

        Returns:
            Sanitized prompt
        """
        sanitized = prompt

        for pattern, pii_type in self.PII_PATTERNS:
            sanitized = re.sub(pattern, f'[REDACTED_{pii_type}]', sanitized, flags=re.IGNORECASE)

        return sanitized


# =============================================================================
# BASE LLM PROVIDER
# =============================================================================

class BaseLLMProvider(ABC):
    """Abstract base class for LLM providers."""

    def __init__(self, config: LLMConfig):
        """
        Initialize provider.

        Args:
            config: LLM configuration
        """
        self.config = config
        self.safety_auditor = SafetyAuditor(
            config,
            audit_log_dir=config.audit_log_path
        )

    @abstractmethod
    def generate(self, request: LLMRequest) -> LLMResponse:
        """
        Generate a response from the LLM.

        Args:
            request: LLM request

        Returns:
            LLM response
        """
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """Check if the provider is available."""
        pass

    @abstractmethod
    def get_provider_name(self) -> str:
        """Get the provider name."""
        pass

    @abstractmethod
    def get_model_name(self) -> str:
        """Get the model name."""
        pass

    def _check_safety(self, request: LLMRequest) -> SafetyAuditRecord:
        """
        Run safety audit on request.

        Args:
            request: LLM request

        Returns:
            Safety audit record

        Raises:
            ValueError: If request is blocked due to PII
        """
        record = self.safety_auditor.audit_request(
            request,
            self.get_provider_name(),
            self.get_model_name()
        )

        if record.blocked:
            raise ValueError(f"Request blocked by safety layer: {record.block_reason}")

        return record


# =============================================================================
# CLAUDE PROVIDER
# =============================================================================

class ClaudeProvider(BaseLLMProvider):
    """Claude LLM provider using Anthropic API."""

    def __init__(self, config: LLMConfig):
        super().__init__(config)
        self.api_key = config.anthropic_api_key
        self.model = config.claude_model
        self._client = None

    def get_provider_name(self) -> str:
        return "claude"

    def get_model_name(self) -> str:
        return self.model

    def _get_client(self):
        """Get or create Anthropic client."""
        if self._client is None:
            if not self.api_key:
                raise ValueError(
                    "ANTHROPIC_API_KEY not set. "
                    "Please set the environment variable or configure in settings."
                )
            try:
                import anthropic
                self._client = anthropic.Anthropic(api_key=self.api_key)
            except ImportError:
                raise ImportError(
                    "anthropic package not installed. "
                    "Install with: pip install anthropic"
                )
        return self._client

    def is_available(self) -> bool:
        """Check if Claude API is available."""
        if not self.api_key:
            return False
        try:
            # Just verify we can create a client
            self._get_client()
            return True
        except Exception:
            return False

    def generate(self, request: LLMRequest) -> LLMResponse:
        """Generate response using Claude API."""
        # CRITICAL: Safety audit before external API call
        if self.config.enable_safety_audit:
            audit_record = self._check_safety(request)
            logger.info(
                f"Claude API call approved: schema_only={audit_record.contains_schema_only}, "
                f"prompt_hash={audit_record.prompt_hash}"
            )

        start_time = time.time()
        client = self._get_client()

        # Build messages
        messages = [{"role": "user", "content": request.prompt}]

        try:
            response = client.messages.create(
                model=self.model,
                max_tokens=request.max_tokens,
                system=request.system_prompt or "You are a SQL expert for clinical data analysis.",
                messages=messages,
                temperature=request.temperature
            )

            # Extract content
            content = ""
            if response.content:
                for block in response.content:
                    if hasattr(block, 'text'):
                        content += block.text

            generation_time = (time.time() - start_time) * 1000

            return LLMResponse(
                content=content,
                model=self.model,
                provider="claude",
                generation_time_ms=generation_time,
                tokens_used=response.usage.input_tokens + response.usage.output_tokens if response.usage else None,
                raw_response=response
            )

        except Exception as e:
            error_msg = str(e)
            if "authentication" in error_msg.lower() or "api key" in error_msg.lower():
                raise ValueError(f"Claude API authentication failed: {error_msg}")
            elif "rate" in error_msg.lower():
                raise RuntimeError(f"Claude API rate limited: {error_msg}")
            else:
                raise RuntimeError(f"Claude API error: {error_msg}")


# =============================================================================
# MOCK PROVIDER (for testing)
# =============================================================================

class MockProvider(BaseLLMProvider):
    """Mock LLM provider for testing."""

    def get_provider_name(self) -> str:
        return "mock"

    def get_model_name(self) -> str:
        return "mock-model"

    def is_available(self) -> bool:
        return True

    def generate(self, request: LLMRequest) -> LLMResponse:
        """Generate mock response."""
        start_time = time.time()

        # Generate basic SQL based on prompt patterns
        prompt_lower = request.prompt.lower()

        if 'count' in prompt_lower or 'how many' in prompt_lower:
            sql = "SELECT COUNT(DISTINCT USUBJID) as subject_count FROM ADSL WHERE SAFFL = 'Y'"
        elif 'adverse' in prompt_lower or 'ae' in prompt_lower:
            sql = "SELECT AEDECOD, COUNT(*) as count FROM ADAE WHERE SAFFL = 'Y' GROUP BY AEDECOD ORDER BY count DESC LIMIT 10"
        else:
            sql = "SELECT * FROM ADSL WHERE SAFFL = 'Y' LIMIT 10"

        content = f"```sql\n{sql}\n```"
        generation_time = (time.time() - start_time) * 1000

        return LLMResponse(
            content=content,
            model="mock-model",
            provider="mock",
            generation_time_ms=generation_time
        )


# =============================================================================
# PROVIDER FACTORY
# =============================================================================

def create_llm_provider(config: Optional[LLMConfig] = None) -> BaseLLMProvider:
    """
    Create an LLM provider based on configuration.

    Args:
        config: LLM configuration (uses env vars if not provided)

    Returns:
        Configured LLM provider (Claude or Mock)
    """
    if config is None:
        config = LLMConfig.from_env()

    logger.info(f"Creating LLM provider: {config.provider.value}")

    if config.provider == LLMProvider.MOCK:
        return MockProvider(config)

    # Default to Claude
    provider = ClaudeProvider(config)
    if not provider.is_available():
        logger.error("Claude API not available - check ANTHROPIC_API_KEY")
        raise ValueError(
            "Claude API key not configured. "
            "Please set ANTHROPIC_API_KEY in your environment."
        )
    return provider


def get_available_providers() -> Dict[str, bool]:
    """
    Check which providers are available.

    Returns:
        Dict mapping provider name to availability
    """
    config = LLMConfig.from_env()

    return {
        "claude": ClaudeProvider(config).is_available() if config.anthropic_api_key else False,
        "mock": True
    }


# =============================================================================
# RUNTIME PROVIDER MANAGEMENT (Thread-safe)
# =============================================================================

_current_provider: Optional[BaseLLMProvider] = None
_provider_lock = threading.Lock()


def get_current_provider() -> BaseLLMProvider:
    """
    Get the current LLM provider (creates one if needed).

    Uses double-checked locking for thread-safe singleton initialization.
    """
    global _current_provider

    # Fast path: if already initialized, return immediately
    if _current_provider is not None:
        return _current_provider

    # Slow path: acquire lock and check again
    with _provider_lock:
        # Double-check after acquiring lock
        if _current_provider is None:
            _current_provider = create_llm_provider()

    return _current_provider


def set_provider(provider: LLMProvider, **kwargs) -> BaseLLMProvider:
    """
    Set the current LLM provider.

    Thread-safe: acquires lock before modifying.

    Args:
        provider: Provider to use
        **kwargs: Additional config overrides

    Returns:
        The new provider
    """
    global _current_provider

    with _provider_lock:
        config = LLMConfig.from_env()
        config.provider = provider

        # Apply overrides
        for key, value in kwargs.items():
            if hasattr(config, key):
                setattr(config, key, value)

        _current_provider = create_llm_provider(config)
        logger.info(f"LLM provider set to: {provider.value}")

    return _current_provider


def reset_provider():
    """Reset the current provider (forces recreation on next use)."""
    global _current_provider
    with _provider_lock:
        _current_provider = None
