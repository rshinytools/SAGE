"""
Settings Defaults
=================
Default values and definitions for all SAGE platform settings.
"""

from typing import Dict, List, Any
from .schemas import SettingDefinition, SettingType


# Category definitions with display order
SETTING_CATEGORIES = {
    "general": {
        "name": "General",
        "description": "General platform configuration",
        "icon": "Settings",
        "order": 1,
    },
    "auth": {
        "name": "Authentication & Security",
        "description": "User authentication and security policies",
        "icon": "Shield",
        "order": 2,
    },
    "llm": {
        "name": "LLM Configuration",
        "description": "AI model and inference settings",
        "icon": "Brain",
        "order": 3,
    },
    "data": {
        "name": "Data Factory",
        "description": "Data processing and storage settings",
        "icon": "Database",
        "order": 4,
    },
    "metadata": {
        "name": "Metadata Factory",
        "description": "Metadata management and approval workflow",
        "icon": "FileSpreadsheet",
        "order": 5,
    },
    "dictionary": {
        "name": "Dictionary",
        "description": "Fuzzy matching and vector search settings",
        "icon": "BookOpen",
        "order": 6,
    },
    "audit": {
        "name": "Audit & Compliance",
        "description": "Logging, retention, and regulatory compliance",
        "icon": "ScrollText",
        "order": 7,
    },
    "system": {
        "name": "System & Performance",
        "description": "Cache, timeouts, and performance tuning",
        "icon": "Gauge",
        "order": 8,
    },
}


# All setting definitions grouped by category
SETTING_DEFINITIONS: Dict[str, List[SettingDefinition]] = {
    "general": [
        SettingDefinition(
            key="site_name",
            label="Site Name",
            description="Platform display name shown in header and title",
            value_type=SettingType.STRING,
            default_value="SAGE",
        ),
        SettingDefinition(
            key="site_description",
            label="Site Description",
            description="Platform tagline or subtitle",
            value_type=SettingType.STRING,
            default_value="Study Analytics Generative Engine",
        ),
        SettingDefinition(
            key="default_theme",
            label="Default Theme",
            description="Default color theme for new users",
            value_type=SettingType.ENUM,
            default_value="system",
            options=["light", "dark", "system"],
        ),
        SettingDefinition(
            key="timezone",
            label="Timezone",
            description="System timezone for timestamps",
            value_type=SettingType.STRING,
            default_value="UTC",
        ),
        SettingDefinition(
            key="date_format",
            label="Date Format",
            description="Display format for dates",
            value_type=SettingType.ENUM,
            default_value="YYYY-MM-DD",
            options=["YYYY-MM-DD", "MM/DD/YYYY", "DD/MM/YYYY", "DD-MMM-YYYY"],
        ),
        SettingDefinition(
            key="maintenance_mode",
            label="Maintenance Mode",
            description="When enabled, only admins can access the platform",
            value_type=SettingType.BOOLEAN,
            default_value=False,
        ),
    ],
    "auth": [
        SettingDefinition(
            key="session_timeout_minutes",
            label="Session Timeout",
            description="Minutes until JWT access token expires",
            value_type=SettingType.NUMBER,
            default_value=60,
            min_value=5,
            max_value=1440,
        ),
        SettingDefinition(
            key="refresh_token_days",
            label="Refresh Token Lifetime",
            description="Days until refresh token expires",
            value_type=SettingType.NUMBER,
            default_value=7,
            min_value=1,
            max_value=30,
        ),
        SettingDefinition(
            key="max_login_attempts",
            label="Max Login Attempts",
            description="Failed attempts before account lockout",
            value_type=SettingType.NUMBER,
            default_value=5,
            min_value=3,
            max_value=10,
        ),
        SettingDefinition(
            key="lockout_duration_minutes",
            label="Lockout Duration",
            description="Minutes until locked account unlocks",
            value_type=SettingType.NUMBER,
            default_value=30,
            min_value=5,
            max_value=1440,
        ),
        SettingDefinition(
            key="password_min_length",
            label="Minimum Password Length",
            description="Minimum characters required for passwords",
            value_type=SettingType.NUMBER,
            default_value=8,
            min_value=6,
            max_value=32,
        ),
        SettingDefinition(
            key="password_require_uppercase",
            label="Require Uppercase",
            description="Passwords must contain uppercase letter",
            value_type=SettingType.BOOLEAN,
            default_value=True,
        ),
        SettingDefinition(
            key="password_require_number",
            label="Require Number",
            description="Passwords must contain numeric digit",
            value_type=SettingType.BOOLEAN,
            default_value=True,
        ),
        SettingDefinition(
            key="password_require_special",
            label="Require Special Character",
            description="Passwords must contain special character",
            value_type=SettingType.BOOLEAN,
            default_value=False,
        ),
        SettingDefinition(
            key="allow_registration",
            label="Allow Self-Registration",
            description="Allow new users to register themselves",
            value_type=SettingType.BOOLEAN,
            default_value=False,
        ),
    ],
    "llm": [
        SettingDefinition(
            key="llm_provider",
            label="LLM Provider",
            description="AI model provider for query processing",
            value_type=SettingType.ENUM,
            default_value="anthropic",
            options=["anthropic", "openai", "ollama"],
        ),
        SettingDefinition(
            key="llm_model",
            label="Model",
            description="Specific model identifier",
            value_type=SettingType.STRING,
            default_value="claude-sonnet-4-20250514",
        ),
        SettingDefinition(
            key="llm_api_key",
            label="API Key",
            description="API key for cloud LLM providers",
            value_type=SettingType.PASSWORD,
            default_value="",
            is_sensitive=True,
            required=False,
        ),
        SettingDefinition(
            key="llm_base_url",
            label="Base URL",
            description="Custom API endpoint (for Ollama or proxies)",
            value_type=SettingType.STRING,
            default_value="",
            required=False,
        ),
        SettingDefinition(
            key="llm_temperature",
            label="Temperature",
            description="Response randomness (0=deterministic, 1=creative)",
            value_type=SettingType.NUMBER,
            default_value=0.0,
            min_value=0.0,
            max_value=1.0,
        ),
        SettingDefinition(
            key="llm_max_tokens",
            label="Max Tokens",
            description="Maximum response length in tokens",
            value_type=SettingType.NUMBER,
            default_value=4096,
            min_value=256,
            max_value=16384,
        ),
        SettingDefinition(
            key="llm_timeout_seconds",
            label="Request Timeout",
            description="Seconds before LLM request times out",
            value_type=SettingType.NUMBER,
            default_value=60,
            min_value=10,
            max_value=300,
        ),
        SettingDefinition(
            key="confidence_threshold_high",
            label="High Confidence Threshold",
            description="Score above which responses are high confidence",
            value_type=SettingType.NUMBER,
            default_value=90,
            min_value=70,
            max_value=100,
        ),
        SettingDefinition(
            key="confidence_threshold_medium",
            label="Medium Confidence Threshold",
            description="Score above which responses are medium confidence",
            value_type=SettingType.NUMBER,
            default_value=70,
            min_value=50,
            max_value=90,
        ),
    ],
    "data": [
        SettingDefinition(
            key="max_upload_size_mb",
            label="Max Upload Size",
            description="Maximum file upload size in megabytes",
            value_type=SettingType.NUMBER,
            default_value=500,
            min_value=10,
            max_value=5000,
        ),
        SettingDefinition(
            key="allowed_file_types",
            label="Allowed File Types",
            description="File extensions that can be uploaded",
            value_type=SettingType.ARRAY,
            default_value=[".sas7bdat", ".parquet", ".csv", ".xpt"],
        ),
        SettingDefinition(
            key="auto_process_uploads",
            label="Auto-Process Uploads",
            description="Automatically process files after upload",
            value_type=SettingType.BOOLEAN,
            default_value=True,
        ),
        SettingDefinition(
            key="backup_before_replace",
            label="Backup Before Replace",
            description="Create backup when replacing existing tables",
            value_type=SettingType.BOOLEAN,
            default_value=True,
        ),
        SettingDefinition(
            key="duckdb_memory_limit",
            label="DuckDB Memory Limit",
            description="Maximum memory for DuckDB operations",
            value_type=SettingType.STRING,
            default_value="4GB",
        ),
        SettingDefinition(
            key="duckdb_threads",
            label="DuckDB Threads",
            description="Number of parallel threads for DuckDB",
            value_type=SettingType.NUMBER,
            default_value=4,
            min_value=1,
            max_value=32,
        ),
    ],
    "metadata": [
        SettingDefinition(
            key="require_approval",
            label="Require Approval",
            description="Metadata changes require approval workflow",
            value_type=SettingType.BOOLEAN,
            default_value=True,
        ),
        SettingDefinition(
            key="auto_draft_enabled",
            label="Auto-Generate Drafts",
            description="Use AI to generate plain English descriptions",
            value_type=SettingType.BOOLEAN,
            default_value=True,
        ),
        SettingDefinition(
            key="approval_workflow",
            label="Approval Workflow",
            description="Type of approval process required",
            value_type=SettingType.ENUM,
            default_value="single",
            options=["single", "dual", "committee"],
        ),
        SettingDefinition(
            key="notify_on_pending",
            label="Notify on Pending",
            description="Send notifications for pending approvals",
            value_type=SettingType.BOOLEAN,
            default_value=False,
        ),
    ],
    "dictionary": [
        SettingDefinition(
            key="fuzzy_match_threshold",
            label="Fuzzy Match Threshold",
            description="Minimum similarity score for fuzzy matches (0-1)",
            value_type=SettingType.NUMBER,
            default_value=0.8,
            min_value=0.5,
            max_value=1.0,
        ),
        SettingDefinition(
            key="vector_similarity_weight",
            label="Vector Similarity Weight",
            description="Weight for vector-based similarity scoring",
            value_type=SettingType.NUMBER,
            default_value=0.5,
            min_value=0.0,
            max_value=1.0,
        ),
        SettingDefinition(
            key="fuzzy_similarity_weight",
            label="Fuzzy Similarity Weight",
            description="Weight for fuzzy string matching scoring",
            value_type=SettingType.NUMBER,
            default_value=0.5,
            min_value=0.0,
            max_value=1.0,
        ),
        SettingDefinition(
            key="auto_rebuild_enabled",
            label="Auto-Rebuild Dictionary",
            description="Automatically rebuild dictionary on schedule",
            value_type=SettingType.BOOLEAN,
            default_value=False,
        ),
        SettingDefinition(
            key="embedding_model",
            label="Embedding Model",
            description="Model used for text embeddings",
            value_type=SettingType.STRING,
            default_value="nomic-embed-text",
        ),
    ],
    "audit": [
        SettingDefinition(
            key="audit_log_retention_days",
            label="Log Retention",
            description="Days to keep audit logs (0=forever)",
            value_type=SettingType.NUMBER,
            default_value=365,
            min_value=0,
            max_value=3650,
        ),
        SettingDefinition(
            key="audit_log_api_requests",
            label="Log API Requests",
            description="Log all API calls to audit trail",
            value_type=SettingType.BOOLEAN,
            default_value=True,
        ),
        SettingDefinition(
            key="audit_log_queries",
            label="Log Queries",
            description="Log all chat queries to audit trail",
            value_type=SettingType.BOOLEAN,
            default_value=True,
        ),
        SettingDefinition(
            key="audit_include_response",
            label="Include Responses",
            description="Store AI responses in audit logs",
            value_type=SettingType.BOOLEAN,
            default_value=False,
        ),
        SettingDefinition(
            key="audit_checksum_enabled",
            label="Enable Checksums",
            description="Calculate integrity checksums for logs",
            value_type=SettingType.BOOLEAN,
            default_value=True,
        ),
        SettingDefinition(
            key="audit_export_format",
            label="Export Format",
            description="Default format for audit log exports",
            value_type=SettingType.ENUM,
            default_value="json",
            options=["json", "csv"],
        ),
    ],
    "system": [
        SettingDefinition(
            key="cache_enabled",
            label="Enable Cache",
            description="Cache query results for faster responses",
            value_type=SettingType.BOOLEAN,
            default_value=True,
        ),
        SettingDefinition(
            key="cache_ttl_seconds",
            label="Cache TTL",
            description="Seconds before cached results expire",
            value_type=SettingType.NUMBER,
            default_value=3600,
            min_value=60,
            max_value=86400,
        ),
        SettingDefinition(
            key="cache_max_size_mb",
            label="Cache Max Size",
            description="Maximum cache size in megabytes",
            value_type=SettingType.NUMBER,
            default_value=100,
            min_value=10,
            max_value=1000,
        ),
        SettingDefinition(
            key="query_timeout_seconds",
            label="Query Timeout",
            description="Maximum seconds for query execution",
            value_type=SettingType.NUMBER,
            default_value=30,
            min_value=5,
            max_value=300,
        ),
        SettingDefinition(
            key="max_concurrent_queries",
            label="Max Concurrent Queries",
            description="Maximum simultaneous query executions",
            value_type=SettingType.NUMBER,
            default_value=10,
            min_value=1,
            max_value=50,
        ),
        SettingDefinition(
            key="dashboard_refresh_seconds",
            label="Dashboard Refresh",
            description="Seconds between dashboard auto-refresh",
            value_type=SettingType.NUMBER,
            default_value=30,
            min_value=10,
            max_value=300,
        ),
        SettingDefinition(
            key="health_check_interval",
            label="Health Check Interval",
            description="Seconds between service health checks",
            value_type=SettingType.NUMBER,
            default_value=60,
            min_value=10,
            max_value=300,
        ),
    ],
}


def get_default_value(category: str, key: str) -> Any:
    """Get the default value for a setting."""
    for definition in SETTING_DEFINITIONS.get(category, []):
        if definition.key == key:
            return definition.default_value
    return None


def get_all_defaults() -> Dict[str, Dict[str, Any]]:
    """Get all default values grouped by category."""
    defaults = {}
    for category, definitions in SETTING_DEFINITIONS.items():
        defaults[category] = {}
        for definition in definitions:
            defaults[category][definition.key] = definition.default_value
    return defaults
