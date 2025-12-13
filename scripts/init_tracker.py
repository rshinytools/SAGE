#!/usr/bin/env python3
"""
SAGE Project Tracker Initialization Script

Seeds the project tracker database with all phases, tasks, and subtasks
defined in the implementation plan.

Usage:
    python scripts/init_tracker.py [--reset]

Options:
    --reset     Drop all existing data and reinitialize
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.admin.tracker_db import TrackerDB


def init_tracker(reset: bool = False):
    """Initialize the project tracker with all tasks."""

    db_path = project_root / "tracker" / "project_tracker.db"

    if reset and db_path.exists():
        print("Resetting database...")
        db_path.unlink()

    tracker = TrackerDB(str(db_path))
    print(f"Database initialized at: {db_path}")

    # Check if already initialized
    phases = tracker.get_all_phases()
    if phases:
        print(f"Database already contains {len(phases)} phases.")
        if not reset:
            print("Use --reset to reinitialize.")
            return
        print("Reinitializing...")

    # ==================== PHASE 0: Project Tracker Setup ====================
    phase_0 = tracker.create_phase(
        name="Phase 0: Project Tracker Setup",
        description="Set up the project tracking system and Admin UI foundation",
        order_index=0,
        estimated_hours=16
    )

    tasks_phase_0 = [
        ("Create directory structure", "Set up all project directories", "high", [
            "Create core/ directory",
            "Create docker/ directory",
            "Create scripts/ directory",
            "Create data/ subdirectories",
            "Create specs/ directory",
            "Create knowledge/ directory",
            "Create logs/ directory",
            "Create tracker/ directory"
        ]),
        ("Set up Python package structure", "Create __init__.py files and requirements", "high", [
            "Create core/__init__.py",
            "Create core/admin/__init__.py",
            "Create core/data/__init__.py",
            "Create core/metadata/__init__.py",
            "Create core/dictionary/__init__.py",
            "Create core/engine/__init__.py",
            "Create requirements.txt"
        ]),
        ("Create tracker database module", "Implement SQLite operations for tracking", "critical", [
            "Define database schema",
            "Implement CRUD for phases",
            "Implement CRUD for tasks",
            "Implement CRUD for subtasks",
            "Implement activity logging",
            "Implement progress calculations"
        ]),
        ("Create tracker initialization script", "Script to seed all phases and tasks", "high", [
            "Define all phases",
            "Define all tasks and subtasks",
            "Create seeding logic"
        ]),
        ("Build Admin UI authentication", "Implement login and session management", "critical", [
            "Create auth provider interface",
            "Implement local auth provider",
            "Create LDAP auth scaffold",
            "Implement session management",
            "Create login page"
        ]),
        ("Build Project Tracker page", "Full-featured tracking UI in Streamlit", "critical", [
            "Create overview dashboard",
            "Implement phase progress bars",
            "Create task management interface",
            "Implement subtask checklists",
            "Add next steps panel",
            "Create activity feed",
            "Add export functionality"
        ]),
        ("Create .env configuration", "Environment variables and configuration", "medium", [
            "Define all environment variables",
            "Create .env.example template",
            "Document configuration options"
        ])
    ]

    for name, desc, priority, subtasks in tasks_phase_0:
        task_id = tracker.create_task(phase_0, name, desc, priority)
        for subtask in subtasks:
            tracker.create_subtask(task_id, subtask)

    print(f"Phase 0 created with {len(tasks_phase_0)} tasks")

    # ==================== PHASE 1: Infrastructure & Foundation ====================
    phase_1 = tracker.create_phase(
        name="Phase 1: Infrastructure & Foundation",
        description="Set up the server environment and Docker deployment",
        order_index=1,
        estimated_hours=40
    )

    tasks_phase_1 = [
        ("Install NVIDIA drivers on server", "Set up GPU drivers for LLM inference", "critical", [
            "Verify GPU detection with lspci",
            "Install NVIDIA driver package",
            "Verify with nvidia-smi"
        ]),
        ("Install NVIDIA Container Toolkit", "Enable GPU access from Docker", "critical", [
            "Add NVIDIA container toolkit repo",
            "Install nvidia-container-toolkit",
            "Restart Docker daemon",
            "Verify with test container"
        ]),
        ("Install Docker & Docker Compose", "Container runtime setup", "critical", [
            "Install Docker Engine",
            "Install Docker Compose",
            "Configure Docker for non-root",
            "Test Docker installation"
        ]),
        ("Install Ollama", "Local LLM server setup", "critical", [
            "Download and install Ollama",
            "Configure OLLAMA_HOST",
            "Start Ollama service",
            "Verify API endpoint"
        ]),
        ("Pull LLaMA 3.1 70B model", "Download primary model", "high", [
            "Pull llama3.1:70b-instruct-q4_K_M",
            "Verify model loads correctly",
            "Test inference"
        ]),
        ("Pull LLaMA 3.1 8B fallback model", "Download fallback model", "medium", [
            "Pull llama3.1:8b-instruct-q8_0",
            "Verify model loads"
        ]),
        ("Pull nomic-embed-text model", "Download embedding model", "high", [
            "Pull nomic-embed-text",
            "Test embedding generation"
        ]),
        ("Create docker-compose.yml", "Define all services", "critical", [
            "Define Ollama service",
            "Define Chat UI service",
            "Define Admin UI service",
            "Define API service",
            "Define Docs service",
            "Define Prometheus service",
            "Define Grafana service",
            "Define ChromaDB service",
            "Configure volumes and networks"
        ]),
        ("Create Ollama Dockerfile", "Custom Ollama container", "medium", [
            "Base from ollama/ollama",
            "Configure GPU passthrough",
            "Set environment variables"
        ]),
        ("Create Chat UI Dockerfile", "Chainlit application container", "high", [
            "Base from Python image",
            "Install dependencies",
            "Copy application code",
            "Configure entrypoint"
        ]),
        ("Create Admin UI Dockerfile", "Streamlit application container", "high", [
            "Base from Python image",
            "Install dependencies",
            "Copy application code",
            "Configure Streamlit settings"
        ]),
        ("Create API Dockerfile", "FastAPI service container", "medium", [
            "Base from Python image",
            "Install FastAPI and dependencies",
            "Copy application code"
        ]),
        ("Create startup scripts", "One-click deployment scripts", "high", [
            "Create start.sh for Linux",
            "Create start.bat for Windows",
            "Add prerequisite checks",
            "Add model pull automation"
        ]),
        ("Test one-click deployment", "Verify complete deployment", "critical", [
            "Test docker compose up",
            "Verify all containers start",
            "Test health check endpoints",
            "Verify inter-service communication"
        ])
    ]

    for name, desc, priority, subtasks in tasks_phase_1:
        task_id = tracker.create_task(phase_1, name, desc, priority)
        for subtask in subtasks:
            tracker.create_subtask(task_id, subtask)

    print(f"Phase 1 created with {len(tasks_phase_1)} tasks")

    # ==================== PHASE 2: Data Factory ====================
    phase_2 = tracker.create_phase(
        name="Phase 2: Data Factory (Factory 1)",
        description="Build the ETL pipeline for SAS to DuckDB conversion",
        order_index=2,
        estimated_hours=40
    )

    tasks_phase_2 = [
        ("Create SAS7BDAT reader module", "Read SAS files with pandas", "critical", [
            "Implement read_sas wrapper",
            "Handle encoding issues",
            "Handle large files efficiently"
        ]),
        ("Implement date standardization", "Convert dates to ISO 8601", "high", [
            "Identify date columns",
            "Convert SAS dates to Python dates",
            "Handle timezone considerations"
        ]),
        ("Implement partial date imputation", "Handle incomplete dates", "high", [
            "Define imputation rules",
            "Create imputed date columns",
            "Document imputation logic"
        ]),
        ("Create Parquet converter", "Convert to columnar format", "high", [
            "Implement Parquet writer",
            "Optimize compression settings",
            "Preserve data types"
        ]),
        ("Create DuckDB loader", "Load data into DuckDB", "critical", [
            "Create database connection",
            "Generate CREATE TABLE statements",
            "Load Parquet files",
            "Create indexes"
        ]),
        ("Implement row count validation", "Verify data integrity", "high", [
            "Compare SAS row counts",
            "Compare DuckDB row counts",
            "Generate validation report"
        ]),
        ("Implement column type verification", "Validate schema", "medium", [
            "Compare source and target types",
            "Flag type mismatches",
            "Generate type report"
        ]),
        ("Implement null value reporting", "Track data quality", "medium", [
            "Count nulls per column",
            "Calculate null percentages",
            "Generate quality report"
        ]),
        ("Load test SDTM datasets", "Process standard SDTM domains", "high", [
            "Load DM (Demographics)",
            "Load AE (Adverse Events)",
            "Load CM (Concomitant Meds)",
            "Load LB (Labs)",
            "Load VS (Vital Signs)"
        ]),
        ("Load test ADaM datasets", "Process analysis datasets", "high", [
            "Load ADSL (Subject Level)",
            "Load ADAE (Adverse Events)",
            "Load ADLB (Labs)"
        ]),
        ("Build Admin UI data management page", "Data upload and processing UI", "high", [
            "Create file upload widget",
            "Show processing progress",
            "Display validation results",
            "Add dataset browser"
        ]),
        ("Create data refresh automation", "Scheduled data updates", "medium", [
            "Design refresh trigger",
            "Implement incremental update",
            "Add scheduling option"
        ])
    ]

    for name, desc, priority, subtasks in tasks_phase_2:
        task_id = tracker.create_task(phase_2, name, desc, priority)
        for subtask in subtasks:
            tracker.create_subtask(task_id, subtask)

    print(f"Phase 2 created with {len(tasks_phase_2)} tasks")

    # ==================== PHASE 3: Metadata Factory ====================
    phase_3 = tracker.create_phase(
        name="Phase 3: Metadata Factory (Factory 2)",
        description="Build the metadata curation pipeline with human approval",
        order_index=3,
        estimated_hours=60
    )

    tasks_phase_3 = [
        ("Create Excel spec parser", "Parse SDTM/ADaM specifications", "critical", [
            "Read domain sheets",
            "Parse variable definitions",
            "Extract codelists",
            "Handle different spec formats"
        ]),
        ("Implement codelist merger", "Combine codelists with variables", "high", [
            "Match codelist references",
            "Expand codelist values",
            "Handle missing codelists"
        ]),
        ("Create LLM drafting module", "Generate plain-English descriptions", "high", [
            "Design prompts for derivations",
            "Call Ollama API",
            "Parse LLM responses",
            "Handle complex logic"
        ]),
        ("Build metadata review UI", "Admin interface for approval", "critical", [
            "Create variable grid view",
            "Show raw spec vs AI draft",
            "Add edit capability",
            "Style approval buttons"
        ]),
        ("Implement approval workflow", "Track variable approvals", "critical", [
            "Record approver name",
            "Record approval timestamp",
            "Track approval status",
            "Require approval for use"
        ]),
        ("Implement version control", "Track metadata changes", "high", [
            "Store version history",
            "Track who changed what",
            "Allow rollback"
        ]),
        ("Implement delta detection", "Highlight spec changes", "medium", [
            "Compare new vs old specs",
            "Highlight new variables",
            "Highlight changed definitions",
            "Flag removed variables"
        ]),
        ("Generate golden_metadata.json", "Export approved metadata", "critical", [
            "Define JSON schema",
            "Export approved entries",
            "Include version info"
        ]),
        ("Review ADSL variables", "Approve subject-level metadata", "high", [
            "Review all ADSL variables",
            "Approve or edit each",
            "Document edge cases"
        ]),
        ("Review AE variables", "Approve adverse event metadata", "high", [
            "Review all AE variables",
            "Approve or edit each"
        ]),
        ("Review CM variables", "Approve conmed metadata", "high", [
            "Review all CM variables",
            "Approve or edit each"
        ]),
        ("Review LB variables", "Approve lab metadata", "medium", [
            "Review all LB variables",
            "Approve or edit each"
        ]),
        ("Review VS variables", "Approve vital signs metadata", "medium", [
            "Review all VS variables",
            "Approve or edit each"
        ]),
        ("Create metadata backup system", "Automated backups", "medium", [
            "Schedule daily backups",
            "Store backup copies",
            "Implement restore"
        ]),
        ("Build metadata search", "Search across definitions", "medium", [
            "Full-text search",
            "Filter by dataset",
            "Filter by approval status"
        ])
    ]

    for name, desc, priority, subtasks in tasks_phase_3:
        task_id = tracker.create_task(phase_3, name, desc, priority)
        for subtask in subtasks:
            tracker.create_subtask(task_id, subtask)

    print(f"Phase 3 created with {len(tasks_phase_3)} tasks")

    # ==================== PHASE 4: Dictionary Factory ====================
    phase_4 = tracker.create_phase(
        name="Phase 4: Dictionary Factory (Factory 3)",
        description="Build the fuzzy matching and entity resolution system",
        order_index=4,
        estimated_hours=40
    )

    tasks_phase_4 = [
        ("Create value scanner module", "Extract unique values from data", "critical", [
            "Identify key columns",
            "Extract unique values",
            "Store value counts"
        ]),
        ("Scan AETERM/AEDECOD values", "Index adverse event terms", "high", [
            "Extract unique AE terms",
            "Store in index",
            "Generate embeddings"
        ]),
        ("Scan CMTRT/CMDECOD values", "Index medication names", "high", [
            "Extract unique med names",
            "Store in index",
            "Generate embeddings"
        ]),
        ("Scan LBTEST/LBTESTCD values", "Index lab test names", "high", [
            "Extract unique lab tests",
            "Store in index",
            "Generate embeddings"
        ]),
        ("Set up ChromaDB", "Configure vector database", "critical", [
            "Create ChromaDB collection",
            "Configure persistence",
            "Test basic operations"
        ]),
        ("Generate embeddings", "Create vectors for all terms", "high", [
            "Connect to nomic-embed-text",
            "Batch generate embeddings",
            "Store in ChromaDB"
        ]),
        ("Implement RapidFuzz matching", "Levenshtein distance matching", "high", [
            "Install RapidFuzz",
            "Implement fuzzy search",
            "Tune similarity threshold"
        ]),
        ("Implement phonetic matching", "Double Metaphone algorithm", "medium", [
            "Implement phonetic encoder",
            "Create phonetic index",
            "Test phonetic matches"
        ]),
        ("Build hybrid matching pipeline", "Combine all methods", "critical", [
            "Layer 1: Exact match",
            "Layer 2: Levenshtein",
            "Layer 3: Phonetic",
            "Layer 4: Vector similarity",
            "Return confidence scores"
        ]),
        ("Create schema_map.json", "Define table relationships", "high", [
            "Map primary keys",
            "Map foreign keys",
            "Define join paths"
        ]),
        ("Build synonym manager UI", "Admin interface for synonyms", "medium", [
            "List existing synonyms",
            "Add new synonyms",
            "Edit/delete synonyms"
        ]),
        ("Test fuzzy matching accuracy", "Validate corrections", "critical", [
            "Test Tyleonl -> TYLENOL",
            "Test safty -> Safety",
            "Test headake -> HEADACHE",
            "Measure precision/recall"
        ]),
        ("Create dictionary refresh automation", "Update after data changes", "medium", [
            "Trigger on data refresh",
            "Rebuild indexes",
            "Update embeddings"
        ])
    ]

    for name, desc, priority, subtasks in tasks_phase_4:
        task_id = tracker.create_task(phase_4, name, desc, priority)
        for subtask in subtasks:
            tracker.create_subtask(task_id, subtask)

    print(f"Phase 4 created with {len(tasks_phase_4)} tasks")

    # ==================== PHASE 5: Inference Engine ====================
    phase_5 = tracker.create_phase(
        name="Phase 5: Inference Engine (Factory 4)",
        description="Build the query processing pipeline and chat interface",
        order_index=5,
        estimated_hours=80
    )

    tasks_phase_5 = [
        ("Set up LlamaIndex", "Configure RAG framework", "critical", [
            "Install LlamaIndex",
            "Configure Ollama backend",
            "Set up service context"
        ]),
        ("Create input sanitizer", "Security and validation layer", "critical", [
            "PHI/PII detection",
            "SQL injection blocking",
            "Prompt injection detection",
            "Input length limits"
        ]),
        ("Implement query router", "Classify query intent", "critical", [
            "Define routing categories",
            "Train/configure classifier",
            "Route to DATA pipeline",
            "Route to DOCUMENT pipeline",
            "Handle HYBRID queries"
        ]),
        ("Create SQL generation agent", "LLM-based SQL writing", "critical", [
            "Design SQL generation prompt",
            "Include schema context",
            "Include metadata context",
            "Generate DuckDB SQL"
        ]),
        ("Implement SQL validation", "Validate generated SQL", "critical", [
            "Parse SQL syntax",
            "Verify table/column names",
            "Block dangerous operations",
            "Estimate query complexity"
        ]),
        ("Create sandboxed executor", "Safe code execution", "critical", [
            "Set timeout limits",
            "Limit memory usage",
            "Restrict operations",
            "Capture errors safely"
        ]),
        ("Implement DuckDB query execution", "Run validated queries", "high", [
            "Connect to DuckDB",
            "Execute SQL",
            "Format results",
            "Handle large results"
        ]),
        ("Implement confidence scoring", "4-component weighted score", "critical", [
            "Dictionary match (40%)",
            "Metadata coverage (30%)",
            "Execution success (20%)",
            "Result sanity (10%)",
            "Calculate total score"
        ]),
        ("Create explanation generator", "Generate methodology text", "high", [
            "Template-based generation",
            "List filters applied",
            "Show data sources",
            "Explain calculations"
        ]),
        ("Build Chainlit chat interface", "Production chat UI", "critical", [
            "Set up Chainlit app",
            "Configure streaming",
            "Add chat history",
            "Style interface"
        ]),
        ("Implement user authentication", "Chat UI login", "high", [
            "Add login page",
            "Integrate auth provider",
            "Protect routes"
        ]),
        ("Create chat session management", "Persist conversations", "medium", [
            "Store chat history",
            "Load previous sessions",
            "Clear session option"
        ]),
        ("Implement audit logging", "Log all queries", "critical", [
            "Log user query",
            "Log generated SQL",
            "Log results",
            "Log confidence score",
            "Log timestamp and user"
        ]),
        ("Build confidence display", "Traffic light indicator", "high", [
            "Green for 90-100%",
            "Yellow for 70-89%",
            "Orange for 50-69%",
            "Red for below 50%"
        ]),
        ("Create expandable methodology section", "Show reasoning", "medium", [
            "Show SQL query",
            "Show metadata used",
            "Show corrections made"
        ]),
        ("Test population queries", "Validate basic queries", "critical", [
            "How many subjects in Safety Population?",
            "Count ITT subjects by arm",
            "Verify accuracy"
        ]),
        ("Test adverse event queries", "Validate AE queries", "critical", [
            "Subjects with fever",
            "Subjects with fever who took Tylenol",
            "Verify fuzzy matching"
        ]),
        ("Test complex queries", "Validate multi-join queries", "high", [
            "Multi-table joins",
            "Time window filters",
            "Aggregations"
        ])
    ]

    for name, desc, priority, subtasks in tasks_phase_5:
        task_id = tracker.create_task(phase_5, name, desc, priority)
        for subtask in subtasks:
            tracker.create_subtask(task_id, subtask)

    print(f"Phase 5 created with {len(tasks_phase_5)} tasks")

    # ==================== PHASE 6: Admin Panel & Monitoring ====================
    phase_6 = tracker.create_phase(
        name="Phase 6: Admin Panel & Monitoring",
        description="Complete admin UI and system observability",
        order_index=6,
        estimated_hours=60
    )

    tasks_phase_6 = [
        ("Migrate Admin UI to React", "Replace Streamlit with modern React UI", "critical", [
            "Set up Vite + React + TypeScript project",
            "Implement WordPress Darkr theme with light/dark mode",
            "Create all admin pages (Dashboard, Data, Metadata, etc.)",
            "Build AI Chat interface (replacing Chainlit)",
            "Configure NGINX API Gateway",
            "Docker multi-stage build setup",
            "Remove Streamlit and Chainlit services"
        ]),
        ("Build main dashboard", "System overview page", "high", [
            "Container health status",
            "Key metrics display",
            "Activity feed",
            "Quick actions"
        ]),
        ("Build data management page", "Data upload and status", "high", [
            "File upload interface",
            "Processing queue",
            "Validation reports",
            "Dataset browser"
        ]),
        ("Build metadata auditor page", "Metadata review interface", "high", [
            "Variable review grid",
            "Approve/Reject/Edit",
            "Version history",
            "Export options"
        ]),
        ("Build dictionary manager page", "Fuzzy matching tools", "medium", [
            "Value index browser",
            "Fuzzy match tester",
            "Synonym manager"
        ]),
        ("Build user management page", "User CRUD operations", "high", [
            "List users",
            "Add new users",
            "Edit user roles",
            "Deactivate users"
        ]),
        ("Build audit logs page", "Query history viewer", "high", [
            "Searchable log table",
            "Date range filter",
            "User filter",
            "Export to CSV"
        ]),
        ("Build system config page", "Configuration interface", "medium", [
            "LLM model selection",
            "Confidence thresholds",
            "Timeout settings",
            "Prompt templates"
        ]),
        ("Configure Prometheus", "Metrics collection", "medium", [
            "Install Prometheus",
            "Configure scrape targets",
            "Define metrics"
        ]),
        ("Create Grafana dashboards", "Visualizations", "medium", [
            "GPU utilization panel",
            "Query latency panel",
            "Confidence score trends",
            "Error rate panel"
        ]),
        ("Set up alerting rules", "Proactive monitoring", "medium", [
            "Ollama down alert",
            "High GPU memory alert",
            "Low confidence spike alert",
            "Disk space alert"
        ]),
        ("Implement health check endpoints", "Service health APIs", "high", [
            "Add /health to all services",
            "Return detailed status",
            "Check dependencies"
        ]),
        ("Create backup automation", "Database backups", "medium", [
            "Schedule backups",
            "Rotate old backups",
            "Test restore"
        ]),
        ("Implement log rotation", "Prevent disk fill", "low", [
            "Configure rotation",
            "Set retention policy"
        ]),
        ("Create system restart UI", "Service management", "medium", [
            "Restart individual services",
            "View container logs",
            "Stop/Start controls"
        ]),
        ("Build performance reports", "Analytics dashboard", "low", [
            "Query count trends",
            "Response time analysis",
            "Popular queries"
        ]),
        ("Create admin activity log", "Track admin actions", "medium", [
            "Log all admin operations",
            "Who did what when",
            "Exportable"
        ])
    ]

    for name, desc, priority, subtasks in tasks_phase_6:
        task_id = tracker.create_task(phase_6, name, desc, priority)
        for subtask in subtasks:
            tracker.create_subtask(task_id, subtask)

    print(f"Phase 6 created with {len(tasks_phase_6)} tasks")

    # ==================== PHASE 7: Documentation & Validation ====================
    phase_7 = tracker.create_phase(
        name="Phase 7: Documentation & Validation",
        description="Create audit-ready documentation and validation records",
        order_index=7,
        estimated_hours=40
    )

    tasks_phase_7 = [
        ("Set up MkDocs", "Documentation framework", "high", [
            "Install MkDocs",
            "Configure Material theme",
            "Set up search plugin",
            "Configure navigation"
        ]),
        ("Write installation guide", "Setup documentation", "high", [
            "Prerequisites",
            "Step-by-step install",
            "Troubleshooting"
        ]),
        ("Write configuration guide", "Config documentation", "medium", [
            "Environment variables",
            "Configuration files",
            "Default values"
        ]),
        ("Write user guide - chat interface", "End user docs", "high", [
            "How to login",
            "How to ask questions",
            "Example queries"
        ]),
        ("Write user guide - understanding results", "Results documentation", "high", [
            "Confidence scores explained",
            "Methodology section",
            "When to verify"
        ]),
        ("Write admin guide - user management", "Admin docs", "medium", [
            "Adding users",
            "Role permissions",
            "Password resets"
        ]),
        ("Write admin guide - metadata management", "Metadata docs", "high", [
            "Approval workflow",
            "Editing metadata",
            "Version control"
        ]),
        ("Write admin guide - data loading", "Data management docs", "high", [
            "Uploading SAS files",
            "Processing steps",
            "Validation reports"
        ]),
        ("Write technical architecture docs", "System design", "medium", [
            "Four Factories model",
            "Component diagram",
            "Technology stack"
        ]),
        ("Write data flow documentation", "Pipeline docs", "medium", [
            "Query processing flow",
            "Each step explained",
            "Error handling"
        ]),
        ("Create validation summary", "IQ/OQ/PQ records", "critical", [
            "Installation tests",
            "Operational tests",
            "Performance tests",
            "Document results"
        ]),
        ("Document audit trail", "Compliance documentation", "critical", [
            "What is logged",
            "Log retention",
            "Access controls"
        ]),
        ("Create access control matrix", "Security documentation", "high", [
            "Role definitions",
            "Permission matrix",
            "Access procedures"
        ]),
        ("Write API reference", "API documentation", "medium", [
            "Endpoint list",
            "Request/response formats",
            "Authentication"
        ])
    ]

    for name, desc, priority, subtasks in tasks_phase_7:
        task_id = tracker.create_task(phase_7, name, desc, priority)
        for subtask in subtasks:
            tracker.create_subtask(task_id, subtask)

    print(f"Phase 7 created with {len(tasks_phase_7)} tasks")

    # ==================== PHASE 8: UAT & Production Deployment ====================
    phase_8 = tracker.create_phase(
        name="Phase 8: UAT & Production Deployment",
        description="User acceptance testing and go-live",
        order_index=8,
        estimated_hours=40
    )

    tasks_phase_8 = [
        ("Recruit pilot users", "Identify test users", "high", [
            "Identify 5-10 users",
            "Get management approval",
            "Schedule availability"
        ]),
        ("Conduct training sessions", "Train pilot users", "high", [
            "Prepare training materials",
            "Conduct sessions",
            "Answer questions"
        ]),
        ("Collect UAT feedback", "Gather user input", "critical", [
            "Create feedback form",
            "Conduct testing sessions",
            "Document issues found"
        ]),
        ("Fix critical bugs", "Address UAT findings", "critical", [
            "Prioritize issues",
            "Fix critical bugs",
            "Retest fixes"
        ]),
        ("Perform security review", "Security assessment", "critical", [
            "Review access controls",
            "Check for vulnerabilities",
            "Address findings"
        ]),
        ("Conduct load testing", "Performance testing", "high", [
            "Simulate concurrent users",
            "Measure response times",
            "Identify bottlenecks"
        ]),
        ("Create backup procedures", "Disaster recovery", "high", [
            "Document backup process",
            "Test restore procedure",
            "Schedule regular backups"
        ]),
        ("Deploy to production server", "Production deployment", "critical", [
            "Prepare production server",
            "Deploy application",
            "Verify deployment"
        ]),
        ("Configure SSL (if needed)", "HTTPS setup", "medium", [
            "Obtain certificates",
            "Configure reverse proxy",
            "Test HTTPS"
        ]),
        ("Go-live announcement", "Launch communication", "high", [
            "Prepare announcement",
            "Notify stakeholders",
            "Provide support contacts"
        ])
    ]

    for name, desc, priority, subtasks in tasks_phase_8:
        task_id = tracker.create_task(phase_8, name, desc, priority)
        for subtask in subtasks:
            tracker.create_subtask(task_id, subtask)

    print(f"Phase 8 created with {len(tasks_phase_8)} tasks")

    # ==================== CREATE MILESTONES ====================
    milestones = [
        ("Project Tracker Live", "Admin UI with project tracking available", phase_0, None),
        ("Infrastructure Ready", "All Docker services running", phase_1, None),
        ("Data Pipeline Complete", "SAS to DuckDB conversion working", phase_2, None),
        ("Metadata Approved", "Golden Metadata v1 complete", phase_3, None),
        ("Fuzzy Matching Live", "Dictionary Factory operational", phase_4, None),
        ("Chat Interface Launch", "End users can query data", phase_5, None),
        ("Admin Panel Complete", "Full admin capabilities available", phase_6, None),
        ("Documentation Ready", "Audit-ready docs published", phase_7, None),
        ("Production Go-Live", "Platform in production use", phase_8, None)
    ]

    for name, desc, phase_id, target_date in milestones:
        tracker.create_milestone(name, desc, phase_id, target_date)

    print(f"Created {len(milestones)} milestones")

    # Recalculate all progress
    tracker.recalculate_all_progress()

    # Print summary
    progress = tracker.get_overall_progress()
    print("\n" + "=" * 50)
    print("INITIALIZATION COMPLETE")
    print("=" * 50)
    print(f"Total Phases: {progress['phases']['total_phases']}")
    print(f"Total Tasks: {progress['tasks']['total_tasks']}")
    print(f"Total Subtasks: {progress['subtasks']['total_subtasks']}")
    print(f"Overall Progress: {progress['overall_progress']}%")
    print("=" * 50)


if __name__ == "__main__":
    reset = "--reset" in sys.argv
    init_tracker(reset=reset)
