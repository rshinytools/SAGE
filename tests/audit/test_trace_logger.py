"""Tests for Audit Trace Logger."""

import pytest
import tempfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.engine.audit.trace_logger import (
    AuditTraceLogger,
    AuditEvent,
    AuditLevel
)


class TestAuditTraceLogger:
    """Test suite for AuditTraceLogger."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        temp = tempfile.mkdtemp()
        yield temp
        shutil.rmtree(temp, ignore_errors=True)

    @pytest.fixture
    def logger(self, temp_dir):
        """Create AuditTraceLogger with temp path."""
        db_path = Path(temp_dir) / "audit.db"
        return AuditTraceLogger(db_path=str(db_path))

    def test_init_creates_database(self, logger, temp_dir):
        """Test that initialization creates the database."""
        db_path = Path(temp_dir) / "audit.db"
        assert db_path.exists()

    def test_start_trace(self, logger):
        """Test starting a new trace."""
        trace_id = logger.start_trace(
            question="How many patients?",
            user_id="test_user",
            session_id="session_123"
        )

        assert trace_id is not None
        assert len(trace_id) == 36  # UUID format

    def test_log_entry(self, logger):
        """Test logging an entry."""
        trace_id = logger.start_trace("Test question")

        entry_id = logger.log(
            trace_id=trace_id,
            event=AuditEvent.SQL_GENERATED,
            level=AuditLevel.INFO,
            component="sql_generator",
            message="SQL query generated",
            details={"sql": "SELECT COUNT(*) FROM adsl"}
        )

        assert entry_id is not None
        assert len(entry_id) == 36

    def test_complete_trace(self, logger):
        """Test completing a trace."""
        trace_id = logger.start_trace("Test question")

        logger.log(
            trace_id=trace_id,
            event=AuditEvent.SQL_GENERATED,
            level=AuditLevel.INFO,
            component="test",
            message="Test log"
        )

        logger.complete_trace(
            trace_id=trace_id,
            sql="SELECT COUNT(*) FROM adsl",
            confidence=85.5,
            action="RETURN_NORMAL",
            success=True
        )

        trace = logger.get_trace(trace_id)
        assert trace is not None
        assert trace["success"] == 1
        assert trace["final_confidence"] == 85.5

    def test_get_trace(self, logger):
        """Test retrieving a complete trace."""
        trace_id = logger.start_trace(
            question="Count patients",
            user_id="user_1"
        )

        logger.log(
            trace_id=trace_id,
            event=AuditEvent.ENTITY_EXTRACTED,
            level=AuditLevel.INFO,
            component="extractor",
            message="Entities extracted",
            details={"entities": ["patients"]}
        )

        logger.log(
            trace_id=trace_id,
            event=AuditEvent.SQL_GENERATED,
            level=AuditLevel.INFO,
            component="generator",
            message="SQL generated"
        )

        logger.complete_trace(trace_id, success=True)

        trace = logger.get_trace(trace_id)

        assert trace is not None
        assert trace["question"] == "Count patients"
        assert trace["user_id"] == "user_1"
        assert len(trace["entries"]) >= 3  # start + 2 logs + complete

    def test_get_entries_by_event(self, logger):
        """Test filtering entries by event."""
        trace_id = logger.start_trace("Test")

        logger.log(
            trace_id=trace_id,
            event=AuditEvent.SQL_GENERATED,
            level=AuditLevel.INFO,
            component="test",
            message="SQL generated"
        )
        logger.log(
            trace_id=trace_id,
            event=AuditEvent.EXECUTION_ERROR,
            level=AuditLevel.ERROR,
            component="test",
            message="Execution failed"
        )

        sql_entries = logger.get_entries(event=AuditEvent.SQL_GENERATED)
        error_entries = logger.get_entries(event=AuditEvent.EXECUTION_ERROR)

        assert len(sql_entries) >= 1
        assert len(error_entries) >= 1
        assert all(e["event"] == "SQL_GENERATED" for e in sql_entries)

    def test_get_entries_by_level(self, logger):
        """Test filtering entries by level."""
        trace_id = logger.start_trace("Test")

        logger.log(
            trace_id=trace_id,
            event=AuditEvent.VALIDATION_ERROR,
            level=AuditLevel.ERROR,
            component="test",
            message="Error occurred"
        )
        logger.log(
            trace_id=trace_id,
            event=AuditEvent.SQL_GENERATED,
            level=AuditLevel.INFO,
            component="test",
            message="Info message"
        )

        error_entries = logger.get_entries(level=AuditLevel.ERROR)
        assert len(error_entries) >= 1
        assert all(e["level"] == "ERROR" for e in error_entries)

    def test_get_entries_by_user(self, logger):
        """Test filtering entries by user."""
        trace_id = logger.start_trace(
            question="Test",
            user_id="specific_user"
        )

        logger.log(
            trace_id=trace_id,
            event=AuditEvent.SQL_GENERATED,
            level=AuditLevel.INFO,
            component="test",
            message="Test",
            user_id="specific_user"
        )

        user_entries = logger.get_entries(user_id="specific_user")
        assert len(user_entries) >= 1

    def test_get_recent_traces(self, logger):
        """Test getting recent traces."""
        for i in range(5):
            trace_id = logger.start_trace(f"Question {i}")
            logger.complete_trace(trace_id, success=True)

        traces = logger.get_recent_traces(limit=3)
        assert len(traces) == 3

    def test_get_recent_traces_by_user(self, logger):
        """Test getting recent traces by user."""
        logger.start_trace("Q1", user_id="user_a")
        logger.start_trace("Q2", user_id="user_b")
        logger.start_trace("Q3", user_id="user_a")

        user_a_traces = logger.get_recent_traces(user_id="user_a")
        assert len(user_a_traces) == 2

    def test_get_statistics(self, logger):
        """Test getting statistics."""
        for i in range(5):
            trace_id = logger.start_trace(f"Question {i}", user_id=f"user_{i % 2}")
            logger.complete_trace(
                trace_id,
                confidence=80 + i * 2,
                action="RETURN_NORMAL",
                success=i < 4  # 4 successes, 1 failure
            )

        stats = logger.get_statistics()

        assert stats["total_queries"] == 5
        assert stats["successful_queries"] == 4
        assert stats["success_rate"] == 80.0
        assert stats["average_confidence"] is not None
        assert stats["unique_users"] == 2

    def test_verify_integrity_valid(self, logger):
        """Test integrity verification for valid trace."""
        trace_id = logger.start_trace("Test question")

        logger.log(
            trace_id=trace_id,
            event=AuditEvent.SQL_GENERATED,
            level=AuditLevel.INFO,
            component="test",
            message="Generated SQL"
        )

        verification = logger.verify_integrity(trace_id)

        assert verification["valid"]
        assert verification["entries_checked"] >= 1
        assert len(verification["issues"]) == 0

    def test_export_trace(self, logger):
        """Test exporting a trace."""
        trace_id = logger.start_trace("Export test", user_id="test_user")

        logger.log(
            trace_id=trace_id,
            event=AuditEvent.SQL_GENERATED,
            level=AuditLevel.INFO,
            component="test",
            message="SQL generated"
        )

        logger.complete_trace(trace_id, success=True)

        export = logger.export_trace(trace_id)

        assert "export_timestamp" in export
        assert "trace" in export
        assert "integrity_verification" in export
        assert export["integrity_verification"]["valid"]

    def test_export_trace_not_found(self, logger):
        """Test exporting non-existent trace."""
        export = logger.export_trace("non-existent")
        assert "error" in export

    def test_checksum_generation(self, logger):
        """Test that checksums are generated for entries."""
        trace_id = logger.start_trace("Test")

        logger.log(
            trace_id=trace_id,
            event=AuditEvent.SQL_GENERATED,
            level=AuditLevel.INFO,
            component="test",
            message="Test message"
        )

        entries = logger.get_entries(trace_id=trace_id)
        assert all(e["checksum"] is not None for e in entries)
        assert all(len(e["checksum"]) == 16 for e in entries)

    def test_details_stored_as_json(self, logger):
        """Test that details are stored as JSON."""
        trace_id = logger.start_trace("Test")

        logger.log(
            trace_id=trace_id,
            event=AuditEvent.ENTITY_EXTRACTED,
            level=AuditLevel.INFO,
            component="test",
            message="Test",
            details={"entities": ["patient", "count"], "confidence": 0.95}
        )

        entries = logger.get_entries(trace_id=trace_id, event=AuditEvent.ENTITY_EXTRACTED)
        assert len(entries) > 0

        import json
        details = json.loads(entries[0]["details"])
        assert details["entities"] == ["patient", "count"]
        assert details["confidence"] == 0.95

    def test_execution_time_calculated(self, logger):
        """Test that execution time is calculated."""
        trace_id = logger.start_trace("Test")

        # Small delay to ensure measurable time
        import time
        time.sleep(0.01)

        logger.complete_trace(trace_id, success=True)

        trace = logger.get_trace(trace_id)
        assert trace["execution_time_ms"] is not None
        assert trace["execution_time_ms"] >= 0

    def test_clear_all(self, logger):
        """Test clearing all audit data."""
        trace_id = logger.start_trace("Test")
        logger.log(
            trace_id=trace_id,
            event=AuditEvent.SQL_GENERATED,
            level=AuditLevel.INFO,
            component="test",
            message="Test"
        )
        logger.complete_trace(trace_id, success=True)

        stats = logger.get_statistics()
        assert stats["total_queries"] >= 1

        logger.clear_all()

        stats = logger.get_statistics()
        assert stats["total_queries"] == 0

    def test_multiple_events_in_trace(self, logger):
        """Test logging multiple events in a single trace."""
        trace_id = logger.start_trace("Complex query")

        events = [
            (AuditEvent.QUERY_SANITIZED, "Sanitized"),
            (AuditEvent.ENTITY_EXTRACTED, "Extracted"),
            (AuditEvent.DICTIONARY_MATCHED, "Matched"),
            (AuditEvent.SQL_GENERATED, "Generated"),
            (AuditEvent.SQL_VALIDATED, "Validated"),
            (AuditEvent.QUERY_EXECUTED, "Executed"),
            (AuditEvent.CONFIDENCE_CALCULATED, "Calculated")
        ]

        for event, msg in events:
            logger.log(
                trace_id=trace_id,
                event=event,
                level=AuditLevel.INFO,
                component="test",
                message=msg
            )

        logger.complete_trace(trace_id, success=True)

        trace = logger.get_trace(trace_id)
        # start + events + complete
        assert len(trace["entries"]) >= len(events) + 2

    def test_action_statistics(self, logger):
        """Test action breakdown in statistics."""
        actions = ["RETURN_NORMAL", "RETURN_WITH_WARNING", "REFUSE"]

        for action in actions:
            trace_id = logger.start_trace(f"Query for {action}")
            logger.complete_trace(trace_id, action=action, success=True)

        stats = logger.get_statistics()
        assert "actions" in stats
        assert stats["actions"].get("RETURN_NORMAL", 0) >= 1

    def test_session_tracking(self, logger):
        """Test session ID tracking."""
        trace_id = logger.start_trace(
            question="Test",
            user_id="user_1",
            session_id="session_abc"
        )

        trace = logger.get_trace(trace_id)
        assert trace["session_id"] == "session_abc"
