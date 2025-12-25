"""
Tests for the Audit Logging System.

Tests the core audit module including:
- Database operations
- AuditService methods
- 21 CFR Part 11 compliance features
"""

import os
import sys
import tempfile
import pytest
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


class TestAuditDatabase:
    """Tests for the AuditDB database class."""

    def test_database_creation(self):
        """Test that the database is created with correct schema."""
        from core.audit import AuditDB

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_audit.db")
            db = AuditDB(db_path)

            # Check tables exist using the context manager properly
            with db._get_connection() as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
                tables = {row[0] for row in cursor.fetchall()}

            assert "audit_logs" in tables
            assert "query_audit_details" in tables
            assert "electronic_signatures" in tables

    def test_insert_audit_log(self):
        """Test inserting an audit log entry."""
        from core.audit import AuditDB, AuditEvent, AuditAction, AuditStatus

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_audit.db")
            db = AuditDB(db_path)

            event = AuditEvent(
                user_id="test_user",
                username="test_user",
                action=AuditAction.LOGIN,
                status=AuditStatus.SUCCESS,
                ip_address="127.0.0.1"
            )

            log_id = db.insert_log(event)
            assert log_id > 0

            # Retrieve and verify
            log = db.get_log(log_id)
            assert log is not None
            assert log.user_id == "test_user"
            assert log.action == AuditAction.LOGIN.value
            assert log.status == AuditStatus.SUCCESS.value

    def test_checksum_verification(self):
        """Test that checksums are correctly computed and verified."""
        from core.audit import AuditDB, AuditEvent, AuditAction, AuditStatus

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_audit.db")
            db = AuditDB(db_path)

            event = AuditEvent(
                user_id="test_user",
                username="test_user",
                action=AuditAction.QUERY,
                status=AuditStatus.SUCCESS,
                resource_type="chat"
            )

            log_id = db.insert_log(event)
            log = db.get_log(log_id)

            # Checksum should be non-empty
            assert log.checksum is not None
            assert len(log.checksum) == 64  # SHA-256 hex string

    def test_search_logs(self):
        """Test searching logs with filters."""
        from core.audit import AuditDB, AuditEvent, AuditAction, AuditStatus, AuditFilters

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_audit.db")
            db = AuditDB(db_path)

            # Insert multiple logs
            for i in range(5):
                event = AuditEvent(
                    user_id=f"user_{i % 2}",
                    username=f"user_{i % 2}",
                    action=AuditAction.LOGIN if i % 2 == 0 else AuditAction.QUERY,
                    status=AuditStatus.SUCCESS
                )
                db.insert_log(event)

            # Search by user - filters object contains page and page_size
            filters = AuditFilters(user_id="user_0", page=1, page_size=10)
            results, total = db.search_logs(filters)
            assert len(results) == 3  # 3 logs for user_0

            # Search by action
            filters = AuditFilters(action=AuditAction.LOGIN.value, page=1, page_size=10)
            results, total = db.search_logs(filters)
            assert len(results) == 3  # 3 LOGIN actions


class TestAuditService:
    """Tests for the AuditService class."""

    def test_log_login(self):
        """Test logging a login event."""
        from core.audit import AuditService

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_audit.db")
            service = AuditService(db_path=db_path)

            log_id = service.log_login(
                user_id="test_user",
                username="test_user",
                ip_address="127.0.0.1",
                success=True
            )

            assert log_id > 0

            # Verify in database
            log = service._db.get_log(log_id)
            assert log.action == "LOGIN"
            assert log.status == "success"

    def test_log_login_failure(self):
        """Test logging a failed login attempt."""
        from core.audit import AuditService

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_audit.db")
            service = AuditService(db_path=db_path)

            log_id = service.log_login(
                user_id="test_user",
                username="test_user",
                ip_address="127.0.0.1",
                success=False,
                failure_reason="Invalid password"
            )

            log = service._db.get_log(log_id)
            assert log.action == "LOGIN_FAILED"
            assert log.status == "failure"
            assert "Invalid password" in (log.error_message or "")

    def test_log_query(self):
        """Test logging a query event."""
        from core.audit import AuditService

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_audit.db")
            service = AuditService(db_path=db_path)

            log_id = service.log_query(
                user_id="test_user",
                username="test_user",
                question="How many patients are in the study?",
                success=True,
                duration_ms=150
            )

            log = service._db.get_log(log_id)
            assert log.action == "QUERY"
            assert log.status == "success"
            assert log.duration_ms == 150

    def test_log_data_upload(self):
        """Test logging a data upload event."""
        from core.audit import AuditService

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_audit.db")
            service = AuditService(db_path=db_path)

            log_id = service.log_data_upload(
                user_id="test_user",
                username="test_user",
                filename="dm.sas7bdat",
                file_size=1024000,  # Required parameter
                row_count=100,
                success=True
            )

            log = service._db.get_log(log_id)
            assert log.action == "DATA_UPLOAD"
            assert log.resource_type == "data_file"
            assert log.resource_id == "dm.sas7bdat"

    def test_verify_integrity(self):
        """Test integrity verification of audit logs."""
        from core.audit import AuditService

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_audit.db")
            service = AuditService(db_path=db_path)

            log_id = service.log_login(
                user_id="test_user",
                username="test_user",
                ip_address="127.0.0.1",
                success=True
            )

            result = service.verify_integrity(log_id)
            assert result is not None
            assert result.integrity_valid is True
            assert result.log_id == log_id

    def test_add_signature(self):
        """Test adding electronic signature to audit log."""
        from core.audit import AuditService

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_audit.db")
            service = AuditService(db_path=db_path)

            log_id = service.log_login(
                user_id="test_user",
                username="test_user",
                ip_address="127.0.0.1",
                success=True
            )

            # Use correct parameter names: audit_log_id, user_id, username, meaning
            signature_id = service.add_signature(
                audit_log_id=log_id,
                user_id="reviewer",
                username="reviewer",
                meaning="Reviewed"
            )

            assert signature_id > 0

            # Retrieve log and check signatures
            log = service._db.get_log(log_id, include_details=True)
            assert log.signatures is not None
            assert len(log.signatures) > 0
            assert log.signatures[0].signature_meaning == "Reviewed"

    def test_get_statistics(self):
        """Test getting audit statistics."""
        from core.audit import AuditService

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_audit.db")
            service = AuditService(db_path=db_path)

            # Create some log entries
            service.log_login("user1", "user1", "127.0.0.1", success=True)
            service.log_login("user2", "user2", "127.0.0.1", success=True)
            service.log_login("user3", "user3", "127.0.0.1", success=False, failure_reason="Wrong password")
            service.log_query("user1", "user1", "test query", success=True, duration_ms=100)

            stats = service.get_statistics()
            assert stats.total_events == 4
            assert stats.by_status.get("success", 0) == 3
            assert stats.by_status.get("failure", 0) == 1
            assert len(stats.by_user) >= 2


class TestQueryAuditDetails:
    """Tests for query audit details logging."""

    def test_log_query_details(self):
        """Test logging detailed query information."""
        from core.audit import AuditService, QueryAuditDetails

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_audit.db")
            service = AuditService(db_path=db_path)

            # First create a query log
            log_id = service.log_query(
                user_id="test_user",
                username="test_user",
                question="How many patients?",
                success=True,
                duration_ms=200
            )

            # Add detailed query info
            details = QueryAuditDetails(
                original_question="How many patients?",
                sanitized_question="How many patients?",
                intent_classification="DATA",
                generated_sql="SELECT COUNT(*) FROM DM",
                confidence_score=0.85,
                execution_time_ms=150,
                result_row_count=1
            )

            service.log_query_details(log_id, details)

            # Verify details were saved
            retrieved = service.get_query_details(log_id)
            assert retrieved is not None
            assert retrieved.original_question == "How many patients?"
            assert retrieved.generated_sql == "SELECT COUNT(*) FROM DM"
            assert retrieved.confidence_score == 0.85


class TestExportFunctions:
    """Tests for export functionality."""

    def test_export_to_csv(self):
        """Test exporting logs to CSV format."""
        from core.audit import AuditService, AuditFilters

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_audit.db")
            service = AuditService(db_path=db_path)

            # Create some logs
            for i in range(3):
                service.log_login(f"user_{i}", f"user_{i}", "127.0.0.1", success=True)

            # Export to CSV
            csv_path = os.path.join(tmpdir, "export.csv")
            filters = AuditFilters()
            result_path = service.export_to_csv(filters, csv_path)

            assert os.path.exists(result_path)
            with open(result_path, 'r') as f:
                content = f.read()
                assert "user_0" in content
                assert "LOGIN" in content


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
