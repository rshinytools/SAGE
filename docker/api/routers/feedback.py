"""
SAGE Feedback Router
====================
Handles user feedback for the AI learning system.
Allows users to confirm, correct, or reject query responses.
"""

import os
import sys
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

# Import auth dependency
from routers.auth import get_current_user

# Add project root to path for engine imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Import learning components
try:
    from core.engine.learning import (
        FeedbackHandler,
        FeedbackType,
        FeedbackResult,
        ExampleStore
    )
    LEARNING_AVAILABLE = True
except ImportError as e:
    LEARNING_AVAILABLE = False
    logging.warning(f"Learning module not available: {e}")

logger = logging.getLogger(__name__)

router = APIRouter()

# Global instances (lazy initialization)
_feedback_handler: Optional[FeedbackHandler] = None
_example_store: Optional[ExampleStore] = None


def get_feedback_handler() -> FeedbackHandler:
    """Get or create the feedback handler instance."""
    global _feedback_handler
    if _feedback_handler is None:
        if not LEARNING_AVAILABLE:
            raise HTTPException(
                status_code=503,
                detail="Learning system not available"
            )
        db_path = os.getenv("LEARNING_DB_PATH", "data/learning.db")
        _feedback_handler = FeedbackHandler(db_path=db_path)
        logger.info(f"Feedback handler initialized with db: {db_path}")
    return _feedback_handler


def get_example_store() -> ExampleStore:
    """Get or create the example store instance."""
    global _example_store
    if _example_store is None:
        if not LEARNING_AVAILABLE:
            raise HTTPException(
                status_code=503,
                detail="Learning system not available"
            )
        db_path = os.getenv("LEARNING_DB_PATH", "data/learning.db")
        _example_store = ExampleStore(db_path=db_path)
        logger.info(f"Example store initialized with db: {db_path}")
    return _example_store


# ============================================
# Models
# ============================================

class FeedbackSubmission(BaseModel):
    """Request model for submitting feedback."""
    query_id: str = Field(..., description="ID of the query being rated")
    question: str = Field(..., description="Original user question")
    generated_sql: str = Field(..., description="SQL that was generated")
    feedback_type: str = Field(..., description="Type: CONFIRM, CORRECT, REJECT, REPORT")
    corrected_sql: Optional[str] = Field(None, description="Corrected SQL if type is CORRECT")
    correction_notes: Optional[str] = Field(None, description="Notes about the correction")
    issue_description: Optional[str] = Field(None, description="Description of the issue if REJECT/REPORT")
    rating: Optional[int] = Field(None, ge=1, le=5, description="Optional 1-5 rating")


class FeedbackResponse(BaseModel):
    """Response model for feedback submission."""
    success: bool
    message: str
    feedback_id: Optional[str] = None
    example_created: bool = False
    example_id: Optional[str] = None


class FeedbackStats(BaseModel):
    """Statistics about feedback."""
    total_feedback: int
    confirmations: int
    corrections: int
    rejections: int
    reports: int
    pending_reviews: int
    examples_created: int
    average_rating: Optional[float] = None


class PendingReview(BaseModel):
    """A feedback item pending review."""
    id: str
    query_id: str
    question: str
    generated_sql: str
    feedback_type: str
    corrected_sql: Optional[str] = None
    correction_notes: Optional[str] = None
    issue_description: Optional[str] = None
    submitted_by: str
    submitted_at: str
    status: str


class ExampleSummary(BaseModel):
    """Summary of a learning example."""
    id: str
    question: str
    sql: str
    category: str
    confidence: float
    verified: bool
    usage_count: int
    success_rate: float
    created_at: str


# ============================================
# Endpoints
# ============================================

@router.post("/submit", response_model=FeedbackResponse)
async def submit_feedback(
    feedback: FeedbackSubmission,
    current_user: dict = Depends(get_current_user)
):
    """
    Submit feedback for a query response.

    Feedback types:
    - CONFIRM: The response was correct
    - CORRECT: The response was wrong, here's the correct SQL
    - REJECT: The response was wrong, no correction provided
    - REPORT: Report an issue with the response
    """
    handler = get_feedback_handler()

    # Map string to enum
    try:
        fb_type = FeedbackType[feedback.feedback_type.upper()]
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid feedback type: {feedback.feedback_type}. "
                   f"Valid types: CONFIRM, CORRECT, REJECT, REPORT"
        )

    try:
        result = handler.submit_feedback(
            query_id=feedback.query_id,
            question=feedback.question,
            original_sql=feedback.generated_sql,
            feedback_type=fb_type,
            corrected_sql=feedback.corrected_sql,
            correction_notes=feedback.correction_notes,
            user_id=current_user.get("username", "anonymous")
        )

        logger.info(
            f"Feedback submitted: type={feedback.feedback_type}, "
            f"query_id={feedback.query_id}, user={current_user.get('username')}"
        )

        return FeedbackResponse(
            success=result.success,
            message=result.message if hasattr(result, 'message') else "Feedback recorded",
            feedback_id=result.feedback_id if hasattr(result, 'feedback_id') else None,
            example_created=result.example_created if hasattr(result, 'example_created') else False,
            example_id=result.example_id if hasattr(result, 'example_id') else None
        )

    except Exception as e:
        logger.error(f"Failed to submit feedback: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats", response_model=FeedbackStats)
async def get_feedback_stats(
    current_user: dict = Depends(get_current_user)
):
    """Get statistics about feedback submissions."""
    handler = get_feedback_handler()

    try:
        stats = handler.get_statistics()

        return FeedbackStats(
            total_feedback=stats.get("total_feedback", 0),
            confirmations=stats.get("confirmations", 0),
            corrections=stats.get("corrections", 0),
            rejections=stats.get("rejections", 0),
            reports=stats.get("reports", 0),
            pending_reviews=stats.get("pending_reviews", 0),
            examples_created=stats.get("examples_created", 0),
            average_rating=stats.get("average_rating")
        )

    except Exception as e:
        logger.error(f"Failed to get feedback stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pending", response_model=List[PendingReview])
async def get_pending_reviews(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    current_user: dict = Depends(get_current_user)
):
    """
    Get feedback items pending review.

    Requires user_admin or admin permissions.
    """
    # Check permissions
    permissions = current_user.get("permissions", [])
    if "*" not in permissions and "user_admin" not in permissions:
        raise HTTPException(
            status_code=403,
            detail="Requires admin or user_admin permissions"
        )

    handler = get_feedback_handler()

    try:
        pending = handler.get_pending_reviews(limit=limit)

        return [
            PendingReview(
                id=item.get("id", ""),
                query_id=item.get("query_id", ""),
                question=item.get("question", ""),
                generated_sql=item.get("original_sql", ""),
                feedback_type=item.get("feedback_type", ""),
                corrected_sql=item.get("corrected_sql"),
                correction_notes=item.get("correction_notes"),
                issue_description=item.get("issue_description"),
                submitted_by=item.get("user_id", "anonymous"),
                submitted_at=item.get("created_at", ""),
                status=item.get("status", "pending")
            )
            for item in pending[offset:offset+limit]
        ]

    except Exception as e:
        logger.error(f"Failed to get pending reviews: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/pending/{feedback_id}/approve")
async def approve_feedback(
    feedback_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Approve a pending feedback item.

    Creates a verified learning example from the feedback.
    Requires user_admin or admin permissions.
    """
    # Check permissions
    permissions = current_user.get("permissions", [])
    if "*" not in permissions and "user_admin" not in permissions:
        raise HTTPException(
            status_code=403,
            detail="Requires admin or user_admin permissions"
        )

    handler = get_feedback_handler()

    try:
        result = handler.approve_feedback(
            feedback_id=feedback_id,
            reviewer_id=current_user.get("username", "admin")
        )

        if result:
            logger.info(f"Feedback approved: {feedback_id} by {current_user.get('username')}")
            return {"success": True, "message": "Feedback approved and example created"}
        else:
            raise HTTPException(status_code=404, detail="Feedback not found")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to approve feedback: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/pending/{feedback_id}/reject")
async def reject_feedback(
    feedback_id: str,
    reason: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    Reject a pending feedback item.

    Requires user_admin or admin permissions.
    """
    # Check permissions
    permissions = current_user.get("permissions", [])
    if "*" not in permissions and "user_admin" not in permissions:
        raise HTTPException(
            status_code=403,
            detail="Requires admin or user_admin permissions"
        )

    handler = get_feedback_handler()

    try:
        result = handler.reject_feedback(
            feedback_id=feedback_id,
            reviewer_id=current_user.get("username", "admin"),
            reason=reason
        )

        if result:
            logger.info(f"Feedback rejected: {feedback_id} by {current_user.get('username')}")
            return {"success": True, "message": "Feedback rejected"}
        else:
            raise HTTPException(status_code=404, detail="Feedback not found")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to reject feedback: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/examples", response_model=List[ExampleSummary])
async def get_learning_examples(
    verified_only: bool = Query(False),
    category: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: dict = Depends(get_current_user)
):
    """
    Get learning examples from the example store.

    Requires user_admin or admin permissions.
    """
    # Check permissions
    permissions = current_user.get("permissions", [])
    if "*" not in permissions and "user_admin" not in permissions:
        raise HTTPException(
            status_code=403,
            detail="Requires admin or user_admin permissions"
        )

    store = get_example_store()

    try:
        examples = store.get_examples(
            verified_only=verified_only,
            category=category,
            limit=limit,
            offset=offset
        )

        return [
            ExampleSummary(
                id=ex.get("id", ""),
                question=ex.get("question", ""),
                sql=ex.get("sql", ""),
                category=ex.get("category", "unknown"),
                confidence=ex.get("confidence", 0.0),
                verified=ex.get("verified", False),
                usage_count=ex.get("usage_count", 0),
                success_rate=ex.get("success_rate", 0.0),
                created_at=ex.get("created_at", "")
            )
            for ex in examples
        ]

    except Exception as e:
        logger.error(f"Failed to get learning examples: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/examples/{example_id}/verify")
async def verify_example(
    example_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Mark a learning example as verified.

    Verified examples are used with higher confidence in future queries.
    Requires user_admin or admin permissions.
    """
    # Check permissions
    permissions = current_user.get("permissions", [])
    if "*" not in permissions and "user_admin" not in permissions:
        raise HTTPException(
            status_code=403,
            detail="Requires admin or user_admin permissions"
        )

    store = get_example_store()

    try:
        result = store.verify_example(example_id)

        if result:
            logger.info(f"Example verified: {example_id} by {current_user.get('username')}")
            return {"success": True, "message": "Example verified"}
        else:
            raise HTTPException(status_code=404, detail="Example not found")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to verify example: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/examples/{example_id}")
async def delete_example(
    example_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Delete a learning example.

    Requires user_admin or admin permissions.
    """
    # Check permissions
    permissions = current_user.get("permissions", [])
    if "*" not in permissions and "user_admin" not in permissions:
        raise HTTPException(
            status_code=403,
            detail="Requires admin or user_admin permissions"
        )

    store = get_example_store()

    try:
        result = store.delete_example(example_id)

        if result:
            logger.info(f"Example deleted: {example_id} by {current_user.get('username')}")
            return {"success": True, "message": "Example deleted"}
        else:
            raise HTTPException(status_code=404, detail="Example not found")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete example: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/examples/stats")
async def get_example_stats(
    current_user: dict = Depends(get_current_user)
):
    """Get statistics about learning examples."""
    store = get_example_store()

    try:
        stats = store.get_statistics()
        return stats

    except Exception as e:
        logger.error(f"Failed to get example stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))
