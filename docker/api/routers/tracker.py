# SAGE API - Project Tracker Router
# ==================================
"""Project Tracker endpoints for phases, tasks, and progress."""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, Depends

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from .auth import get_current_user

router = APIRouter()

# Configuration - In Docker, tracker is mounted at /app/tracker
# Use environment variable or detect Docker context
if os.path.exists("/app/tracker"):
    TRACKER_DIR = Path("/app/tracker")
else:
    TRACKER_DIR = Path(os.getenv("TRACKER_DIR", project_root / "tracker"))
TRACKER_DB = TRACKER_DIR / "project_tracker.db"


def get_tracker_db():
    """Get tracker database connection."""
    try:
        from core.admin.tracker_db import TrackerDB
        if TRACKER_DB.exists():
            return TrackerDB(str(TRACKER_DB))
        return None
    except Exception:
        return None


# ============================================
# Overview Endpoints
# ============================================

@router.get("/summary")
async def get_tracker_summary(current_user: dict = Depends(get_current_user)):
    """
    Get overall project progress summary.

    Returns total progress percentage, phase counts, and task counts.
    """
    db = get_tracker_db()
    default_summary = {
        "total_progress": 0.0,
        "phases_total": 0,
        "phases_complete": 0,
        "tasks_total": 0,
        "tasks_complete": 0
    }

    if not db:
        return {
            "success": True,
            "data": default_summary,
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    try:
        # Try to get summary - fall back to calculating it
        if hasattr(db, 'get_summary'):
            summary = db.get_summary()
        else:
            # Calculate summary from phases and tasks
            phases = db.get_all_phases() if hasattr(db, 'get_all_phases') else []
            tasks = db.get_all_tasks() if hasattr(db, 'get_all_tasks') else []

            phases_complete = len([p for p in phases if p.get('status') == 'completed'])
            tasks_complete = len([t for t in tasks if t.get('status') == 'completed'])
            total_progress = (tasks_complete / len(tasks) * 100) if tasks else 0.0

            summary = {
                "total_progress": round(total_progress, 1),
                "phases_total": len(phases),
                "phases_complete": phases_complete,
                "tasks_total": len(tasks),
                "tasks_complete": tasks_complete
            }

        return {
            "success": True,
            "data": summary,
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except Exception as e:
        return {
            "success": True,
            "data": default_summary,
            "meta": {"timestamp": datetime.now().isoformat(), "error": str(e)}
        }


@router.get("/next-steps")
async def get_next_steps(
    limit: int = Query(default=5, ge=1, le=20),
    current_user: dict = Depends(get_current_user)
):
    """
    Get recommended next tasks to work on.

    Returns unblocked tasks based on dependencies and priority.
    """
    db = get_tracker_db()
    if not db:
        return {
            "success": True,
            "data": [],
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    try:
        next_tasks = db.get_next_steps(limit=limit)
        return {
            "success": True,
            "data": next_tasks,
            "meta": {"timestamp": datetime.now().isoformat(), "count": len(next_tasks)}
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


# ============================================
# Phase Endpoints
# ============================================

@router.get("/phases")
async def list_phases(current_user: dict = Depends(get_current_user)):
    """
    List all project phases with progress.
    """
    db = get_tracker_db()
    if not db:
        return {
            "success": True,
            "data": [],
            "meta": {"timestamp": datetime.now().isoformat(), "count": 0}
        }

    try:
        phases = db.get_all_phases()
        return {
            "success": True,
            "data": phases,
            "meta": {"timestamp": datetime.now().isoformat(), "count": len(phases)}
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


@router.get("/phases/{phase_id}")
async def get_phase(
    phase_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    Get phase details with all tasks.
    """
    db = get_tracker_db()
    if not db:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Tracker database not found"}
        )

    try:
        phase = db.get_phase(phase_id)
        if not phase:
            raise HTTPException(
                status_code=404,
                detail={"code": "NOT_FOUND", "message": f"Phase not found: {phase_id}"}
            )

        tasks = db.get_tasks_for_phase(phase_id)
        phase["tasks"] = tasks

        return {
            "success": True,
            "data": phase,
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


@router.put("/phases/{phase_id}")
async def update_phase(
    phase_id: int,
    status: Optional[str] = None,
    notes: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    Update phase status or notes.
    """
    db = get_tracker_db()
    if not db:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Tracker database not found"}
        )

    try:
        phase = db.get_phase(phase_id)
        if not phase:
            raise HTTPException(
                status_code=404,
                detail={"code": "NOT_FOUND", "message": f"Phase not found: {phase_id}"}
            )

        updates = {}
        if status:
            updates["status"] = status
        if notes:
            updates["description"] = notes

        if updates:
            db.update_phase(phase_id, **updates)

            # Log activity
            db.log_activity(
                phase_id=phase_id,
                action="phase_updated",
                details=f"Updated: {', '.join(updates.keys())}",
                user=current_user.get("sub", "unknown")
            )

        updated_phase = db.get_phase(phase_id)

        return {
            "success": True,
            "data": updated_phase,
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


# ============================================
# Task Endpoints
# ============================================

@router.get("/tasks")
async def list_tasks(
    phase_id: Optional[int] = None,
    status: Optional[str] = None,
    priority: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    List tasks with optional filters.
    """
    db = get_tracker_db()
    if not db:
        return {
            "success": True,
            "data": [],
            "meta": {"timestamp": datetime.now().isoformat(), "count": 0}
        }

    try:
        if phase_id:
            tasks = db.get_tasks_for_phase(phase_id)
        else:
            tasks = db.get_all_tasks()

        # Apply filters
        if status:
            tasks = [t for t in tasks if t.get("status") == status]
        if priority:
            tasks = [t for t in tasks if t.get("priority") == priority]

        return {
            "success": True,
            "data": tasks,
            "meta": {"timestamp": datetime.now().isoformat(), "count": len(tasks)}
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


@router.get("/tasks/{task_id}")
async def get_task(
    task_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    Get task details with subtasks.
    """
    db = get_tracker_db()
    if not db:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Tracker database not found"}
        )

    try:
        task = db.get_task(task_id)
        if not task:
            raise HTTPException(
                status_code=404,
                detail={"code": "NOT_FOUND", "message": f"Task not found: {task_id}"}
            )

        subtasks = db.get_subtasks_for_task(task_id)
        task["subtasks"] = subtasks

        return {
            "success": True,
            "data": task,
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


@router.put("/tasks/{task_id}")
async def update_task(
    task_id: int,
    status: Optional[str] = None,
    priority: Optional[str] = None,
    assignee: Optional[str] = None,
    notes: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    Update task properties.
    """
    db = get_tracker_db()
    if not db:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Tracker database not found"}
        )

    try:
        task = db.get_task(task_id)
        if not task:
            raise HTTPException(
                status_code=404,
                detail={"code": "NOT_FOUND", "message": f"Task not found: {task_id}"}
            )

        updates = {}
        if status:
            updates["status"] = status
        if priority:
            updates["priority"] = priority
        if assignee:
            updates["assignee"] = assignee
        if notes:
            updates["notes"] = notes

        if updates:
            db.update_task(task_id, **updates)

            # Log activity
            db.log_activity(
                task_id=task_id,
                action="task_updated",
                details=f"Updated: {', '.join(updates.keys())}",
                user=current_user.get("sub", "unknown")
            )

        updated_task = db.get_task(task_id)

        return {
            "success": True,
            "data": updated_task,
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


@router.post("/tasks/{task_id}/complete")
async def complete_task(
    task_id: int,
    notes: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    Mark a task as completed.
    """
    db = get_tracker_db()
    if not db:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Tracker database not found"}
        )

    try:
        task = db.get_task(task_id)
        if not task:
            raise HTTPException(
                status_code=404,
                detail={"code": "NOT_FOUND", "message": f"Task not found: {task_id}"}
            )

        updates = {
            "status": "completed",
            "completed_at": datetime.now().isoformat()
        }
        if notes:
            updates["notes"] = notes

        db.update_task(task_id, **updates)

        # Log activity
        db.log_activity(
            task_id=task_id,
            action="task_completed",
            details=notes or "Task marked as completed",
            user=current_user.get("sub", "unknown")
        )

        # Update phase progress
        db.recalculate_phase_progress(task["phase_id"])

        return {
            "success": True,
            "data": {"message": f"Task {task_id} marked as completed"},
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


@router.post("/tasks/{task_id}/start")
async def start_task(
    task_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    Mark a task as in progress.
    """
    db = get_tracker_db()
    if not db:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Tracker database not found"}
        )

    try:
        task = db.get_task(task_id)
        if not task:
            raise HTTPException(
                status_code=404,
                detail={"code": "NOT_FOUND", "message": f"Task not found: {task_id}"}
            )

        db.update_task(task_id, status="in_progress", started_at=datetime.now().isoformat())

        # Log activity
        db.log_activity(
            task_id=task_id,
            action="task_started",
            details="Task work started",
            user=current_user.get("sub", "unknown")
        )

        return {
            "success": True,
            "data": {"message": f"Task {task_id} started"},
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


# ============================================
# Subtask Endpoints
# ============================================

@router.get("/tasks/{task_id}/subtasks")
async def list_subtasks(
    task_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    List subtasks for a task.
    """
    db = get_tracker_db()
    if not db:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Tracker database not found"}
        )

    try:
        subtasks = db.get_subtasks_for_task(task_id)
        return {
            "success": True,
            "data": subtasks,
            "meta": {"timestamp": datetime.now().isoformat(), "count": len(subtasks)}
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


@router.put("/subtasks/{subtask_id}")
async def update_subtask(
    subtask_id: int,
    status: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Update subtask status.
    """
    db = get_tracker_db()
    if not db:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Tracker database not found"}
        )

    try:
        subtask = db.get_subtask(subtask_id)
        if not subtask:
            raise HTTPException(
                status_code=404,
                detail={"code": "NOT_FOUND", "message": f"Subtask not found: {subtask_id}"}
            )

        completed_at = datetime.now().isoformat() if status == "completed" else None
        db.update_subtask(subtask_id, status=status, completed_at=completed_at)

        return {
            "success": True,
            "data": {"message": f"Subtask {subtask_id} updated"},
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


@router.post("/subtasks/{subtask_id}/complete")
async def complete_subtask(
    subtask_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    Mark a subtask as completed.
    """
    db = get_tracker_db()
    if not db:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Tracker database not found"}
        )

    try:
        subtask = db.get_subtask(subtask_id)
        if not subtask:
            raise HTTPException(
                status_code=404,
                detail={"code": "NOT_FOUND", "message": f"Subtask not found: {subtask_id}"}
            )

        db.update_subtask(
            subtask_id,
            status="completed",
            completed_at=datetime.now().isoformat()
        )

        return {
            "success": True,
            "data": {"message": f"Subtask {subtask_id} completed"},
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


# ============================================
# Activity Log Endpoints
# ============================================

@router.get("/activity")
async def get_activity_log(
    limit: int = Query(default=50, ge=1, le=500),
    task_id: Optional[int] = None,
    phase_id: Optional[int] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    Get activity log entries.
    """
    db = get_tracker_db()
    if not db:
        return {
            "success": True,
            "data": [],
            "meta": {"timestamp": datetime.now().isoformat(), "count": 0}
        }

    try:
        # Use the correct method name - TrackerDB uses get_recent_activity
        if phase_id and hasattr(db, 'get_activity_by_phase'):
            activities = db.get_activity_by_phase(phase_id, limit=limit)
        elif hasattr(db, 'get_recent_activity'):
            activities = db.get_recent_activity(limit=limit)
        else:
            activities = []

        return {
            "success": True,
            "data": activities,
            "meta": {"timestamp": datetime.now().isoformat(), "count": len(activities)}
        }
    except Exception as e:
        # Return empty list on error instead of 500
        return {
            "success": True,
            "data": [],
            "meta": {"timestamp": datetime.now().isoformat(), "count": 0, "error": str(e)}
        }


@router.post("/activity")
async def log_activity(
    action: str,
    task_id: Optional[int] = None,
    phase_id: Optional[int] = None,
    details: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    Log a custom activity entry.
    """
    db = get_tracker_db()
    if not db:
        return {
            "success": True,
            "data": {"message": "Activity logged (tracker not available)"},
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    try:
        if hasattr(db, 'log_activity'):
            db.log_activity(
                task_id=task_id,
                phase_id=phase_id,
                action=action,
                details=details,
                user=current_user.get("sub", "unknown")
            )

        return {
            "success": True,
            "data": {"message": "Activity logged"},
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except Exception as e:
        return {
            "success": True,
            "data": {"message": "Activity logging failed", "error": str(e)},
            "meta": {"timestamp": datetime.now().isoformat()}
        }
