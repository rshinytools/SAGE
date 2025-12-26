"""
Settings Router
===============
API endpoints for platform settings management.
"""

import sys
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from core.settings import get_settings_service, SettingsResponse, SettingCategory
from routers.auth import get_current_user, require_permission


router = APIRouter()


# ============================================
# Request/Response Models
# ============================================

class SettingUpdateRequest(BaseModel):
    """Request to update a setting."""
    value: Any


class SettingUpdateResponse(BaseModel):
    """Response after updating a setting."""
    success: bool
    message: str
    category: str
    key: str
    value: Any


class ResetResponse(BaseModel):
    """Response after resetting settings."""
    success: bool
    message: str
    category: Optional[str] = None


class ImportRequest(BaseModel):
    """Request to import settings."""
    settings: Dict[str, Dict[str, Any]]
    overwrite: bool = False


class ImportResponse(BaseModel):
    """Response after importing settings."""
    success: bool
    imported: int
    skipped: int
    errors: int


class AuditEntry(BaseModel):
    """Setting change audit entry."""
    setting_category: str
    setting_key: str
    old_value: Any
    new_value: Any
    changed_by: Optional[str]
    changed_at: str


# ============================================
# Endpoints
# ============================================

@router.get("", response_model=SettingsResponse)
async def get_all_settings(
    current_user: dict = Depends(require_permission("*"))
):
    """Get all settings grouped by category.

    Requires Full Admin (*) permission.
    """
    service = get_settings_service()
    return service.get_all_for_api()


@router.get("/categories")
async def list_categories(
    current_user: dict = Depends(require_permission("*"))
):
    """List all setting categories.

    Requires Full Admin (*) permission.
    """
    from core.settings.defaults import SETTING_CATEGORIES

    categories = []
    for cat_id, cat_info in SETTING_CATEGORIES.items():
        categories.append({
            "id": cat_id,
            "name": cat_info["name"],
            "description": cat_info["description"],
            "icon": cat_info["icon"],
            "order": cat_info["order"],
        })

    categories.sort(key=lambda c: c["order"])
    return {
        "success": True,
        "data": categories,
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.get("/{category}", response_model=SettingCategory)
async def get_category_settings(
    category: str,
    current_user: dict = Depends(require_permission("*"))
):
    """Get all settings in a specific category.

    Requires Full Admin (*) permission.
    """
    service = get_settings_service()
    result = service.get_category(category)

    if result is None:
        raise HTTPException(status_code=404, detail=f"Category not found: {category}")

    return result


@router.get("/{category}/{key}")
async def get_setting(
    category: str,
    key: str,
    current_user: dict = Depends(require_permission("*"))
):
    """Get a single setting value.

    Requires Full Admin (*) permission.
    """
    service = get_settings_service()
    value = service.get(category, key)

    if value is None:
        raise HTTPException(status_code=404, detail=f"Setting not found: {category}/{key}")

    return {
        "success": True,
        "data": {
            "category": category,
            "key": key,
            "value": value,
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.put("/{category}/{key}", response_model=SettingUpdateResponse)
async def update_setting(
    category: str,
    key: str,
    request: SettingUpdateRequest,
    current_user: dict = Depends(require_permission("*"))
):
    """Update a setting value.

    Requires Full Admin (*) permission.
    """
    service = get_settings_service()

    try:
        success = service.update_setting(
            category=category,
            key=key,
            value=request.value,
            updated_by=current_user.get("username", "unknown")
        )

        if not success:
            raise HTTPException(status_code=404, detail=f"Setting not found: {category}/{key}")

        return SettingUpdateResponse(
            success=True,
            message=f"Setting {category}/{key} updated successfully",
            category=category,
            key=key,
            value=request.value,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/reset", response_model=ResetResponse)
async def reset_all_settings(
    current_user: dict = Depends(require_permission("*"))
):
    """Reset all settings to default values.

    Requires Full Admin (*) permission.
    """
    service = get_settings_service()
    service.reset_all(updated_by=current_user.get("username", "unknown"))

    return ResetResponse(
        success=True,
        message="All settings have been reset to defaults",
    )


@router.post("/{category}/reset", response_model=ResetResponse)
async def reset_category_settings(
    category: str,
    current_user: dict = Depends(require_permission("*"))
):
    """Reset a specific category to default values.

    Requires Full Admin (*) permission.
    """
    service = get_settings_service()
    success = service.reset_category(
        category=category,
        updated_by=current_user.get("username", "unknown")
    )

    if not success:
        raise HTTPException(status_code=404, detail=f"Category not found: {category}")

    return ResetResponse(
        success=True,
        message=f"Category '{category}' has been reset to defaults",
        category=category,
    )


@router.get("/export/json")
async def export_settings(
    current_user: dict = Depends(require_permission("*"))
):
    """Export all settings as JSON.

    Sensitive values are excluded.
    Requires Full Admin (*) permission.
    """
    service = get_settings_service()
    export_data = service.export_settings(
        exported_by=current_user.get("username", "unknown")
    )

    return {
        "success": True,
        "data": export_data.model_dump(),
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/import", response_model=ImportResponse)
async def import_settings(
    request: ImportRequest,
    current_user: dict = Depends(require_permission("*"))
):
    """Import settings from JSON.

    Requires Full Admin (*) permission.
    """
    service = get_settings_service()
    result = service.import_settings(
        settings=request.settings,
        imported_by=current_user.get("username", "unknown"),
        overwrite=request.overwrite,
    )

    return ImportResponse(
        success=result["errors"] == 0,
        imported=result["imported"],
        skipped=result["skipped"],
        errors=result["errors"],
    )


@router.get("/audit/history")
async def get_audit_history(
    category: Optional[str] = None,
    key: Optional[str] = None,
    limit: int = 100,
    current_user: dict = Depends(require_permission("*"))
):
    """Get setting change history.

    Requires Full Admin (*) permission.
    """
    service = get_settings_service()
    history = service.get_audit_history(category, key, limit)

    return {
        "success": True,
        "data": history,
        "meta": {
            "timestamp": datetime.now().isoformat(),
            "total": len(history),
        }
    }
