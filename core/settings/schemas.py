"""
Settings Schemas
================
Pydantic models for settings validation and serialization.
"""

from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field
from enum import Enum


class SettingType(str, Enum):
    """Supported setting value types."""
    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    ENUM = "enum"
    ARRAY = "array"
    PASSWORD = "password"


class SettingValue(BaseModel):
    """A single setting value."""
    key: str
    value: Any
    value_type: SettingType
    category: str
    label: str
    description: Optional[str] = None
    is_sensitive: bool = False
    options: Optional[List[str]] = None  # For enum types
    min_value: Optional[float] = None  # For number types
    max_value: Optional[float] = None  # For number types
    updated_at: Optional[str] = None
    updated_by: Optional[str] = None


class SettingDefinition(BaseModel):
    """Definition of a setting including its constraints."""
    key: str
    label: str
    description: str
    value_type: SettingType
    default_value: Any
    is_sensitive: bool = False
    options: Optional[List[str]] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    required: bool = True


class SettingCategory(BaseModel):
    """A category of settings."""
    id: str
    name: str
    description: str
    icon: str
    order: int
    settings: List[SettingValue] = []


class SettingsResponse(BaseModel):
    """Response containing all settings grouped by category."""
    categories: List[SettingCategory]


class SettingUpdateRequest(BaseModel):
    """Request to update a setting."""
    value: Any = Field(..., description="New value for the setting")


class SettingsExport(BaseModel):
    """Exported settings (excludes sensitive values)."""
    version: str = "1.0"
    exported_at: str
    exported_by: str
    settings: Dict[str, Dict[str, Any]]


class SettingsImportRequest(BaseModel):
    """Request to import settings."""
    settings: Dict[str, Dict[str, Any]]
    overwrite: bool = False
