"""
Settings Service
================
Business logic layer for settings management.
"""

import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from .database import SettingsDB
from .defaults import SETTING_DEFINITIONS, SETTING_CATEGORIES
from .schemas import (
    SettingValue,
    SettingCategory,
    SettingsResponse,
    SettingType,
    SettingsExport,
)


# Singleton instance
_settings_service: Optional["SettingsService"] = None


def get_settings_service() -> "SettingsService":
    """Get or create the global settings service instance."""
    global _settings_service
    if _settings_service is None:
        _settings_service = SettingsService()
    return _settings_service


class SettingsService:
    """Service for managing platform settings."""

    def __init__(self, db_path: Optional[str] = None):
        """Initialize the settings service.

        Args:
            db_path: Path to settings database
        """
        self.db = SettingsDB(db_path)
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_loaded = False

    def _load_cache(self):
        """Load all settings into memory cache."""
        if not self._cache_loaded:
            all_settings = self.db.get_all_settings()
            self._cache = {}
            for category, settings in all_settings.items():
                self._cache[category] = {}
                for setting in settings:
                    self._cache[category][setting["key"]] = setting["value"]
            self._cache_loaded = True

    def _invalidate_cache(self, category: str, key: str):
        """Invalidate a specific cache entry."""
        if category in self._cache and key in self._cache[category]:
            del self._cache[category][key]

    def get(self, category: str, key: str, default: Any = None) -> Any:
        """Get a setting value with caching.

        This is the primary method for other modules to access settings.

        Args:
            category: Setting category
            key: Setting key
            default: Default value if setting not found

        Returns:
            Setting value or default
        """
        self._load_cache()

        if category in self._cache and key in self._cache[category]:
            return self._cache[category][key]

        # Fallback to database
        setting = self.db.get_setting(category, key)
        if setting:
            # Update cache
            if category not in self._cache:
                self._cache[category] = {}
            self._cache[category][key] = setting["value"]
            return setting["value"]

        return default

    def get_all_for_api(self) -> SettingsResponse:
        """Get all settings formatted for API response.

        Returns:
            SettingsResponse with all categories and settings
        """
        all_settings = self.db.get_all_settings()
        categories = []

        for category_id, category_info in SETTING_CATEGORIES.items():
            # Get definitions for this category
            definitions = {d.key: d for d in SETTING_DEFINITIONS.get(category_id, [])}

            # Build setting values
            settings = []
            db_settings = {s["key"]: s for s in all_settings.get(category_id, [])}

            for definition in SETTING_DEFINITIONS.get(category_id, []):
                db_setting = db_settings.get(definition.key, {})

                # Mask sensitive values
                value = db_setting.get("value", definition.default_value)
                if definition.is_sensitive and value:
                    value = "••••••••"

                settings.append(SettingValue(
                    key=definition.key,
                    value=value,
                    value_type=definition.value_type,
                    category=category_id,
                    label=definition.label,
                    description=definition.description,
                    is_sensitive=definition.is_sensitive,
                    options=definition.options,
                    min_value=definition.min_value,
                    max_value=definition.max_value,
                    updated_at=db_setting.get("updated_at"),
                    updated_by=db_setting.get("updated_by"),
                ))

            categories.append(SettingCategory(
                id=category_id,
                name=category_info["name"],
                description=category_info["description"],
                icon=category_info["icon"],
                order=category_info["order"],
                settings=settings,
            ))

        # Sort categories by order
        categories.sort(key=lambda c: c.order)

        return SettingsResponse(categories=categories)

    def get_category(self, category_id: str) -> Optional[SettingCategory]:
        """Get a single category with its settings.

        Args:
            category_id: Category identifier

        Returns:
            SettingCategory or None if not found
        """
        if category_id not in SETTING_CATEGORIES:
            return None

        category_info = SETTING_CATEGORIES[category_id]
        definitions = {d.key: d for d in SETTING_DEFINITIONS.get(category_id, [])}
        db_settings = {s["key"]: s for s in self.db.get_category_settings(category_id)}

        settings = []
        for definition in SETTING_DEFINITIONS.get(category_id, []):
            db_setting = db_settings.get(definition.key, {})

            value = db_setting.get("value", definition.default_value)
            if definition.is_sensitive and value:
                value = "••••••••"

            settings.append(SettingValue(
                key=definition.key,
                value=value,
                value_type=definition.value_type,
                category=category_id,
                label=definition.label,
                description=definition.description,
                is_sensitive=definition.is_sensitive,
                options=definition.options,
                min_value=definition.min_value,
                max_value=definition.max_value,
                updated_at=db_setting.get("updated_at"),
                updated_by=db_setting.get("updated_by"),
            ))

        return SettingCategory(
            id=category_id,
            name=category_info["name"],
            description=category_info["description"],
            icon=category_info["icon"],
            order=category_info["order"],
            settings=settings,
        )

    def update_setting(
        self,
        category: str,
        key: str,
        value: Any,
        updated_by: Optional[str] = None
    ) -> bool:
        """Update a setting value with validation.

        Args:
            category: Setting category
            key: Setting key
            value: New value
            updated_by: Username making the change

        Returns:
            True if updated successfully

        Raises:
            ValueError: If validation fails
        """
        # Find definition
        definition = None
        for d in SETTING_DEFINITIONS.get(category, []):
            if d.key == key:
                definition = d
                break

        if definition is None:
            raise ValueError(f"Unknown setting: {category}/{key}")

        # Validate value
        self._validate_value(value, definition)

        # Update in database
        success = self.db.update_setting(category, key, value, updated_by)

        if success:
            # Update cache
            if category not in self._cache:
                self._cache[category] = {}
            self._cache[category][key] = value

        return success

    def _validate_value(self, value: Any, definition) -> None:
        """Validate a value against its definition.

        Raises:
            ValueError: If validation fails
        """
        value_type = definition.value_type

        if value_type == SettingType.STRING or value_type == SettingType.PASSWORD:
            if not isinstance(value, str):
                raise ValueError(f"{definition.key}: must be a string")

        elif value_type == SettingType.NUMBER:
            if not isinstance(value, (int, float)):
                raise ValueError(f"{definition.key}: must be a number")
            if definition.min_value is not None and value < definition.min_value:
                raise ValueError(f"{definition.key}: must be at least {definition.min_value}")
            if definition.max_value is not None and value > definition.max_value:
                raise ValueError(f"{definition.key}: must be at most {definition.max_value}")

        elif value_type == SettingType.BOOLEAN:
            if not isinstance(value, bool):
                raise ValueError(f"{definition.key}: must be a boolean")

        elif value_type == SettingType.ENUM:
            if definition.options and value not in definition.options:
                raise ValueError(f"{definition.key}: must be one of {definition.options}")

        elif value_type == SettingType.ARRAY:
            if not isinstance(value, list):
                raise ValueError(f"{definition.key}: must be an array")

    def reset_category(self, category: str, updated_by: Optional[str] = None) -> bool:
        """Reset a category to default values.

        Args:
            category: Category to reset
            updated_by: Username making the change

        Returns:
            True if successful
        """
        if category not in SETTING_CATEGORIES:
            return False

        self.db.reset_to_defaults(category, updated_by)

        # Clear cache for this category
        if category in self._cache:
            del self._cache[category]

        return True

    def reset_all(self, updated_by: Optional[str] = None):
        """Reset all settings to defaults.

        Args:
            updated_by: Username making the change
        """
        self.db.reset_to_defaults(updated_by=updated_by)
        self._cache = {}
        self._cache_loaded = False

    def export_settings(self, exported_by: str) -> SettingsExport:
        """Export all settings (excludes sensitive values).

        Args:
            exported_by: Username exporting

        Returns:
            SettingsExport object
        """
        all_settings = self.db.get_all_settings()
        export_data = {}

        for category, settings in all_settings.items():
            export_data[category] = {}
            for setting in settings:
                # Skip sensitive values
                if not setting.get("is_sensitive"):
                    export_data[category][setting["key"]] = setting["value"]

        return SettingsExport(
            version="1.0",
            exported_at=datetime.now().isoformat(),
            exported_by=exported_by,
            settings=export_data,
        )

    def import_settings(
        self,
        settings: Dict[str, Dict[str, Any]],
        imported_by: str,
        overwrite: bool = False
    ) -> Dict[str, int]:
        """Import settings from export data.

        Args:
            settings: Settings data to import
            imported_by: Username importing
            overwrite: Whether to overwrite existing values

        Returns:
            Dict with counts of imported/skipped/errors
        """
        result = {"imported": 0, "skipped": 0, "errors": 0}

        for category, category_settings in settings.items():
            if category not in SETTING_CATEGORIES:
                result["errors"] += len(category_settings)
                continue

            for key, value in category_settings.items():
                try:
                    # Check if setting exists
                    existing = self.db.get_setting(category, key)
                    if existing and not overwrite:
                        result["skipped"] += 1
                        continue

                    self.update_setting(category, key, value, imported_by)
                    result["imported"] += 1
                except ValueError:
                    result["errors"] += 1

        return result

    def get_audit_history(
        self,
        category: Optional[str] = None,
        key: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get setting change history.

        Args:
            category: Filter by category
            key: Filter by key
            limit: Maximum records

        Returns:
            List of audit entries
        """
        return self.db.get_audit_history(category, key, limit)
