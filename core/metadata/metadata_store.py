# SAGE - Metadata Store Module
# =============================
# Stores and manages golden metadata
"""
Metadata store for managing golden metadata.

Features:
- Store approved metadata in JSON format
- Query metadata by domain, variable, codelist
- Export golden_metadata.json
- Integration with version control
- Search across all metadata
"""

import logging
import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime

from .excel_parser import DomainSpec, VariableSpec, CodelistSpec
from .codelist_merger import EnrichedDomain, EnrichedVariable, MergeResult
from .version_control import VersionControl, MetadataChange, ChangeType

logger = logging.getLogger(__name__)


@dataclass
class ApprovalStatus:
    """Approval status for a metadata item."""
    status: str  # pending, approved, rejected
    reviewed_by: Optional[str] = None
    reviewed_at: Optional[str] = None
    comment: Optional[str] = None


@dataclass
class GoldenVariable:
    """A variable in the golden metadata store."""
    domain: str
    name: str
    label: str
    data_type: str
    length: Optional[int] = None
    format: Optional[str] = None
    codelist: Optional[str] = None
    codelist_values: List[Dict[str, str]] = field(default_factory=list)
    origin: Optional[str] = None
    role: Optional[str] = None
    core: Optional[str] = None
    description: Optional[str] = None
    derivation: Optional[str] = None
    plain_english: Optional[str] = None  # LLM-generated description
    approval: ApprovalStatus = field(default_factory=lambda: ApprovalStatus(status="pending"))

    def to_dict(self) -> Dict[str, Any]:
        return {
            'domain': self.domain,
            'name': self.name,
            'label': self.label,
            'data_type': self.data_type,
            'length': self.length,
            'format': self.format,
            'codelist': self.codelist,
            'codelist_values': self.codelist_values,
            'origin': self.origin,
            'role': self.role,
            'core': self.core,
            'description': self.description,
            'derivation': self.derivation,
            'plain_english': self.plain_english,
            'approval': {
                'status': self.approval.status,
                'reviewed_by': self.approval.reviewed_by,
                'reviewed_at': self.approval.reviewed_at,
                'comment': self.approval.comment
            }
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'GoldenVariable':
        approval_data = data.get('approval', {})
        return cls(
            domain=data['domain'],
            name=data['name'],
            label=data.get('label', ''),
            data_type=data.get('data_type', 'Char'),
            length=data.get('length'),
            format=data.get('format'),
            codelist=data.get('codelist'),
            codelist_values=data.get('codelist_values', []),
            origin=data.get('origin'),
            role=data.get('role'),
            core=data.get('core'),
            description=data.get('description'),
            derivation=data.get('derivation'),
            plain_english=data.get('plain_english'),
            approval=ApprovalStatus(
                status=approval_data.get('status', 'pending'),
                reviewed_by=approval_data.get('reviewed_by'),
                reviewed_at=approval_data.get('reviewed_at'),
                comment=approval_data.get('comment')
            )
        )


@dataclass
class GoldenDomain:
    """A domain in the golden metadata store."""
    name: str
    label: str
    structure: str = ""
    purpose: str = ""
    keys: List[str] = field(default_factory=list)
    variables: List[GoldenVariable] = field(default_factory=list)
    approval: ApprovalStatus = field(default_factory=lambda: ApprovalStatus(status="pending"))

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'label': self.label,
            'structure': self.structure,
            'purpose': self.purpose,
            'keys': self.keys,
            'variables': [v.to_dict() for v in self.variables],
            'approval': {
                'status': self.approval.status,
                'reviewed_by': self.approval.reviewed_by,
                'reviewed_at': self.approval.reviewed_at,
                'comment': self.approval.comment
            }
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'GoldenDomain':
        approval_data = data.get('approval', {})
        return cls(
            name=data['name'],
            label=data.get('label', ''),
            structure=data.get('structure', ''),
            purpose=data.get('purpose', ''),
            keys=data.get('keys', []),
            variables=[GoldenVariable.from_dict(v) for v in data.get('variables', [])],
            approval=ApprovalStatus(
                status=approval_data.get('status', 'pending'),
                reviewed_by=approval_data.get('reviewed_by'),
                reviewed_at=approval_data.get('reviewed_at'),
                comment=approval_data.get('comment')
            )
        )

    def get_variable(self, name: str) -> Optional[GoldenVariable]:
        """Get a variable by name."""
        name_upper = name.upper()
        for var in self.variables:
            if var.name.upper() == name_upper:
                return var
        return None


@dataclass
class GoldenCodelist:
    """A codelist in the golden metadata store."""
    name: str
    label: str
    data_type: str = "text"
    values: List[Dict[str, str]] = field(default_factory=list)
    approval: ApprovalStatus = field(default_factory=lambda: ApprovalStatus(status="pending"))

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'label': self.label,
            'data_type': self.data_type,
            'values': self.values,
            'approval': {
                'status': self.approval.status,
                'reviewed_by': self.approval.reviewed_by,
                'reviewed_at': self.approval.reviewed_at,
                'comment': self.approval.comment
            }
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'GoldenCodelist':
        approval_data = data.get('approval', {})
        return cls(
            name=data['name'],
            label=data.get('label', ''),
            data_type=data.get('data_type', 'text'),
            values=data.get('values', []),
            approval=ApprovalStatus(
                status=approval_data.get('status', 'pending'),
                reviewed_by=approval_data.get('reviewed_by'),
                reviewed_at=approval_data.get('reviewed_at'),
                comment=approval_data.get('comment')
            )
        )


class MetadataStore:
    """
    Store and manage golden metadata.

    Example:
        store = MetadataStore("knowledge/golden_metadata.json")

        # Import from merge result
        store.import_merge_result(merge_result)

        # Approve a variable
        store.approve_variable("DM", "USUBJID", user="admin")

        # Export approved metadata
        store.export_golden_metadata("knowledge/golden_metadata.json")
    """

    def __init__(
        self,
        storage_path: str = "knowledge/golden_metadata.json",
        version_db: str = "knowledge/metadata_versions.db"
    ):
        """
        Initialize the metadata store.

        Args:
            storage_path: Path to JSON file for metadata storage
            version_db: Path to version control database
        """
        self.storage_path = Path(storage_path)
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)

        self._domains: Dict[str, GoldenDomain] = {}
        self._codelists: Dict[str, GoldenCodelist] = {}
        self._metadata: Dict[str, Any] = {}

        self.version_control = VersionControl(version_db)

        # Load existing data if available
        if self.storage_path.exists():
            self.load()

    def load(self) -> bool:
        """Load metadata from storage file."""
        try:
            with open(self.storage_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self._domains = {
                d['name']: GoldenDomain.from_dict(d)
                for d in data.get('domains', [])
            }
            self._codelists = {
                c['name']: GoldenCodelist.from_dict(c)
                for c in data.get('codelists', [])
            }
            self._metadata = data.get('metadata', {})

            logger.info(f"Loaded {len(self._domains)} domains, {len(self._codelists)} codelists")
            return True

        except Exception as e:
            logger.error(f"Failed to load metadata: {e}")
            return False

    def save(self, user: str = "system", comment: str = ""):
        """Save metadata to storage file and create version."""
        data = self.to_dict()

        # Save to file
        with open(self.storage_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str)

        # Create version
        self.version_control.create_version(
            content=data,
            comment=comment or "Metadata update",
            user=user
        )

        logger.info(f"Saved metadata to {self.storage_path}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert store to dictionary."""
        return {
            'domains': [d.to_dict() for d in self._domains.values()],
            'codelists': [c.to_dict() for c in self._codelists.values()],
            'metadata': {
                **self._metadata,
                'last_updated': datetime.now().isoformat(),
                'total_domains': len(self._domains),
                'total_variables': sum(len(d.variables) for d in self._domains.values()),
                'total_codelists': len(self._codelists)
            }
        }

    def import_merge_result(self, result: MergeResult, user: str = "system"):
        """
        Import domains and variables from a merge result.

        Args:
            result: MergeResult from CodelistMerger
            user: User performing the import
        """
        changes = []

        for enriched_domain in result.domains:
            domain_name = enriched_domain.domain.name

            # Check if domain exists
            is_new_domain = domain_name not in self._domains

            # Create or update domain
            golden_vars = []
            for ev in enriched_domain.variables:
                golden_var = GoldenVariable(
                    domain=domain_name,
                    name=ev.variable.name,
                    label=ev.variable.label,
                    data_type=ev.variable.data_type,
                    length=ev.variable.length,
                    format=ev.variable.format,
                    codelist=ev.codelist_name,
                    codelist_values=ev.codelist_values,
                    origin=ev.variable.origin,
                    role=ev.variable.role,
                    core=ev.variable.core,
                    description=ev.variable.description,
                    derivation=ev.variable.derivation,
                    approval=ApprovalStatus(status="pending")
                )
                golden_vars.append(golden_var)

                # Record change
                changes.append(MetadataChange(
                    entity_type="variable",
                    entity_id=f"{domain_name}.{ev.variable.name}",
                    change_type=ChangeType.IMPORTED,
                    user=user
                ))

            golden_domain = GoldenDomain(
                name=domain_name,
                label=enriched_domain.domain.label,
                structure=enriched_domain.domain.structure,
                purpose=enriched_domain.domain.purpose,
                keys=enriched_domain.domain.keys,
                variables=golden_vars,
                approval=ApprovalStatus(status="pending")
            )

            self._domains[domain_name] = golden_domain

            changes.append(MetadataChange(
                entity_type="domain",
                entity_id=domain_name,
                change_type=ChangeType.IMPORTED if is_new_domain else ChangeType.MODIFIED,
                user=user
            ))

        # Record all changes
        for change in changes:
            self.version_control.record_change(change)

        logger.info(f"Imported {len(result.domains)} domains with {len(changes)} changes")

    def import_codelists(self, codelists: List[CodelistSpec], user: str = "system"):
        """Import codelists into the store."""
        for cl in codelists:
            self._codelists[cl.name] = GoldenCodelist(
                name=cl.name,
                label=cl.label,
                data_type=cl.data_type,
                values=cl.values,
                approval=ApprovalStatus(status="pending")
            )

            self.version_control.record_change(MetadataChange(
                entity_type="codelist",
                entity_id=cl.name,
                change_type=ChangeType.IMPORTED,
                user=user
            ))

        logger.info(f"Imported {len(codelists)} codelists")

    # Domain operations
    def get_domain(self, name: str) -> Optional[GoldenDomain]:
        """Get a domain by name."""
        return self._domains.get(name.upper())

    def list_domains(self) -> List[str]:
        """List all domain names."""
        return list(self._domains.keys())

    def get_all_domains(self) -> List[GoldenDomain]:
        """Get all domains."""
        return list(self._domains.values())

    def delete_domain(self, name: str) -> bool:
        """Delete a domain and all its variables."""
        domain_key = name.upper()
        if domain_key in self._domains:
            del self._domains[domain_key]
            logger.info(f"Deleted domain: {name}")
            return True
        return False

    # Variable operations
    def get_variable(self, domain: str, name: str) -> Optional[GoldenVariable]:
        """Get a variable by domain and name."""
        d = self.get_domain(domain)
        if d:
            return d.get_variable(name)
        return None

    def get_all_variables(self) -> List[GoldenVariable]:
        """Get all variables across all domains."""
        variables = []
        for domain in self._domains.values():
            variables.extend(domain.variables)
        return variables

    def update_variable(
        self,
        domain: str,
        name: str,
        updates: Dict[str, Any],
        user: str = "system"
    ) -> bool:
        """
        Update a variable's properties.

        Args:
            domain: Domain name
            name: Variable name
            updates: Dictionary of fields to update
            user: User making the update

        Returns:
            True if updated successfully
        """
        var = self.get_variable(domain, name)
        if not var:
            return False

        for field_name, new_value in updates.items():
            if hasattr(var, field_name):
                old_value = getattr(var, field_name)
                setattr(var, field_name, new_value)

                self.version_control.record_change(MetadataChange(
                    entity_type="variable",
                    entity_id=f"{domain}.{name}",
                    change_type=ChangeType.MODIFIED,
                    field_name=field_name,
                    old_value=str(old_value),
                    new_value=str(new_value),
                    user=user
                ))

        # Reset approval status when modified
        var.approval = ApprovalStatus(status="pending")
        return True

    # Codelist operations
    def get_codelist(self, name: str) -> Optional[GoldenCodelist]:
        """Get a codelist by name."""
        return self._codelists.get(name.upper())

    def list_codelists(self) -> List[str]:
        """List all codelist names."""
        return list(self._codelists.keys())

    def get_all_codelists(self) -> List[GoldenCodelist]:
        """Get all codelists."""
        return list(self._codelists.values())

    # Approval operations
    def approve_variable(
        self,
        domain: str,
        name: str,
        user: str,
        comment: Optional[str] = None
    ) -> bool:
        """Approve a variable."""
        var = self.get_variable(domain, name)
        if not var:
            return False

        var.approval = ApprovalStatus(
            status="approved",
            reviewed_by=user,
            reviewed_at=datetime.now().isoformat(),
            comment=comment
        )

        self.version_control.set_approval_status(
            entity_type="variable",
            entity_id=f"{domain}.{name}",
            status="approved",
            user=user,
            comment=comment
        )

        return True

    def reject_variable(
        self,
        domain: str,
        name: str,
        user: str,
        comment: str
    ) -> bool:
        """Reject a variable."""
        var = self.get_variable(domain, name)
        if not var:
            return False

        var.approval = ApprovalStatus(
            status="rejected",
            reviewed_by=user,
            reviewed_at=datetime.now().isoformat(),
            comment=comment
        )

        self.version_control.set_approval_status(
            entity_type="variable",
            entity_id=f"{domain}.{name}",
            status="rejected",
            user=user,
            comment=comment
        )

        return True

    def approve_domain(self, name: str, user: str, comment: Optional[str] = None) -> bool:
        """Approve a domain and all its variables."""
        domain = self.get_domain(name)
        if not domain:
            return False

        domain.approval = ApprovalStatus(
            status="approved",
            reviewed_by=user,
            reviewed_at=datetime.now().isoformat(),
            comment=comment
        )

        # Approve all pending variables
        for var in domain.variables:
            if var.approval.status == "pending":
                var.approval = ApprovalStatus(
                    status="approved",
                    reviewed_by=user,
                    reviewed_at=datetime.now().isoformat(),
                    comment="Approved with domain"
                )

        self.version_control.set_approval_status(
            entity_type="domain",
            entity_id=name,
            status="approved",
            user=user,
            comment=comment
        )

        return True

    def bulk_approve_variables(
        self,
        domain_name: str,
        user: str,
        comment: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Bulk approve all pending variables in a domain.
        Creates only ONE version entry for the entire operation.

        Returns:
            Dict with 'approved' count and 'total' pending count
        """
        domain = self.get_domain(domain_name)
        if not domain:
            return {'approved': 0, 'total': 0}

        now = datetime.now().isoformat()
        approved_count = 0
        pending_vars = []

        # Find and approve all pending variables
        for var in domain.variables:
            if var.approval.status != "approved":
                pending_vars.append(var.name)
                var.approval = ApprovalStatus(
                    status="approved",
                    reviewed_by=user,
                    reviewed_at=now,
                    comment=comment or "Bulk approved"
                )
                approved_count += 1

        # Create single version entry for bulk operation
        if approved_count > 0:
            self.version_control.record_change(MetadataChange(
                entity_type="domain",
                entity_id=domain_name,
                change_type=ChangeType.APPROVED,
                field_name="bulk_approval",
                old_value=None,
                new_value=f"{approved_count} variables",
                user=user,
                comment=f"Bulk approved {approved_count} variables: {', '.join(pending_vars[:5])}{'...' if len(pending_vars) > 5 else ''}"
            ))

            # Save the changes
            self.save()

        return {'approved': approved_count, 'total': len(pending_vars)}

    def approve_codelist(
        self,
        name: str,
        user: str,
        comment: Optional[str] = None
    ) -> bool:
        """Approve a codelist."""
        codelist = self.get_codelist(name)
        if not codelist:
            return False

        codelist.approval = ApprovalStatus(
            status="approved",
            reviewed_by=user,
            reviewed_at=datetime.now().isoformat(),
            comment=comment
        )

        self.version_control.set_approval_status(
            entity_type="codelist",
            entity_id=name,
            status="approved",
            user=user,
            comment=comment
        )

        return True

    def get_pending_items(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get all items pending approval."""
        pending = {
            'domains': [],
            'variables': [],
            'codelists': []
        }

        for domain in self._domains.values():
            if domain.approval.status == "pending":
                pending['domains'].append({
                    'name': domain.name,
                    'label': domain.label,
                    'variable_count': len(domain.variables)
                })

            for var in domain.variables:
                if var.approval.status == "pending":
                    pending['variables'].append({
                        'domain': domain.name,
                        'name': var.name,
                        'label': var.label,
                        'derivation': var.derivation
                    })

        for codelist in self._codelists.values():
            if codelist.approval.status == "pending":
                pending['codelists'].append({
                    'name': codelist.name,
                    'label': codelist.label,
                    'value_count': len(codelist.values)
                })

        return pending

    def get_approval_stats(self) -> Dict[str, Dict[str, int]]:
        """Get approval statistics."""
        stats = {
            'domains': {'pending': 0, 'approved': 0, 'rejected': 0},
            'variables': {'pending': 0, 'approved': 0, 'rejected': 0},
            'codelists': {'pending': 0, 'approved': 0, 'rejected': 0}
        }

        for domain in self._domains.values():
            stats['domains'][domain.approval.status] += 1
            for var in domain.variables:
                stats['variables'][var.approval.status] += 1

        for codelist in self._codelists.values():
            stats['codelists'][codelist.approval.status] += 1

        return stats

    # Search operations
    def search(self, query: str, search_type: str = "all") -> List[Dict[str, Any]]:
        """
        Search across metadata.

        Args:
            query: Search query string
            search_type: Type to search (all, domain, variable, codelist)

        Returns:
            List of matching items
        """
        results = []
        query_lower = query.lower()

        if search_type in ["all", "domain"]:
            for domain in self._domains.values():
                if query_lower in domain.name.lower() or \
                   query_lower in domain.label.lower():
                    results.append({
                        'type': 'domain',
                        'name': domain.name,
                        'label': domain.label,
                        'match_field': 'name' if query_lower in domain.name.lower() else 'label'
                    })

        if search_type in ["all", "variable"]:
            for domain in self._domains.values():
                for var in domain.variables:
                    match_fields = []
                    if query_lower in var.name.lower():
                        match_fields.append('name')
                    if query_lower in var.label.lower():
                        match_fields.append('label')
                    if var.description and query_lower in var.description.lower():
                        match_fields.append('description')
                    if var.derivation and query_lower in var.derivation.lower():
                        match_fields.append('derivation')

                    if match_fields:
                        results.append({
                            'type': 'variable',
                            'domain': domain.name,
                            'name': var.name,
                            'label': var.label,
                            'match_fields': match_fields
                        })

        if search_type in ["all", "codelist"]:
            for codelist in self._codelists.values():
                match_fields = []
                if query_lower in codelist.name.lower():
                    match_fields.append('name')
                if query_lower in codelist.label.lower():
                    match_fields.append('label')

                # Search values
                for val in codelist.values:
                    if query_lower in val.get('code', '').lower() or \
                       query_lower in val.get('decode', '').lower():
                        match_fields.append('values')
                        break

                if match_fields:
                    results.append({
                        'type': 'codelist',
                        'name': codelist.name,
                        'label': codelist.label,
                        'match_fields': match_fields
                    })

        return results

    # Export operations
    def export_golden_metadata(
        self,
        output_path: Optional[str] = None,
        approved_only: bool = True
    ) -> str:
        """
        Export golden metadata to JSON.

        Args:
            output_path: Output file path (defaults to storage_path)
            approved_only: Only export approved items

        Returns:
            Path to exported file
        """
        output_path = Path(output_path) if output_path else self.storage_path

        if approved_only:
            # Filter to approved items only
            export_data = {
                'domains': [],
                'codelists': [],
                'metadata': {
                    'exported_at': datetime.now().isoformat(),
                    'approved_only': True
                }
            }

            for domain in self._domains.values():
                # Get approved variables for this domain
                approved_vars = [
                    v.to_dict() for v in domain.variables
                    if v.approval.status == 'approved'
                ]

                # Include domain if it has any approved variables
                # (use calculated status based on variables, not stored domain status)
                if approved_vars:
                    domain_dict = domain.to_dict()
                    domain_dict['variables'] = approved_vars
                    export_data['domains'].append(domain_dict)

            for codelist in self._codelists.values():
                if codelist.approval.status == "approved":
                    export_data['codelists'].append(codelist.to_dict())

        else:
            export_data = self.to_dict()

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, default=str)

        logger.info(f"Exported golden metadata to {output_path}")
        return str(output_path)

    def get_statistics(self) -> Dict[str, Any]:
        """Get store statistics."""
        total_vars = sum(len(d.variables) for d in self._domains.values())
        approved_vars = sum(
            sum(1 for v in d.variables if v.approval.status == "approved")
            for d in self._domains.values()
        )

        return {
            'total_domains': len(self._domains),
            'total_variables': total_vars,
            'total_codelists': len(self._codelists),
            'approved_domains': sum(1 for d in self._domains.values() if d.approval.status == "approved"),
            'approved_variables': approved_vars,
            'approved_codelists': sum(1 for c in self._codelists.values() if c.approval.status == "approved"),
            'pending_variables': total_vars - approved_vars,
            'approval_percentage': round(approved_vars / total_vars * 100, 1) if total_vars > 0 else 0
        }
