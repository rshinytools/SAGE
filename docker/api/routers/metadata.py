# SAGE API - Metadata Factory Router
# ====================================
"""Metadata Factory endpoints for golden metadata management."""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, UploadFile, File, Query, Depends
from fastapi.responses import FileResponse, StreamingResponse
import asyncio
import json

# Add project root to path - for local dev it's 4 levels up, in Docker it's /app
project_root = Path(__file__).parent.parent.parent.parent
if not (project_root / "knowledge").exists():
    # Docker environment - use /app
    project_root = Path("/app")
sys.path.insert(0, str(project_root))

from .auth import get_current_user

router = APIRouter()

# Configuration - use env var or fall back to project_root/knowledge
KNOWLEDGE_DIR = Path(os.getenv("KNOWLEDGE_DIR", str(project_root / "knowledge")))
METADATA_PATH = KNOWLEDGE_DIR / "golden_metadata.json"
VERSION_DB = KNOWLEDGE_DIR / "metadata_versions.db"
CDISC_DB = KNOWLEDGE_DIR / "cdisc_library.db"


def get_metadata_store():
    """Get metadata store instance."""
    try:
        from core.metadata import MetadataStore
        return MetadataStore(str(METADATA_PATH), str(VERSION_DB))
    except Exception:
        return None


# ============================================
# Domains
# ============================================

@router.get("/domains")
async def list_domains(current_user: dict = Depends(get_current_user)):
    """
    List all domains in the metadata store.
    """
    store = get_metadata_store()
    if not store:
        return {
            "success": True,
            "data": [],
            "meta": {"timestamp": datetime.now().isoformat(), "count": 0}
        }

    domains = []
    for domain in store.get_all_domains():
        # Count approved and pending variables
        approved_count = sum(1 for v in domain.variables if v.approval.status == 'approved')
        pending_count = sum(1 for v in domain.variables if v.approval.status == 'pending')
        total_count = len(domain.variables)

        # Calculate domain status based on variables:
        # - "approved" if all variables are approved
        # - "pending" if any variables are pending
        # - use stored status as fallback
        if total_count > 0:
            if pending_count == 0 and approved_count == total_count:
                calculated_status = "approved"
            elif approved_count > 0 and pending_count > 0:
                calculated_status = "partial"  # Some approved, some pending
            else:
                calculated_status = "pending"
        else:
            calculated_status = domain.approval.status

        domains.append({
            "name": domain.name,
            "label": domain.label,
            "variable_count": total_count,
            "approved_count": approved_count,
            "pending_count": pending_count,
            "status": calculated_status
        })

    return {
        "success": True,
        "data": domains,
        "meta": {"timestamp": datetime.now().isoformat(), "count": len(domains)}
    }


@router.get("/domains/{name}")
async def get_domain(
    name: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Get domain details with variables.
    """
    store = get_metadata_store()
    if not store:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Metadata store not initialized"}
        )

    domain = store.get_domain(name)
    if not domain:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Domain not found: {name}"}
        )

    return {
        "success": True,
        "data": domain.to_dict(),
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.put("/domains/{name}")
async def update_domain(
    name: str,
    label: Optional[str] = None,
    structure: Optional[str] = None,
    purpose: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    Update domain properties.
    """
    store = get_metadata_store()
    if not store:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Metadata store not initialized"}
        )

    domain = store.get_domain(name)
    if not domain:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Domain not found: {name}"}
        )

    # Update fields
    if label is not None:
        domain.label = label
    if structure is not None:
        domain.structure = structure
    if purpose is not None:
        domain.purpose = purpose

    store.save(user=current_user.get("sub", "api"), comment=f"Updated domain {name}")

    return {
        "success": True,
        "data": domain.to_dict(),
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.delete("/domains/{name}")
async def delete_domain(
    name: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Delete a domain and all its variables.
    """
    store = get_metadata_store()
    if not store:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Metadata store not initialized"}
        )

    domain = store.get_domain(name)
    if not domain:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Domain not found: {name}"}
        )

    variable_count = len(domain.variables)

    # Remove domain from store
    if not store.delete_domain(name):
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": f"Failed to delete domain: {name}"}
        )

    user = current_user.get("sub", "api")
    store.save(user=user, comment=f"Deleted domain {name} with {variable_count} variables")

    return {
        "success": True,
        "data": {"message": f"Domain {name} deleted with {variable_count} variables"},
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/domains/{name}/approve")
async def approve_domain(
    name: str,
    comment: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    Approve a domain and all its variables.
    """
    store = get_metadata_store()
    if not store:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Metadata store not initialized"}
        )

    user = current_user.get("sub", "api")
    if not store.approve_domain(name, user=user, comment=comment):
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Domain not found: {name}"}
        )

    store.save(user=user, comment=f"Approved domain {name}")

    return {
        "success": True,
        "data": {"message": f"Domain {name} approved"},
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/domains/{name}/reject")
async def reject_domain(
    name: str,
    comment: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Reject a domain.
    """
    return {
        "success": True,
        "data": {"message": f"Domain {name} rejected: {comment}"},
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/domains/{name}/bulk-approve")
async def bulk_approve_domain_variables(
    name: str,
    comment: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    Bulk approve all pending variables in a domain.
    Creates only ONE version entry for the entire operation.
    """
    store = get_metadata_store()
    if not store:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Metadata store not initialized"}
        )

    user = current_user.get("sub", "api")
    result = store.bulk_approve_variables(name, user=user, comment=comment)

    if result['approved'] == 0 and result['total'] == 0:
        # Check if domain exists
        if not store.get_domain(name):
            raise HTTPException(
                status_code=404,
                detail={"code": "NOT_FOUND", "message": f"Domain not found: {name}"}
            )

    return {
        "success": True,
        "data": {
            "message": f"Bulk approved {result['approved']} variables in domain {name}",
            "approved": result['approved'],
            "total": result['total']
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


# ============================================
# Variables
# ============================================

@router.get("/domains/{domain}/variables")
async def list_variables(
    domain: str,
    current_user: dict = Depends(get_current_user)
):
    """
    List variables in a domain.
    """
    store = get_metadata_store()
    if not store:
        return {
            "success": True,
            "data": [],
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    domain_obj = store.get_domain(domain)
    if not domain_obj:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Domain not found: {domain}"}
        )

    variables = []
    for var in domain_obj.variables:
        variables.append({
            "name": var.name,
            "label": var.label,
            "data_type": var.data_type,
            "status": var.approval.status
        })

    return {
        "success": True,
        "data": variables,
        "meta": {"timestamp": datetime.now().isoformat(), "count": len(variables)}
    }


@router.get("/domains/{domain}/variables/{name}")
async def get_variable(
    domain: str,
    name: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Get variable details.
    """
    store = get_metadata_store()
    if not store:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Metadata store not initialized"}
        )

    var = store.get_variable(domain, name)
    if not var:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Variable not found: {domain}.{name}"}
        )

    return {
        "success": True,
        "data": var.to_dict(),
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.put("/domains/{domain}/variables/{name}")
async def update_variable(
    domain: str,
    name: str,
    label: Optional[str] = None,
    description: Optional[str] = None,
    plain_english: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    Update variable properties.
    """
    store = get_metadata_store()
    if not store:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Metadata store not initialized"}
        )

    updates = {}
    if label is not None:
        updates['label'] = label
    if description is not None:
        updates['description'] = description
    if plain_english is not None:
        updates['plain_english'] = plain_english

    user = current_user.get("sub", "api")
    if not store.update_variable(domain, name, updates, user=user):
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Variable not found: {domain}.{name}"}
        )

    store.save(user=user, comment=f"Updated variable {domain}.{name}")
    var = store.get_variable(domain, name)

    return {
        "success": True,
        "data": var.to_dict() if var else {},
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/domains/{domain}/variables/{name}/approve")
async def approve_variable(
    domain: str,
    name: str,
    comment: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    Approve a variable.
    """
    store = get_metadata_store()
    if not store:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Metadata store not initialized"}
        )

    user = current_user.get("sub", "api")
    if not store.approve_variable(domain, name, user=user, comment=comment):
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Variable not found: {domain}.{name}"}
        )

    store.save(user=user, comment=f"Approved variable {domain}.{name}")

    return {
        "success": True,
        "data": {"message": f"Variable {domain}.{name} approved"},
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/domains/{domain}/variables/{name}/reject")
async def reject_variable(
    domain: str,
    name: str,
    comment: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Reject a variable.
    """
    store = get_metadata_store()
    if not store:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Metadata store not initialized"}
        )

    user = current_user.get("sub", "api")
    if not store.reject_variable(domain, name, user=user, comment=comment):
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Variable not found: {domain}.{name}"}
        )

    store.save(user=user, comment=f"Rejected variable {domain}.{name}")

    return {
        "success": True,
        "data": {"message": f"Variable {domain}.{name} rejected"},
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/domains/{domain}/variables/{name}/draft")
async def draft_variable_description(
    domain: str,
    name: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Generate LLM description for a variable.
    """
    store = get_metadata_store()
    if not store:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Metadata store not initialized"}
        )

    var = store.get_variable(domain, name)
    if not var:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Variable not found: {domain}.{name}"}
        )

    try:
        from core.metadata import LLMDrafter, DraftRequest
        drafter = LLMDrafter()
        request = DraftRequest(
            variable_name=var.name,
            domain=domain,
            label=var.label,
            derivation=var.derivation,
            codelist=var.codelist,
            codelist_values=var.codelist_values,
            data_type=var.data_type
        )
        result = drafter.draft_description(request)

        return {
            "success": True,
            "data": {
                "plain_english": result.plain_english,
                "confidence": result.confidence,
                "model_used": result.model_used
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


# ============================================
# Codelists
# ============================================

@router.get("/codelists")
async def list_codelists(current_user: dict = Depends(get_current_user)):
    """
    List all codelists.
    """
    store = get_metadata_store()
    if not store:
        return {
            "success": True,
            "data": [],
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    codelists = []
    for cl in store.get_all_codelists():
        codelists.append({
            "name": cl.name,
            "label": cl.label,
            "value_count": len(cl.values),
            "status": cl.approval.status
        })

    return {
        "success": True,
        "data": codelists,
        "meta": {"timestamp": datetime.now().isoformat(), "count": len(codelists)}
    }


@router.get("/codelists/{name}")
async def get_codelist(
    name: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Get codelist details.
    """
    store = get_metadata_store()
    if not store:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Metadata store not initialized"}
        )

    codelist = store.get_codelist(name)
    if not codelist:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Codelist not found: {name}"}
        )

    return {
        "success": True,
        "data": codelist.to_dict(),
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/codelists/{name}/approve")
async def approve_codelist(
    name: str,
    comment: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    Approve a codelist.
    """
    store = get_metadata_store()
    if not store:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Metadata store not initialized"}
        )

    user = current_user.get("sub", "api")
    if not store.approve_codelist(name, user=user, comment=comment):
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Codelist not found: {name}"}
        )

    store.save(user=user, comment=f"Approved codelist {name}")

    return {
        "success": True,
        "data": {"message": f"Codelist {name} approved"},
        "meta": {"timestamp": datetime.now().isoformat()}
    }


# ============================================
# Import/Export
# ============================================

@router.post("/import")
async def import_excel_spec(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Import Excel specification file.
    """
    if not file.filename.endswith(('.xlsx', '.xls', '.xlsm')):
        raise HTTPException(
            status_code=422,
            detail={"code": "VALIDATION_ERROR", "message": "Invalid file type. Must be Excel file."}
        )

    # Save file temporarily
    import tempfile
    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        from core.metadata import ExcelParser, CodelistMerger

        parser = ExcelParser()
        result = parser.parse_file(tmp_path)

        if not result.success:
            raise HTTPException(
                status_code=422,
                detail={"code": "VALIDATION_ERROR", "message": "; ".join(result.errors)}
            )

        # Merge and import
        store = get_metadata_store()
        if store:
            merger = CodelistMerger()
            merger.add_codelists(result.codelists)
            merge_result = merger.merge_domains(result.domains)

            user = current_user.get("sub", "api")
            store.import_merge_result(merge_result, user=user)
            store.import_codelists(result.codelists, user=user)
            store.save(user=user, comment=f"Imported from {file.filename}")

        return {
            "success": True,
            "data": {
                "domains_imported": len(result.domains),
                "variables_imported": sum(len(d.variables) for d in result.domains),
                "codelists_imported": len(result.codelists)
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    finally:
        Path(tmp_path).unlink(missing_ok=True)


@router.get("/export")
async def export_metadata(
    approved_only: bool = Query(default=True),
    current_user: dict = Depends(get_current_user)
):
    """
    Export golden metadata as JSON.
    """
    store = get_metadata_store()
    if not store:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Metadata store not initialized"}
        )

    import tempfile
    with tempfile.NamedTemporaryFile(delete=False, suffix='.json', mode='w') as tmp:
        store.export_golden_metadata(tmp.name, approved_only=approved_only)
        tmp_path = tmp.name

    return FileResponse(
        tmp_path,
        media_type="application/json",
        filename="golden_metadata.json"
    )


# ============================================
# Search & Stats
# ============================================

@router.get("/search")
async def search_metadata(
    q: str = Query(..., min_length=1),
    search_type: str = Query(default="all"),
    current_user: dict = Depends(get_current_user)
):
    """
    Search across metadata.
    """
    store = get_metadata_store()
    if not store:
        return {
            "success": True,
            "data": [],
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    try:
        results = store.search(q, search_type=search_type)
    except Exception:
        results = []

    return {
        "success": True,
        "data": results,
        "meta": {"timestamp": datetime.now().isoformat(), "count": len(results)}
    }


@router.get("/stats")
async def get_metadata_stats(current_user: dict = Depends(get_current_user)):
    """
    Get metadata statistics.
    """
    store = get_metadata_store()
    if not store:
        return {
            "success": True,
            "data": {
                "total_domains": 0,
                "total_variables": 0,
                "total_codelists": 0,
                "approved_domains": 0,
                "approved_variables": 0,
                "approved_codelists": 0,
                "pending_count": 0,
                "approval_percentage": 0.0
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    stats = store.get_statistics()

    return {
        "success": True,
        "data": stats,
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.get("/pending")
async def get_pending_items(current_user: dict = Depends(get_current_user)):
    """
    Get items pending approval.
    """
    store = get_metadata_store()
    if not store:
        return {
            "success": True,
            "data": {"domains": [], "variables": [], "codelists": []},
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    pending = store.get_pending_items()

    return {
        "success": True,
        "data": pending,
        "meta": {"timestamp": datetime.now().isoformat()}
    }


# ============================================
# Version Control
# ============================================

@router.get("/versions")
async def list_versions(
    limit: int = Query(default=50, ge=1, le=200),
    current_user: dict = Depends(get_current_user)
):
    """
    List metadata versions.
    """
    store = get_metadata_store()
    if not store:
        return {
            "success": True,
            "data": [],
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    versions = store.version_control.get_versions(limit=limit)

    return {
        "success": True,
        "data": [v.to_dict() for v in versions],
        "meta": {"timestamp": datetime.now().isoformat(), "count": len(versions)}
    }


@router.get("/versions/{version_id}")
async def get_version(
    version_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Get specific version with content.
    """
    store = get_metadata_store()
    if not store:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Metadata store not initialized"}
        )

    result = store.version_control.get_version(version_id)
    if not result:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Version not found: {version_id}"}
        )

    version, content = result

    return {
        "success": True,
        "data": {
            "version": version.to_dict(),
            "content": content
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/versions/{version_id}/rollback")
async def rollback_version(
    version_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Rollback to a previous version.
    """
    store = get_metadata_store()
    if not store:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Metadata store not initialized"}
        )

    user = current_user.get("sub", "api")
    content = store.version_control.rollback(version_id, user=user)

    if not content:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Version not found: {version_id}"}
        )

    # Write the restored content to the golden_metadata.json file
    import json
    with open(store.storage_path, 'w', encoding='utf-8') as f:
        json.dump(content, f, indent=2, default=str)

    # Reload the store from the restored file
    store.load()

    return {
        "success": True,
        "data": {"message": f"Rolled back to version {version_id}"},
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.get("/versions/diff")
async def diff_versions(
    v1: str = Query(..., description="First version ID"),
    v2: str = Query(..., description="Second version ID"),
    current_user: dict = Depends(get_current_user)
):
    """
    Compare two versions.
    """
    store = get_metadata_store()
    if not store:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Metadata store not initialized"}
        )

    diff = store.version_control.diff_versions(v1, v2)
    if not diff:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "One or both versions not found"}
        )

    return {
        "success": True,
        "data": diff.to_dict(),
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.get("/history")
async def get_change_history(
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    limit: int = Query(default=100, ge=1, le=500),
    current_user: dict = Depends(get_current_user)
):
    """
    Get change history.
    """
    store = get_metadata_store()
    if not store:
        return {
            "success": True,
            "data": [],
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    history = store.version_control.get_history(
        entity_type=entity_type,
        entity_id=entity_id,
        limit=limit
    )

    return {
        "success": True,
        "data": [h.to_dict() for h in history],
        "meta": {"timestamp": datetime.now().isoformat(), "count": len(history)}
    }


# ============================================
# CDISC Library
# ============================================

def get_cdisc_library():
    """Get CDISC library instance."""
    try:
        from core.metadata import CDISCLibrary
        if CDISC_DB.exists():
            return CDISCLibrary(str(CDISC_DB))
        return None
    except Exception:
        return None


@router.get("/cdisc/stats")
async def get_cdisc_stats(current_user: dict = Depends(get_current_user)):
    """
    Get CDISC library statistics.
    """
    library = get_cdisc_library()
    if not library:
        return {
            "success": True,
            "data": {
                "initialized": False,
                "total_domains": 0,
                "total_variables": 0
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    stats = library.get_statistics()
    stats["initialized"] = True

    return {
        "success": True,
        "data": stats,
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.get("/cdisc/domains")
async def list_cdisc_domains(
    standard: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    List all CDISC standard domains.
    """
    library = get_cdisc_library()
    if not library:
        return {
            "success": True,
            "data": [],
            "meta": {"timestamp": datetime.now().isoformat(), "count": 0}
        }

    domains = library.get_all_domains(standard)

    return {
        "success": True,
        "data": [d.to_dict() for d in domains],
        "meta": {"timestamp": datetime.now().isoformat(), "count": len(domains)}
    }


@router.get("/cdisc/domains/{name}/variables")
async def list_cdisc_domain_variables(
    name: str,
    standard: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    List variables for a CDISC standard domain.
    """
    library = get_cdisc_library()
    if not library:
        return {
            "success": True,
            "data": [],
            "meta": {"timestamp": datetime.now().isoformat(), "count": 0}
        }

    variables = library.get_domain_variables(name, standard)

    return {
        "success": True,
        "data": [v.to_dict() for v in variables],
        "meta": {"timestamp": datetime.now().isoformat(), "count": len(variables)}
    }


@router.get("/cdisc/search")
async def search_cdisc_variables(
    q: str = Query(..., min_length=1),
    limit: int = Query(default=50, ge=1, le=200),
    current_user: dict = Depends(get_current_user)
):
    """
    Search CDISC standard variables by name or label.
    """
    library = get_cdisc_library()
    if not library:
        return {
            "success": True,
            "data": [],
            "meta": {"timestamp": datetime.now().isoformat(), "count": 0}
        }

    variables = library.search_variables(q, limit)

    return {
        "success": True,
        "data": [v.to_dict() for v in variables],
        "meta": {"timestamp": datetime.now().isoformat(), "count": len(variables)}
    }


@router.post("/cdisc/match")
async def match_variable(
    domain: str,
    name: str,
    label: Optional[str] = None,
    data_type: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    Match a variable against CDISC standards.
    """
    library = get_cdisc_library()
    if not library:
        raise HTTPException(
            status_code=503,
            detail={"code": "SERVICE_UNAVAILABLE", "message": "CDISC library not initialized"}
        )

    result = library.match_variable(domain, name, label or "", data_type or "")

    return {
        "success": True,
        "data": result.to_dict(),
        "meta": {"timestamp": datetime.now().isoformat()}
    }


# ============================================
# Auto-Approval
# ============================================

@router.post("/auto-approve")
async def run_auto_approval_endpoint(
    dry_run: bool = Query(default=False, description="If true, don't apply approvals"),
    current_user: dict = Depends(get_current_user)
):
    """
    Run auto-approval on all pending variables.
    """
    library = get_cdisc_library()
    store = get_metadata_store()

    if not library:
        raise HTTPException(
            status_code=503,
            detail={"code": "SERVICE_UNAVAILABLE", "message": "CDISC library not initialized"}
        )

    if not store:
        raise HTTPException(
            status_code=503,
            detail={"code": "SERVICE_UNAVAILABLE", "message": "Metadata store not initialized"}
        )

    try:
        from core.metadata import AutoApprovalEngine

        engine = AutoApprovalEngine(library)

        # Get all pending variables
        pending = []
        all_vars = store.get_all_variables()

        for var in all_vars:
            # Check if pending
            if var.approval.status == 'pending':
                pending.append({
                    'domain': var.domain,
                    'name': var.name,
                    'label': var.label or '',
                    'data_type': var.data_type or '',
                    'derivation': var.derivation or '',
                    'codelist': var.codelist or ''
                })

        if not pending:
            return {
                "success": True,
                "data": {
                    "message": "No pending variables to process",
                    "total_variables": 0,
                    "auto_approved": 0,
                    "quick_review": 0,
                    "manual_review": 0
                },
                "meta": {"timestamp": datetime.now().isoformat()}
            }

        # Run analysis
        decisions, summary = engine.analyze_batch(pending)

        # Apply approvals if not dry run
        if not dry_run:
            user = current_user.get('username', 'auto_approval')
            for decision in decisions:
                if decision.decision == 'auto_approved':
                    try:
                        store.approve_variable(
                            domain=decision.domain,
                            name=decision.variable_name,
                            user=user,
                            comment=f"Auto-approved: {decision.reason} (confidence: {decision.confidence}%)"
                        )
                    except Exception as e:
                        pass  # Variable might not exist, skip

        return {
            "success": True,
            "data": {
                "message": "Auto-approval complete" + (" (dry run)" if dry_run else ""),
                "dry_run": dry_run,
                **summary.to_dict()
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


# ============================================
# Audit Variables (SSE endpoint)
# ============================================

@router.get("/audit/stream")
async def audit_variables_stream(
    skip_llm: bool = Query(True, description="Skip LLM analysis for non-CDISC variables (fast mode)"),
    current_user: dict = Depends(get_current_user)
):
    """
    Run variable audit with Server-Sent Events for progress updates.

    Step 1: CDISC Library check (instant) - auto-approve CDISC matches
    Step 2: LLM analysis for non-CDISC variables (optional, can be skipped for speed)
    Step 3: Apply approvals and return summary

    Args:
        skip_llm: If True (default), skip LLM analysis and mark non-CDISC variables for manual review.
                  If False, run DeepSeek-R1 analysis on non-CDISC variables (slow but thorough).
    """
    store = get_metadata_store()
    if not store:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Metadata store not initialized"}
        )

    library = get_cdisc_library()
    if not library:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "CDISC library not initialized"}
        )

    # Capture skip_llm in closure for generator
    do_skip_llm = skip_llm

    async def generate_events():
        """Generator for SSE events."""
        try:
            from core.metadata import AutoApprovalEngine

            # Get Ollama host from environment
            ollama_host = os.getenv("OLLAMA_HOST", "http://ollama:11434")
            llm_model = os.getenv("PRIMARY_MODEL", "deepseek-r1:8b")

            engine = AutoApprovalEngine(library, ollama_host, llm_model) if not do_skip_llm else None

            # Get all pending variables
            pending = []
            for domain in store.get_all_domains():
                for var in domain.variables:
                    if var.approval.status == 'pending':
                        pending.append({
                            'domain': domain.name,
                            'name': var.name,
                            'label': var.label or '',
                            'data_type': var.data_type or '',
                            'derivation': var.derivation or '',
                            'codelist': var.codelist or ''
                        })

            if not pending:
                yield f"data: {json.dumps({'type': 'complete', 'data': {'message': 'No pending variables', 'total': 0}})}\n\n"
                return

            # Send initial status
            yield f"data: {json.dumps({'type': 'progress', 'data': {'step': 1, 'step_name': 'CDISC Library Check', 'current': 0, 'total': len(pending), 'message': 'Starting audit...'}})}\n\n"
            await asyncio.sleep(0.1)

            # Run CDISC check first (fast)
            cdisc_approved = 0
            needs_llm = []
            user = current_user.get('sub', 'audit')

            for i, var in enumerate(pending):
                match_result = library.match_variable(
                    domain=var.get('domain', ''),
                    name=var.get('name', ''),
                    label=var.get('label', ''),
                    data_type=var.get('data_type', '')
                )

                if match_result.matched and match_result.confidence >= 85:
                    cdisc_approved += 1
                    # Apply approval immediately
                    store.approve_variable(
                        domain=var['domain'],
                        name=var['name'],
                        user=user,
                        comment=f"CDISC auto-approved: {match_result.reason} ({match_result.confidence}%)"
                    )
                else:
                    needs_llm.append(var)

                # Progress update every 100 variables
                if (i + 1) % 100 == 0 or (i + 1) == len(pending):
                    yield f"data: {json.dumps({'type': 'progress', 'data': {'step': 1, 'step_name': 'CDISC Library Check', 'current': i + 1, 'total': len(pending), 'message': f'Checked {i + 1}/{len(pending)} variables', 'cdisc_approved': cdisc_approved, 'needs_llm': len(needs_llm)}})}\n\n"
                    await asyncio.sleep(0.01)

            # Save CDISC approvals
            store.save(user=user, comment=f"CDISC audit: {cdisc_approved} variables auto-approved")

            # Step 2: LLM Analysis (or skip to manual review)
            llm_approved = 0
            quick_review = 0
            manual_review = 0

            if needs_llm:
                if do_skip_llm:
                    # Skip LLM - mark all non-CDISC as manual review
                    manual_review = len(needs_llm)
                    yield f"data: {json.dumps({'type': 'progress', 'data': {'step': 2, 'step_name': 'Skipping LLM', 'current': len(needs_llm), 'total': len(needs_llm), 'message': f'Skipped LLM - {len(needs_llm)} variables marked for manual review', 'cdisc_approved': cdisc_approved, 'manual_review': manual_review}})}\n\n"
                    await asyncio.sleep(0.1)
                else:
                    # Run LLM analysis
                    yield f"data: {json.dumps({'type': 'progress', 'data': {'step': 2, 'step_name': 'LLM Analysis', 'current': 0, 'total': len(needs_llm), 'message': 'Starting LLM analysis...', 'cdisc_approved': cdisc_approved}})}\n\n"
                    await asyncio.sleep(0.1)

                    # Process in batches of 10
                    batch_size = 10
                    for batch_start in range(0, len(needs_llm), batch_size):
                        batch_end = min(batch_start + batch_size, len(needs_llm))
                        batch = needs_llm[batch_start:batch_end]

                        # Analyze batch with LLM
                        try:
                            batch_decisions = engine._analyze_batch_with_llm(
                                [(i, var) for i, var in enumerate(batch)]
                            )

                            for var, decision in zip(batch, batch_decisions):
                                if decision.decision == 'auto_approved':
                                    llm_approved += 1
                                    store.approve_variable(
                                        domain=var['domain'],
                                        name=var['name'],
                                        user=user,
                                        comment=f"LLM auto-approved: {decision.reason} ({decision.confidence}%)"
                                    )
                                elif decision.decision == 'quick_review':
                                    quick_review += 1
                                else:
                                    manual_review += 1

                        except Exception as e:
                            # LLM failed for this batch, mark all as manual review
                            manual_review += len(batch)

                        yield f"data: {json.dumps({'type': 'progress', 'data': {'step': 2, 'step_name': 'LLM Analysis', 'current': batch_end, 'total': len(needs_llm), 'message': f'Analyzed {batch_end}/{len(needs_llm)} variables', 'cdisc_approved': cdisc_approved, 'llm_approved': llm_approved, 'quick_review': quick_review, 'manual_review': manual_review}})}\n\n"
                        await asyncio.sleep(0.01)

                    # Save LLM approvals
                    if llm_approved > 0:
                        store.save(user=user, comment=f"LLM audit: {llm_approved} variables auto-approved")

            # Step 3: Complete
            total_approved = cdisc_approved + llm_approved
            result = {
                'type': 'complete',
                'data': {
                    'message': 'Audit complete!',
                    'total_variables': len(pending),
                    'cdisc_approved': cdisc_approved,
                    'llm_approved': llm_approved,
                    'total_approved': total_approved,
                    'quick_review': quick_review,
                    'manual_review': manual_review,
                    'approval_rate': round(100 * total_approved / len(pending), 1) if pending else 0
                }
            }
            yield f"data: {json.dumps(result)}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'data': {'message': str(e)}})}\n\n"

    return StreamingResponse(
        generate_events(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@router.get("/audit/pending-count")
async def get_pending_count(
    current_user: dict = Depends(get_current_user)
):
    """Get count of pending variables for audit."""
    store = get_metadata_store()
    if not store:
        return {"success": True, "data": {"pending": 0}}

    pending = 0
    for domain in store.get_all_domains():
        for var in domain.variables:
            if var.approval.status == 'pending':
                pending += 1

    return {
        "success": True,
        "data": {"pending": pending},
        "meta": {"timestamp": datetime.now().isoformat()}
    }
