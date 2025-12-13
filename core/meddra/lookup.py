# SAGE MedDRA Lookup
# ===================
"""
MedDRA term lookup and search functionality.

Provides:
- Exact term matching
- Fuzzy term search
- Hierarchy navigation
- Abbreviation resolution (MI -> Myocardial infarction)
"""

import logging
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, asdict

import duckdb

from .loader import MedDRATerm, MedDRAHierarchy

logger = logging.getLogger(__name__)

# Common medical abbreviations
ABBREVIATIONS = {
    "MI": ["MYOCARDIAL INFARCTION"],
    "HTN": ["HYPERTENSION"],
    "DM": ["DIABETES MELLITUS"],
    "CHF": ["CARDIAC FAILURE", "CONGESTIVE HEART FAILURE"],
    "COPD": ["CHRONIC OBSTRUCTIVE PULMONARY DISEASE"],
    "DVT": ["DEEP VEIN THROMBOSIS"],
    "PE": ["PULMONARY EMBOLISM"],
    "CVA": ["CEREBROVASCULAR ACCIDENT", "STROKE"],
    "TIA": ["TRANSIENT ISCHAEMIC ATTACK"],
    "UTI": ["URINARY TRACT INFECTION"],
    "URI": ["UPPER RESPIRATORY TRACT INFECTION"],
    "SOB": ["DYSPNOEA", "SHORTNESS OF BREATH"],
    "N/V": ["NAUSEA", "VOMITING"],
    "HA": ["HEADACHE"],
    "CP": ["CHEST PAIN"],
    "AF": ["ATRIAL FIBRILLATION"],
    "VT": ["VENTRICULAR TACHYCARDIA"],
    "VF": ["VENTRICULAR FIBRILLATION"],
    "GI": ["GASTROINTESTINAL"],
    "AKI": ["ACUTE KIDNEY INJURY"],
    "CKD": ["CHRONIC KIDNEY DISEASE"],
    "GERD": ["GASTRO-OESOPHAGEAL REFLUX DISEASE"],
}


@dataclass
class SearchResult:
    """Search result with term and hierarchy."""
    term: MedDRATerm
    match_score: float
    hierarchy: MedDRAHierarchy


@dataclass
class LookupResult:
    """Result of term lookup."""
    found: bool
    query: str
    exact_match: Optional[MedDRATerm]
    hierarchy: Optional[MedDRAHierarchy]
    related_terms: List[MedDRATerm]
    message: str


class MedDRALookup:
    """MedDRA term lookup and search."""

    def __init__(self, db_path: str):
        """
        Initialize MedDRA lookup.

        Args:
            db_path: Path to DuckDB database with MedDRA tables
        """
        self.db_path = Path(db_path)

    def _connect(self) -> duckdb.DuckDBPyConnection:
        """Get database connection."""
        # Don't use read_only=True as it conflicts with write connections during loading
        return duckdb.connect(str(self.db_path))

    def _normalize_code(self, code: str) -> str:
        """Normalize code by removing .0 suffix from float conversion."""
        if code and '.' in str(code):
            try:
                return str(int(float(code)))
            except (ValueError, TypeError):
                return str(code)
        return str(code) if code else code

    def lookup(self, term: str) -> LookupResult:
        """
        Look up a term in MedDRA.

        Handles:
        - Exact matches (case insensitive)
        - Common abbreviations (MI, HTN, etc.)
        - Returns hierarchy and related terms

        Args:
            term: Term to look up

        Returns:
            LookupResult with match info
        """
        term_upper = term.strip().upper()

        # Check abbreviations first
        if term_upper in ABBREVIATIONS:
            expanded_terms = ABBREVIATIONS[term_upper]
            for expanded in expanded_terms:
                result = self._lookup_exact(expanded)
                if result.found:
                    result.message = f"'{term}' resolved to '{expanded}' via abbreviation"
                    return result

        # Try exact match
        result = self._lookup_exact(term_upper)
        if result.found:
            return result

        # No exact match - get suggestions
        suggestions = self.search(term, limit=10)
        related = [s.term for s in suggestions]

        if related:
            return LookupResult(
                found=False,
                query=term,
                exact_match=None,
                hierarchy=None,
                related_terms=related,
                message=f"'{term}' not found in MedDRA. Did you mean one of these?"
            )
        else:
            return LookupResult(
                found=False,
                query=term,
                exact_match=None,
                hierarchy=None,
                related_terms=[],
                message=f"'{term}' not found in MedDRA and no similar terms found."
            )

    def _lookup_exact(self, term_upper: str) -> LookupResult:
        """Look up exact term match."""
        conn = self._connect()

        try:
            # Check PT first (most common)
            result = conn.execute("""
                SELECT code, name FROM meddra_pt
                WHERE name_upper = ?
            """, [term_upper]).fetchone()

            if result:
                pt = MedDRATerm(code=result[0], name=result[1], level="PT")
                hierarchy = self.get_hierarchy(result[0])
                return LookupResult(
                    found=True,
                    query=term_upper,
                    exact_match=pt,
                    hierarchy=hierarchy,
                    related_terms=[],
                    message=f"Exact match found: {pt.name}"
                )

            # Check LLT
            result = conn.execute("""
                SELECT code, name, pt_code FROM meddra_llt
                WHERE name_upper = ?
            """, [term_upper]).fetchone()

            if result:
                llt = MedDRATerm(
                    code=result[0],
                    name=result[1],
                    level="LLT",
                    parent_code=result[2]
                )
                hierarchy = self.get_hierarchy_for_llt(result[0])
                return LookupResult(
                    found=True,
                    query=term_upper,
                    exact_match=llt,
                    hierarchy=hierarchy,
                    related_terms=[],
                    message=f"Exact match found (LLT): {llt.name}"
                )

            return LookupResult(
                found=False,
                query=term_upper,
                exact_match=None,
                hierarchy=None,
                related_terms=[],
                message="No exact match found"
            )

        finally:
            conn.close()

    def search(
        self,
        query: str,
        level: Optional[str] = None,
        limit: int = 20
    ) -> List[SearchResult]:
        """
        Search for MedDRA terms.

        Args:
            query: Search query
            level: Filter by level (SOC, HLGT, HLT, PT, LLT)
            limit: Maximum results

        Returns:
            List of SearchResult
        """
        query_upper = query.strip().upper()
        results = []

        conn = self._connect()

        try:
            # Search PT level
            if level is None or level == "PT":
                pts = conn.execute("""
                    SELECT code, name,
                        CASE
                            WHEN name_upper = ? THEN 100
                            WHEN name_upper LIKE ? THEN 90
                            WHEN name_upper LIKE ? THEN 80
                            ELSE 70
                        END as score
                    FROM meddra_pt
                    WHERE name_upper LIKE ?
                    ORDER BY score DESC, name
                    LIMIT ?
                """, [query_upper, f"{query_upper}%", f"% {query_upper}%", f"%{query_upper}%", limit]).fetchall()

                for row in pts:
                    term = MedDRATerm(code=row[0], name=row[1], level="PT")
                    hierarchy = self.get_hierarchy(row[0])
                    results.append(SearchResult(
                        term=term,
                        match_score=row[2],
                        hierarchy=hierarchy
                    ))

            # Search LLT level
            if level is None or level == "LLT":
                llts = conn.execute("""
                    SELECT code, name, pt_code,
                        CASE
                            WHEN name_upper = ? THEN 100
                            WHEN name_upper LIKE ? THEN 90
                            WHEN name_upper LIKE ? THEN 80
                            ELSE 70
                        END as score
                    FROM meddra_llt
                    WHERE name_upper LIKE ?
                    ORDER BY score DESC, name
                    LIMIT ?
                """, [query_upper, f"{query_upper}%", f"% {query_upper}%", f"%{query_upper}%", limit]).fetchall()

                for row in llts:
                    term = MedDRATerm(code=row[0], name=row[1], level="LLT", parent_code=row[2])
                    hierarchy = self.get_hierarchy_for_llt(row[0])
                    results.append(SearchResult(
                        term=term,
                        match_score=row[3],
                        hierarchy=hierarchy
                    ))

            # Search SOC level
            if level == "SOC":
                socs = conn.execute("""
                    SELECT code, name FROM meddra_soc
                    WHERE UPPER(name) LIKE ?
                    LIMIT ?
                """, [f"%{query_upper}%", limit]).fetchall()

                for row in socs:
                    term = MedDRATerm(code=row[0], name=row[1], level="SOC")
                    results.append(SearchResult(
                        term=term,
                        match_score=80,
                        hierarchy=MedDRAHierarchy(
                            soc=term,
                            hlgt=MedDRATerm(code="", name="", level="HLGT"),
                            hlt=MedDRATerm(code="", name="", level="HLT"),
                            pt=MedDRATerm(code="", name="", level="PT")
                        )
                    ))

            # Sort by score and limit
            results.sort(key=lambda x: x.match_score, reverse=True)
            return results[:limit]

        finally:
            conn.close()

    def get_hierarchy(self, pt_code: str) -> Optional[MedDRAHierarchy]:
        """Get full hierarchy for a PT code."""
        conn = self._connect()
        # Normalize code to handle .0 suffix
        pt_code = self._normalize_code(pt_code)

        try:
            result = conn.execute("""
                SELECT
                    soc_code, soc_name,
                    hlgt_code, hlgt_name,
                    hlt_code, hlt_name,
                    pt_code, pt_name
                FROM meddra_hierarchy
                WHERE pt_code = ?
                LIMIT 1
            """, [pt_code]).fetchone()

            if not result:
                return None

            return MedDRAHierarchy(
                soc=MedDRATerm(code=result[0], name=result[1], level="SOC"),
                hlgt=MedDRATerm(code=result[2], name=result[3], level="HLGT", parent_code=result[0]),
                hlt=MedDRATerm(code=result[4], name=result[5], level="HLT", parent_code=result[2]),
                pt=MedDRATerm(code=result[6], name=result[7], level="PT", parent_code=result[4])
            )

        finally:
            conn.close()

    def get_hierarchy_for_llt(self, llt_code: str) -> Optional[MedDRAHierarchy]:
        """Get full hierarchy for an LLT code."""
        conn = self._connect()
        # Normalize code to handle .0 suffix
        llt_code = self._normalize_code(llt_code)

        try:
            result = conn.execute("""
                SELECT
                    h.soc_code, h.soc_name,
                    h.hlgt_code, h.hlgt_name,
                    h.hlt_code, h.hlt_name,
                    h.pt_code, h.pt_name,
                    h.llt_code, h.llt_name
                FROM meddra_hierarchy h
                WHERE h.llt_code = ?
                LIMIT 1
            """, [llt_code]).fetchone()

            if not result:
                return None

            return MedDRAHierarchy(
                soc=MedDRATerm(code=result[0], name=result[1], level="SOC"),
                hlgt=MedDRATerm(code=result[2], name=result[3], level="HLGT", parent_code=result[0]),
                hlt=MedDRATerm(code=result[4], name=result[5], level="HLT", parent_code=result[2]),
                pt=MedDRATerm(code=result[6], name=result[7], level="PT", parent_code=result[4]),
                llt=MedDRATerm(code=result[8], name=result[9], level="LLT", parent_code=result[6])
            )

        finally:
            conn.close()

    def get_term_by_code(self, code: str) -> Tuple[Optional[MedDRATerm], Optional[MedDRAHierarchy]]:
        """Get term and hierarchy by code."""
        conn = self._connect()
        # Normalize code to handle .0 suffix
        code = self._normalize_code(code)

        try:
            # Check each level
            for table, level in [
                ("meddra_pt", "PT"),
                ("meddra_llt", "LLT"),
                ("meddra_soc", "SOC"),
                ("meddra_hlgt", "HLGT"),
                ("meddra_hlt", "HLT")
            ]:
                result = conn.execute(f"""
                    SELECT code, name FROM {table}
                    WHERE code = ?
                """, [code]).fetchone()

                if result:
                    term = MedDRATerm(code=result[0], name=result[1], level=level)

                    if level == "PT":
                        hierarchy = self.get_hierarchy(code)
                    elif level == "LLT":
                        hierarchy = self.get_hierarchy_for_llt(code)
                    else:
                        hierarchy = None

                    return term, hierarchy

            return None, None

        finally:
            conn.close()

    def get_children(self, code: str) -> Tuple[Optional[MedDRATerm], List[MedDRATerm]]:
        """Get children of a term."""
        conn = self._connect()
        # Normalize code to handle .0 suffix
        code = self._normalize_code(code)

        try:
            # Determine level and get children
            # Check SOC
            result = conn.execute("""
                SELECT code, name FROM meddra_soc WHERE code = ?
            """, [code]).fetchone()

            if result:
                parent = MedDRATerm(code=result[0], name=result[1], level="SOC")
                children = conn.execute("""
                    SELECT code, name FROM meddra_hlgt WHERE soc_code = ?
                """, [code]).fetchall()
                return parent, [MedDRATerm(code=c[0], name=c[1], level="HLGT") for c in children]

            # Check HLGT
            result = conn.execute("""
                SELECT code, name FROM meddra_hlgt WHERE code = ?
            """, [code]).fetchone()

            if result:
                parent = MedDRATerm(code=result[0], name=result[1], level="HLGT")
                children = conn.execute("""
                    SELECT code, name FROM meddra_hlt WHERE hlgt_code = ?
                """, [code]).fetchall()
                return parent, [MedDRATerm(code=c[0], name=c[1], level="HLT") for c in children]

            # Check HLT
            result = conn.execute("""
                SELECT code, name FROM meddra_hlt WHERE code = ?
            """, [code]).fetchone()

            if result:
                parent = MedDRATerm(code=result[0], name=result[1], level="HLT")
                children = conn.execute("""
                    SELECT code, name FROM meddra_pt WHERE hlt_code = ?
                """, [code]).fetchall()
                return parent, [MedDRATerm(code=c[0], name=c[1], level="PT") for c in children]

            # Check PT
            result = conn.execute("""
                SELECT code, name FROM meddra_pt WHERE code = ?
            """, [code]).fetchone()

            if result:
                parent = MedDRATerm(code=result[0], name=result[1], level="PT")
                children = conn.execute("""
                    SELECT code, name FROM meddra_llt WHERE pt_code = ?
                """, [code]).fetchall()
                return parent, [MedDRATerm(code=c[0], name=c[1], level="LLT") for c in children]

            return None, []

        finally:
            conn.close()

    def get_all_socs(self) -> List[MedDRATerm]:
        """Get all System Organ Classes."""
        conn = self._connect()

        try:
            results = conn.execute("""
                SELECT code, name, pt_count FROM meddra_soc
                ORDER BY name
            """).fetchall()

            return [MedDRATerm(code=r[0], name=r[1], level="SOC") for r in results]

        finally:
            conn.close()

    def get_statistics(self) -> Dict[str, Any]:
        """Get MedDRA statistics."""
        conn = self._connect()

        try:
            stats = {
                "soc": conn.execute("SELECT COUNT(*) FROM meddra_soc").fetchone()[0],
                "hlgt": conn.execute("SELECT COUNT(*) FROM meddra_hlgt").fetchone()[0],
                "hlt": conn.execute("SELECT COUNT(*) FROM meddra_hlt").fetchone()[0],
                "pt": conn.execute("SELECT COUNT(*) FROM meddra_pt").fetchone()[0],
                "llt": conn.execute("SELECT COUNT(*) FROM meddra_llt").fetchone()[0],
            }
            stats["total"] = sum(stats.values())

            # Top SOCs by PT count
            top_socs = conn.execute("""
                SELECT name, pt_count FROM meddra_soc
                ORDER BY pt_count DESC
                LIMIT 10
            """).fetchall()

            return {
                "term_counts": stats,
                "top_socs": [{"name": s[0], "pt_count": s[1]} for s in top_socs]
            }

        finally:
            conn.close()

    def get_pts_by_soc(self, soc_code: str) -> Tuple[Optional[MedDRATerm], List[MedDRATerm]]:
        """
        Get all Preferred Terms (PTs) under a System Organ Class.
        Skips HLGT and HLT levels for simplified browsing.
        """
        conn = self._connect()
        soc_code = self._normalize_code(soc_code)

        try:
            # Get SOC info
            soc_result = conn.execute("""
                SELECT code, name FROM meddra_soc WHERE code = ?
            """, [soc_code]).fetchone()

            if not soc_result:
                return None, []

            soc = MedDRATerm(code=soc_result[0], name=soc_result[1], level="SOC")

            # Get all PTs under this SOC from hierarchy table
            pts = conn.execute("""
                SELECT DISTINCT pt_code, pt_name
                FROM meddra_hierarchy
                WHERE soc_code = ?
                ORDER BY pt_name
            """, [soc_code]).fetchall()

            pt_terms = [MedDRATerm(code=p[0], name=p[1], level="PT") for p in pts]

            return soc, pt_terms

        finally:
            conn.close()

    def get_llts_by_pt(self, pt_code: str) -> Tuple[Optional[MedDRATerm], List[MedDRATerm]]:
        """
        Get all Lowest Level Terms (LLTs) under a Preferred Term.
        """
        conn = self._connect()
        pt_code = self._normalize_code(pt_code)

        try:
            # Get PT info
            pt_result = conn.execute("""
                SELECT code, name FROM meddra_pt WHERE code = ?
            """, [pt_code]).fetchone()

            if not pt_result:
                return None, []

            pt = MedDRATerm(code=pt_result[0], name=pt_result[1], level="PT")

            # Get all LLTs under this PT
            llts = conn.execute("""
                SELECT code, name
                FROM meddra_llt
                WHERE pt_code = ?
                ORDER BY name
            """, [pt_code]).fetchall()

            llt_terms = [MedDRATerm(code=l[0], name=l[1], level="LLT") for l in llts]

            return pt, llt_terms

        finally:
            conn.close()
