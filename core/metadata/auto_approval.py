"""
Auto-Approval Engine for Metadata Variables

Automatically approves CDISC-standard variables and uses LLM for non-standard ones.
Uses a tiered approach:
1. CDISC Library match → Auto-approve (instant)
2. LLM batch analysis → Categorize remaining variables
3. Manual review → For truly ambiguous cases
"""

import logging
import re
import json
import httpx
from dataclasses import dataclass, asdict, field
from typing import Optional, List, Dict, Any, Tuple, Callable
from datetime import datetime

from .cdisc_library import CDISCLibrary, MatchResult

logger = logging.getLogger(__name__)


@dataclass
class ApprovalDecision:
    """Result of auto-approval analysis for a variable."""
    variable_name: str
    domain: str
    decision: str  # 'auto_approved', 'quick_review', 'manual_review'
    confidence: int  # 0-100
    match_type: str  # 'cdisc_exact', 'cdisc_pattern', 'llm_approved', 'llm_review', 'unknown'
    reason: str
    cdisc_match: Optional[Dict] = None
    llm_analysis: Optional[Dict] = None
    approved_at: Optional[str] = None
    approved_by: str = "auto_approval_engine"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AuditProgress:
    """Progress update during audit."""
    step: int  # 1=CDISC, 2=LLM, 3=Complete
    step_name: str
    current: int
    total: int
    message: str
    cdisc_approved: int = 0
    llm_approved: int = 0
    quick_review: int = 0
    manual_review: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AuditResult:
    """Final result of the audit process."""
    total_variables: int
    cdisc_approved: int
    llm_approved: int
    quick_review: int
    manual_review: int
    processing_time_seconds: float
    decisions: List[ApprovalDecision] = field(default_factory=list)

    def to_dict(self) -> dict:
        result = asdict(self)
        result['decisions'] = [d.to_dict() for d in self.decisions]
        return result


class AutoApprovalEngine:
    """
    Automatic approval engine for metadata variables.

    Two-step process:
    1. CDISC Library check - instant matching against standards
    2. LLM analysis - batch processing for non-standard variables
    """

    # Confidence thresholds
    CDISC_AUTO_APPROVE_THRESHOLD = 85
    LLM_AUTO_APPROVE_THRESHOLD = 80
    QUICK_REVIEW_THRESHOLD = 60

    # LLM batch size
    LLM_BATCH_SIZE = 10

    def __init__(
        self,
        cdisc_library: CDISCLibrary,
        ollama_host: str = "http://ollama:11434",
        llm_model: str = "deepseek-r1:8b"
    ):
        """
        Initialize the auto-approval engine.

        Args:
            cdisc_library: CDISCLibrary instance for standard matching
            ollama_host: Ollama API host URL
            llm_model: Model to use for LLM analysis
        """
        self.cdisc_library = cdisc_library
        self.ollama_host = ollama_host
        self.llm_model = llm_model

    def run_audit(
        self,
        variables: List[Dict[str, Any]],
        progress_callback: Optional[Callable[[AuditProgress], None]] = None
    ) -> AuditResult:
        """
        Run the full audit process on pending variables.

        Args:
            variables: List of variable dicts with domain, name, label, etc.
            progress_callback: Optional callback for progress updates

        Returns:
            AuditResult with all decisions and summary
        """
        import time
        start_time = time.time()

        total = len(variables)
        decisions: List[ApprovalDecision] = []
        cdisc_approved = 0
        llm_approved = 0
        quick_review = 0
        manual_review = 0

        # Variables that need LLM analysis
        needs_llm: List[Tuple[int, Dict[str, Any]]] = []

        # ==========================================
        # STEP 1: CDISC Library Check
        # ==========================================
        if progress_callback:
            progress_callback(AuditProgress(
                step=1,
                step_name="CDISC Library Check",
                current=0,
                total=total,
                message="Starting CDISC standards matching..."
            ))

        for i, var in enumerate(variables):
            match_result = self.cdisc_library.match_variable(
                domain=var.get('domain', ''),
                name=var.get('name', ''),
                label=var.get('label', ''),
                data_type=var.get('data_type', '')
            )

            if match_result.matched and match_result.confidence >= self.CDISC_AUTO_APPROVE_THRESHOLD:
                # CDISC match - auto approve
                decision = ApprovalDecision(
                    variable_name=var.get('name', ''),
                    domain=var.get('domain', ''),
                    decision='auto_approved',
                    confidence=match_result.confidence,
                    match_type=f'cdisc_{match_result.match_type}',
                    reason=match_result.reason,
                    cdisc_match=match_result.to_dict(),
                    approved_at=datetime.now().isoformat()
                )
                decisions.append(decision)
                cdisc_approved += 1
            else:
                # Needs LLM analysis
                needs_llm.append((i, var))
                # Placeholder decision - will be updated by LLM
                decisions.append(None)

            # Progress update every 100 variables
            if progress_callback and (i + 1) % 100 == 0:
                progress_callback(AuditProgress(
                    step=1,
                    step_name="CDISC Library Check",
                    current=i + 1,
                    total=total,
                    message=f"Checked {i + 1}/{total} variables",
                    cdisc_approved=cdisc_approved
                ))

        # Final CDISC step update
        if progress_callback:
            progress_callback(AuditProgress(
                step=1,
                step_name="CDISC Library Check",
                current=total,
                total=total,
                message=f"CDISC check complete: {cdisc_approved} auto-approved, {len(needs_llm)} need LLM analysis",
                cdisc_approved=cdisc_approved
            ))

        # ==========================================
        # STEP 2: LLM Analysis for remaining
        # ==========================================
        if needs_llm:
            if progress_callback:
                progress_callback(AuditProgress(
                    step=2,
                    step_name="LLM Analysis",
                    current=0,
                    total=len(needs_llm),
                    message="Starting LLM analysis for non-CDISC variables...",
                    cdisc_approved=cdisc_approved
                ))

            # Process in batches
            for batch_start in range(0, len(needs_llm), self.LLM_BATCH_SIZE):
                batch_end = min(batch_start + self.LLM_BATCH_SIZE, len(needs_llm))
                batch = needs_llm[batch_start:batch_end]

                # Analyze batch with LLM
                batch_decisions = self._analyze_batch_with_llm(batch)

                # Update decisions and counts
                for (orig_idx, var), decision in zip(batch, batch_decisions):
                    decisions[orig_idx] = decision
                    if decision.decision == 'auto_approved':
                        llm_approved += 1
                    elif decision.decision == 'quick_review':
                        quick_review += 1
                    else:
                        manual_review += 1

                if progress_callback:
                    progress_callback(AuditProgress(
                        step=2,
                        step_name="LLM Analysis",
                        current=batch_end,
                        total=len(needs_llm),
                        message=f"Analyzed {batch_end}/{len(needs_llm)} variables with LLM",
                        cdisc_approved=cdisc_approved,
                        llm_approved=llm_approved,
                        quick_review=quick_review,
                        manual_review=manual_review
                    ))

        # ==========================================
        # STEP 3: Complete
        # ==========================================
        processing_time = time.time() - start_time

        if progress_callback:
            progress_callback(AuditProgress(
                step=3,
                step_name="Complete",
                current=total,
                total=total,
                message="Audit complete!",
                cdisc_approved=cdisc_approved,
                llm_approved=llm_approved,
                quick_review=quick_review,
                manual_review=manual_review
            ))

        return AuditResult(
            total_variables=total,
            cdisc_approved=cdisc_approved,
            llm_approved=llm_approved,
            quick_review=quick_review,
            manual_review=manual_review,
            processing_time_seconds=round(processing_time, 2),
            decisions=[d for d in decisions if d is not None]
        )

    def _analyze_batch_with_llm(
        self,
        batch: List[Tuple[int, Dict[str, Any]]]
    ) -> List[ApprovalDecision]:
        """
        Analyze a batch of variables using LLM.

        Args:
            batch: List of (index, variable_dict) tuples

        Returns:
            List of ApprovalDecision for each variable
        """
        variables_info = []
        for idx, var in batch:
            variables_info.append({
                'domain': var.get('domain', ''),
                'name': var.get('name', ''),
                'label': var.get('label', ''),
                'type': var.get('data_type', ''),
                'derivation': var.get('derivation', '')[:200] if var.get('derivation') else ''
            })

        # Build prompt for batch analysis
        prompt = self._build_llm_prompt(variables_info)

        try:
            # Call Ollama
            response = self._call_ollama(prompt)

            # Parse LLM response
            llm_decisions = self._parse_llm_response(response, variables_info)

            # Build ApprovalDecision objects
            decisions = []
            for i, (idx, var) in enumerate(batch):
                if i < len(llm_decisions):
                    llm_dec = llm_decisions[i]
                    decision = ApprovalDecision(
                        variable_name=var.get('name', ''),
                        domain=var.get('domain', ''),
                        decision=llm_dec.get('decision', 'manual_review'),
                        confidence=llm_dec.get('confidence', 50),
                        match_type='llm_approved' if llm_dec.get('decision') == 'auto_approved' else 'llm_review',
                        reason=llm_dec.get('reason', 'LLM analysis'),
                        llm_analysis=llm_dec,
                        approved_at=datetime.now().isoformat() if llm_dec.get('decision') == 'auto_approved' else None
                    )
                else:
                    # Fallback if LLM didn't return enough decisions
                    decision = ApprovalDecision(
                        variable_name=var.get('name', ''),
                        domain=var.get('domain', ''),
                        decision='manual_review',
                        confidence=50,
                        match_type='llm_error',
                        reason='LLM analysis incomplete - manual review required'
                    )
                decisions.append(decision)

            return decisions

        except Exception as e:
            logger.error(f"LLM analysis failed: {e}")
            # Return manual review for all on error
            return [
                ApprovalDecision(
                    variable_name=var.get('name', ''),
                    domain=var.get('domain', ''),
                    decision='manual_review',
                    confidence=50,
                    match_type='llm_error',
                    reason=f'LLM analysis failed: {str(e)}'
                )
                for idx, var in batch
            ]

    def _build_llm_prompt(self, variables: List[Dict]) -> str:
        """Build the prompt for LLM batch analysis."""
        var_list = "\n".join([
            f"{i+1}. {v['domain']}.{v['name']} - \"{v['label']}\" ({v['type']})"
            + (f" | Derivation: {v['derivation']}" if v['derivation'] else "")
            for i, v in enumerate(variables)
        ])

        return f"""You are a clinical data standards expert reviewing ADaM/SDTM variables.

Analyze each variable and classify as:
- STANDARD: Follows CDISC conventions, clear derivation, can be auto-approved
- REVIEW: Mostly standard but needs quick human verification
- CUSTOM: Study-specific or ambiguous, needs manual review

Variables to analyze:
{var_list}

Respond with a JSON array. For each variable:
{{"name": "VARNAME", "decision": "STANDARD|REVIEW|CUSTOM", "confidence": 50-100, "reason": "brief explanation"}}

Example response:
[
  {{"name": "TRTSDT", "decision": "STANDARD", "confidence": 95, "reason": "Standard treatment start date variable"}},
  {{"name": "CUSTOMFL", "decision": "CUSTOM", "confidence": 40, "reason": "Study-specific flag, needs documentation review"}}
]

Respond with ONLY the JSON array, no other text."""

    def _call_ollama(self, prompt: str, timeout: float = 120.0) -> str:
        """Call Ollama API and return response text."""
        try:
            with httpx.Client(timeout=timeout) as client:
                response = client.post(
                    f"{self.ollama_host}/api/generate",
                    json={
                        "model": self.llm_model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {
                            "temperature": 0.1,
                            "num_predict": 2000
                        }
                    }
                )
                response.raise_for_status()
                return response.json().get("response", "")
        except Exception as e:
            logger.error(f"Ollama API call failed: {e}")
            raise

    def _parse_llm_response(
        self,
        response: str,
        variables: List[Dict]
    ) -> List[Dict]:
        """Parse LLM response into decision dicts."""
        decisions = []

        # Try to extract JSON from response
        try:
            # Find JSON array in response
            json_match = re.search(r'\[[\s\S]*\]', response)
            if json_match:
                parsed = json.loads(json_match.group())
                if isinstance(parsed, list):
                    for item in parsed:
                        decision_str = item.get('decision', 'CUSTOM').upper()
                        if decision_str == 'STANDARD':
                            decision = 'auto_approved'
                        elif decision_str == 'REVIEW':
                            decision = 'quick_review'
                        else:
                            decision = 'manual_review'

                        decisions.append({
                            'decision': decision,
                            'confidence': item.get('confidence', 70),
                            'reason': item.get('reason', 'LLM analysis')
                        })
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse LLM JSON response: {e}")

        # Pad with manual review if not enough decisions
        while len(decisions) < len(variables):
            decisions.append({
                'decision': 'manual_review',
                'confidence': 50,
                'reason': 'LLM response parsing incomplete'
            })

        return decisions


def run_audit(
    metadata_store,
    cdisc_library: CDISCLibrary,
    ollama_host: str = "http://ollama:11434",
    llm_model: str = "deepseek-r1:8b",
    user: str = "auto_approval_engine",
    progress_callback: Optional[Callable[[AuditProgress], None]] = None,
    apply_approvals: bool = True
) -> AuditResult:
    """
    Run audit on all pending variables in the metadata store.

    Args:
        metadata_store: MetadataStore instance
        cdisc_library: CDISCLibrary for standard matching
        ollama_host: Ollama API host
        llm_model: Model for LLM analysis
        user: Username for audit trail
        progress_callback: Optional progress callback
        apply_approvals: Whether to apply approvals to store

    Returns:
        AuditResult with all decisions
    """
    engine = AutoApprovalEngine(cdisc_library, ollama_host, llm_model)

    # Get all pending variables
    pending = []
    for domain in metadata_store.get_all_domains():
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
        return AuditResult(
            total_variables=0,
            cdisc_approved=0,
            llm_approved=0,
            quick_review=0,
            manual_review=0,
            processing_time_seconds=0,
            decisions=[]
        )

    # Run audit
    result = engine.run_audit(pending, progress_callback)

    # Apply approvals if requested
    if apply_approvals:
        for decision in result.decisions:
            if decision.decision == 'auto_approved':
                metadata_store.approve_variable(
                    domain=decision.domain,
                    name=decision.variable_name,
                    user=user,
                    comment=f"Auto-approved ({decision.match_type}): {decision.reason}"
                )

        # Save changes
        metadata_store.save(
            user=user,
            comment=f"Audit complete: {result.cdisc_approved} CDISC + {result.llm_approved} LLM approved"
        )

    return result
