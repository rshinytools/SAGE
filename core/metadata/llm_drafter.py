# SAGE - LLM Drafter Module
# ==========================
# Generates plain-English descriptions for complex derivations using LLM
"""
LLM-powered description generator for metadata.

Features:
- Generate plain-English descriptions for complex derivations
- Explain codelist usage in context
- Summarize variable purposes
- Support for Claude (Anthropic API) and Mock backends
"""

import logging
import os
import json
import re
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


@dataclass
class DraftRequest:
    """Request for LLM drafting."""
    variable_name: str
    domain: str
    label: str
    derivation: Optional[str] = None
    description: Optional[str] = None
    codelist: Optional[str] = None
    codelist_values: List[Dict[str, str]] = field(default_factory=list)
    data_type: str = "Char"
    context: Optional[str] = None  # Additional context


@dataclass
class DraftResult:
    """Result of LLM drafting."""
    variable_name: str
    domain: str
    plain_english: str
    confidence: float  # 0.0 to 1.0
    model_used: str
    generated_at: str = ""
    tokens_used: int = 0
    error: Optional[str] = None

    def __post_init__(self):
        if not self.generated_at:
            self.generated_at = datetime.now().isoformat()


class LLMBackend(ABC):
    """Abstract base class for LLM backends."""

    @abstractmethod
    def generate(self, prompt: str, system_prompt: str = "") -> tuple[str, int]:
        """Generate text from prompt. Returns (text, token_count)."""
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """Check if the backend is available."""
        pass


class ClaudeBackend(LLMBackend):
    """Claude (Anthropic API) LLM backend."""

    def __init__(
        self,
        model: str = "claude-sonnet-4-20250514"
    ):
        self.model = model
        self._client = None
        self._available = None

    def _get_client(self):
        """Get or create Anthropic client."""
        if self._client is None:
            try:
                import anthropic
                api_key = os.getenv("ANTHROPIC_API_KEY")
                if api_key:
                    self._client = anthropic.Anthropic(api_key=api_key)
            except ImportError:
                logger.warning("anthropic package not installed")
        return self._client

    def is_available(self) -> bool:
        """Check if Claude API is available."""
        if self._available is not None:
            return self._available

        client = self._get_client()
        self._available = client is not None
        return self._available

    def generate(self, prompt: str, system_prompt: str = "") -> tuple[str, int]:
        """Generate using Claude API."""
        client = self._get_client()
        if not client:
            raise RuntimeError("Claude API not available")

        try:
            response = client.messages.create(
                model=self.model,
                max_tokens=500,
                system=system_prompt if system_prompt else None,
                messages=[{"role": "user", "content": prompt}]
            )
            text = response.content[0].text
            tokens = response.usage.input_tokens + response.usage.output_tokens
            return text.strip(), tokens
        except Exception as e:
            raise RuntimeError(f"Claude request failed: {e}")


class MockBackend(LLMBackend):
    """Mock backend for testing when no LLM is available."""

    def is_available(self) -> bool:
        return True

    def generate(self, prompt: str, system_prompt: str = "") -> tuple[str, int]:
        """Generate mock response based on input."""
        # Extract variable info from prompt for basic response
        if "derivation" in prompt.lower():
            return "This variable is calculated based on the specified derivation logic.", 50
        elif "codelist" in prompt.lower():
            return "This variable uses controlled terminology from the associated codelist.", 45
        else:
            return "This variable contains clinical study data as specified in the label.", 40


class LLMDrafter:
    """
    Generate plain-English descriptions for metadata using LLM.

    Example:
        drafter = LLMDrafter()

        # Check if LLM is available
        if drafter.is_available():
            result = drafter.draft_description(DraftRequest(
                variable_name="TRTSDT",
                domain="ADSL",
                label="Date of First Exposure to Treatment",
                derivation="Earliest EXSTDTC from EX domain for the subject"
            ))
            print(result.plain_english)
    """

    SYSTEM_PROMPT = """You are a clinical data standards expert helping to document
CDISC SDTM and ADaM datasets. Your task is to generate clear, plain-English descriptions
of clinical trial variables that can be understood by users who may not be familiar
with the technical derivation logic.

Guidelines:
- Write in clear, concise language
- Explain what the variable represents, not just how it's derived
- Use active voice and present tense
- Avoid jargon unless necessary, and explain it if used
- Keep descriptions to 1-3 sentences
- Focus on the purpose and meaning of the data"""

    def __init__(
        self,
        backend: Optional[LLMBackend] = None,
        model: str = "claude-sonnet-4-20250514"
    ):
        """
        Initialize the LLM drafter.

        Args:
            backend: LLM backend to use (defaults to Claude)
            model: Model name for Claude backend
        """
        if backend:
            self.backend = backend
        else:
            # Try Claude first, fall back to mock
            claude = ClaudeBackend(model=model)
            if claude.is_available():
                self.backend = claude
                logger.info(f"Using Claude backend with model {model}")
            else:
                self.backend = MockBackend()
                logger.warning("LLM not available, using mock backend")

    def is_available(self) -> bool:
        """Check if LLM backend is available."""
        return self.backend.is_available()

    def _build_prompt(self, request: DraftRequest) -> str:
        """Build the prompt for the LLM."""
        parts = [
            f"Generate a plain-English description for the following clinical variable:",
            f"",
            f"Domain: {request.domain}",
            f"Variable: {request.variable_name}",
            f"Label: {request.label}",
            f"Data Type: {request.data_type}",
        ]

        if request.description:
            parts.append(f"Description: {request.description}")

        if request.derivation:
            parts.append(f"Derivation/Algorithm: {request.derivation}")

        if request.codelist:
            parts.append(f"Codelist: {request.codelist}")
            if request.codelist_values:
                values = ", ".join([
                    f"{v['code']}={v.get('decode', v['code'])}"
                    for v in request.codelist_values[:10]  # Limit values
                ])
                parts.append(f"Values: {values}")
                if len(request.codelist_values) > 10:
                    parts.append(f"  ... and {len(request.codelist_values) - 10} more values")

        if request.context:
            parts.append(f"")
            parts.append(f"Additional context: {request.context}")

        parts.extend([
            "",
            "Provide a clear, plain-English description that explains what this variable",
            "represents and how it should be interpreted. Keep it to 1-3 sentences."
        ])

        return "\n".join(parts)

    def draft_description(self, request: DraftRequest) -> DraftResult:
        """
        Generate a plain-English description for a variable.

        Args:
            request: DraftRequest with variable details

        Returns:
            DraftResult with generated description
        """
        prompt = self._build_prompt(request)

        try:
            text, tokens = self.backend.generate(prompt, self.SYSTEM_PROMPT)

            # Clean up response
            text = self._clean_response(text)

            # Estimate confidence based on response quality
            confidence = self._estimate_confidence(text, request)

            return DraftResult(
                variable_name=request.variable_name,
                domain=request.domain,
                plain_english=text,
                confidence=confidence,
                model_used=getattr(self.backend, 'model', 'mock'),
                tokens_used=tokens
            )

        except Exception as e:
            logger.error(f"LLM drafting failed: {e}")
            return DraftResult(
                variable_name=request.variable_name,
                domain=request.domain,
                plain_english="",
                confidence=0.0,
                model_used=getattr(self.backend, 'model', 'mock'),
                error=str(e)
            )

    def _clean_response(self, text: str) -> str:
        """Clean up LLM response."""
        # Remove common prefixes
        prefixes = [
            "Here is", "Here's", "The variable", "This variable:",
            "Description:", "Plain English:", "Answer:"
        ]
        for prefix in prefixes:
            if text.lower().startswith(prefix.lower()):
                text = text[len(prefix):].strip()
                if text.startswith(":"):
                    text = text[1:].strip()

        # Remove markdown formatting
        text = re.sub(r'\*\*(.*?)\*\*', r'\1', text)
        text = re.sub(r'\*(.*?)\*', r'\1', text)

        # Remove quotes if wrapping entire text
        if text.startswith('"') and text.endswith('"'):
            text = text[1:-1]

        # Ensure ends with period
        text = text.strip()
        if text and not text.endswith(('.', '!', '?')):
            text += '.'

        return text

    def _estimate_confidence(self, text: str, request: DraftRequest) -> float:
        """Estimate confidence in the generated description."""
        if not text:
            return 0.0

        confidence = 0.7  # Base confidence

        # Boost for reasonable length
        words = len(text.split())
        if 10 <= words <= 100:
            confidence += 0.1
        elif words < 5 or words > 200:
            confidence -= 0.2

        # Boost if mentions key terms
        key_terms = [
            request.variable_name.lower(),
            request.domain.lower(),
        ]
        for term in key_terms:
            if term in text.lower():
                confidence += 0.05

        # Reduce if seems like error message
        error_indicators = ['error', 'cannot', 'unable', 'sorry', 'apologize']
        for indicator in error_indicators:
            if indicator in text.lower():
                confidence -= 0.3
                break

        return max(0.0, min(1.0, confidence))

    def draft_batch(
        self,
        requests: List[DraftRequest],
        progress_callback: Optional[callable] = None
    ) -> List[DraftResult]:
        """
        Generate descriptions for multiple variables.

        Args:
            requests: List of DraftRequest objects
            progress_callback: Optional callback(current, total) for progress

        Returns:
            List of DraftResult objects
        """
        results = []
        total = len(requests)

        for i, request in enumerate(requests):
            if progress_callback:
                progress_callback(i, total)

            result = self.draft_description(request)
            results.append(result)

        if progress_callback:
            progress_callback(total, total)

        return results

    def draft_for_domain(
        self,
        domain_name: str,
        variables: List[Dict[str, Any]],
        derivations_only: bool = True
    ) -> List[DraftResult]:
        """
        Generate descriptions for all variables in a domain.

        Args:
            domain_name: Name of the domain
            variables: List of variable dictionaries
            derivations_only: Only draft for variables with derivations

        Returns:
            List of DraftResult objects
        """
        requests = []

        for var in variables:
            # Skip if no derivation and derivations_only is True
            if derivations_only and not var.get('derivation'):
                continue

            requests.append(DraftRequest(
                variable_name=var.get('name', ''),
                domain=domain_name,
                label=var.get('label', ''),
                derivation=var.get('derivation'),
                description=var.get('description'),
                codelist=var.get('codelist'),
                codelist_values=var.get('codelist_values', []),
                data_type=var.get('data_type', 'Char')
            ))

        return self.draft_batch(requests)


class TemplateDrafter:
    """
    Template-based description generator for when LLM is not available.

    Uses predefined templates based on variable characteristics.
    """

    TEMPLATES = {
        # Identifier variables
        'STUDYID': "Unique identifier for the clinical study.",
        'USUBJID': "Unique subject identifier across all studies.",
        'SUBJID': "Subject identifier unique within the study.",
        'SITEID': "Identifier for the study site where the subject was enrolled.",

        # Date variables
        'RFSTDTC': "Reference start date for the subject, typically the date of first dose.",
        'RFENDTC': "Reference end date for the subject, typically the date of last dose.",
        'DMDTC': "Date of collection of demographic data.",

        # Common timing variables
        'TRTSDT': "Date when the subject first received study treatment.",
        'TRTEDT': "Date when the subject last received study treatment.",
        'TRTSDTM': "Date and time when the subject first received study treatment.",
        'TRTEDTM': "Date and time when the subject last received study treatment.",

        # Population flags
        'SAFFL': "Flag indicating whether the subject is part of the Safety Population.",
        'ITTFL': "Flag indicating whether the subject is part of the Intent-to-Treat Population.",
        'EFFFL': "Flag indicating whether the subject is part of the Efficacy Population.",

        # Common analysis variables
        'AVAL': "Analysis value, the numeric result for the analysis.",
        'AVALC': "Analysis value in character format.",
        'CHG': "Change from baseline value.",
        'PCHG': "Percent change from baseline value.",
        'BASE': "Baseline value for the analysis parameter.",
    }

    PATTERNS = [
        # Date patterns
        (r'.*DT$', "Date of {label}."),
        (r'.*DTM$', "Date and time of {label}."),
        (r'.*DTC$', "Date/time of {label} in ISO 8601 format."),

        # Flag patterns
        (r'.*FL$', "Flag indicating {label}. Values are typically Y or N."),
        (r'.*FN$', "Flag in numeric format indicating {label}. Values are typically 1 or 0."),

        # Sequence patterns
        (r'.*SEQ$', "Sequence number for {label}."),
        (r'.*SEQN$', "Sequence number for {label}."),

        # Numeric patterns
        (r'.*STRESN$', "Numeric result in standard units for {label}."),
        (r'.*STNRLO$', "Lower limit of normal range in standard units for {label}."),
        (r'.*STNRHI$', "Upper limit of normal range in standard units for {label}."),

        # Category patterns
        (r'.*CAT$', "Category for {label}."),
        (r'.*SCAT$', "Subcategory for {label}."),
        (r'.*GRP.*$', "Grouping variable for {label}."),
    ]

    def draft_description(self, request: DraftRequest) -> DraftResult:
        """Generate description using templates."""
        var_upper = request.variable_name.upper()

        # Check exact match templates
        if var_upper in self.TEMPLATES:
            text = self.TEMPLATES[var_upper]
        else:
            # Check pattern matches
            text = None
            for pattern, template in self.PATTERNS:
                if re.match(pattern, var_upper):
                    text = template.format(label=request.label.lower())
                    break

            if not text:
                # Default template
                text = f"{request.label}."

        return DraftResult(
            variable_name=request.variable_name,
            domain=request.domain,
            plain_english=text,
            confidence=0.5,  # Template-based has lower confidence
            model_used="template"
        )

    def draft_batch(
        self,
        requests: List[DraftRequest],
        progress_callback: Optional[callable] = None
    ) -> List[DraftResult]:
        """
        Generate descriptions for multiple variables using templates.

        Args:
            requests: List of DraftRequest objects
            progress_callback: Optional callback(current, total) for progress

        Returns:
            List of DraftResult objects
        """
        results = []
        total = len(requests)

        for i, request in enumerate(requests):
            if progress_callback:
                progress_callback(i, total)

            result = self.draft_description(request)
            results.append(result)

        if progress_callback:
            progress_callback(total, total)

        return results
