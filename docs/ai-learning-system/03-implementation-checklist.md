# AI Chat Learning System - Phase 3: Implementation Checklist

## Overview

This document provides the complete implementation checklist with detailed steps for each phase.

---

## Pre-Implementation Requirements

### Environment Verification

- [ ] Python 3.10+ installed
- [ ] ChromaDB available and working
- [ ] DuckDB databases accessible
- [ ] React frontend building correctly
- [ ] Docker services running

### Dependencies to Add

**Backend (`docker/api/requirements.txt`):**
```
chromadb>=0.4.0
sqlparse>=0.4.4
```

**Frontend (`docker/admin-ui-react/package.json`):**
```json
{
  "dependencies": {
    "@radix-ui/react-tooltip": "^1.0.7"
  }
}
```

---

## Phase 0: Enterprise Foundations (Week 0) ✅ COMPLETE

> **STATUS:** All components implemented and tested. 248 tests passing.
> See [00-enterprise-foundations.md](./00-enterprise-foundations.md) for full component code.

### 0.1 Clinical Protocol Guard ✅

- [x] Create directory: `core/engine/clinical/`
- [x] Create `core/engine/clinical/__init__.py`
- [x] Create `core/engine/clinical/protocol_guard.py` with:
  - [x] `AMBIGUOUS_TERMS` registry (high, low, recent, baseline, severe, etc.)
  - [x] `ProtocolGuard` class
  - [x] `check_query()` method
  - [x] `apply_user_clarification()` method
- [x] Create `knowledge/study_protocol.json` template with:
  - [x] `BASELINE_DEFINITION`
  - [x] `ENDPOINT_DEFINITION`
  - [x] `ELDERLY_DEFINITION`
  - [x] `HIGH_THRESHOLD` mappings
  - [x] `RESPONDER_DEFINITION`
- [x] Test with ambiguous queries (16 tests)

### 0.2 Certified Answer System ✅

- [x] Create `core/engine/clinical/certified_answer.py` with:
  - [x] `CertificationLevel` enum (CERTIFIED, VERIFIED, ASSISTED, MANUAL)
  - [x] `CertifiedAnswerSystem` class
  - [x] `check_certification()` - Check if query matches verified example
- [x] Define thresholds:
  - [x] CERTIFIED_THRESHOLD = 0.98 (bypass LLM)
  - [x] VERIFIED_THRESHOLD = 0.90 (high confidence)
  - [x] ASSISTED_THRESHOLD = 0.70 (use as example)

### 0.3 Schema Validation Layer ✅

- [x] Create `core/engine/clinical/schema_validator.py` with:
  - [x] `SchemaValidator` class
  - [x] `validate()` - Validate SQL against current schema
  - [x] `_extract_tables()` - Extract tables from SQL
  - [x] `_extract_columns()` - Extract columns from SQL
  - [x] `COLUMN_ALIASES` - Common column renames (VISIT→VISITNUM, etc.)
  - [x] `auto_repair()` - Attempt to fix schema issues
- [x] 24 tests for schema validation

### 0.4 Golden Views ✅

- [x] Create `database/golden_views.sql` with 8 views:
  - [x] `vw_ae_with_demographics` - AEs joined with demographics
  - [x] `vw_subject_summary` - Subject demographics + disposition
  - [x] `vw_lab_with_ranges` - Labs with reference ranges
  - [x] `vw_conmeds` - Concomitant medications
  - [x] `vw_vitals` - Vital signs
  - [x] `vw_exposure` - Study drug exposure
  - [x] `vw_efficacy_endpoints` - Efficacy analysis
  - [x] `vw_study_overview` - High-level study statistics
- [x] Create `core/engine/clinical/view_router.py` with:
  - [x] `GOLDEN_VIEWS` registry
  - [x] `GoldenViewRouter` class
  - [x] `route_query()` - Map query to appropriate view
  - [x] `get_view_prompt()` - Generate prompt for view usage
  - [x] `ViewRoutingResult` dataclass
- [x] 19 tests for view routing

### 0.5 Structured Audit Trace ✅

- [x] Create `core/engine/audit/` module with:
  - [x] `AuditTraceLogger` class
  - [x] `AuditEvent` enum (18 event types)
  - [x] `AuditLevel` enum
  - [x] `start_trace()` - Start audit trace
  - [x] `log()` - Log events to trace
  - [x] `complete_trace()` - Complete trace with outcome
  - [x] `get_trace()` - Retrieve trace by ID
  - [x] `export_trace()` - Export for regulatory audit (21 CFR Part 11)
  - [x] `verify_integrity()` - Verify trace checksums
- [x] 19 tests for audit logging

### 0.6 Helpful Refusal System ✅

- [x] Create `core/engine/clinical/helpful_refusal.py` with:
  - [x] `RefusalReason` enum (8 reason types)
  - [x] `HelpfulRefusal` dataclass
  - [x] `HelpfulRefusalSystem` class
  - [x] `generate_refusal()` - Create helpful refusal
  - [x] `format_refusal_message()` - Format for user
- [x] Implement refusal handlers for:
  - [x] `AMBIGUOUS_TERM` - Ask for clarification
  - [x] `LOW_SIMILARITY` - Suggest alternatives
  - [x] `SCHEMA_MISMATCH` - Suggest corrections
  - [x] `COMPLEX_QUERY` - Suggest breaking down
  - [x] `OUT_OF_SCOPE` - Explain capabilities
  - [x] `MISSING_DATA` - Suggest loading data
  - [x] `CONFIDENCE_TOO_LOW` - Explain low confidence
  - [x] `VALIDATION_FAILED` - Generic helpful refusal
- [x] 14 tests for helpful refusals

### 0.7 Learning Components ✅

- [x] Create `core/engine/learning/` module with:
  - [x] `ExampleStore` - Store and retrieve learning examples
  - [x] `ComplexityScorer` - Assess query complexity
  - [x] `SemanticValidator` - Validate SQL semantics
  - [x] `ResultValidator` - Validate query results
  - [x] `ConfidenceManager` - 8-component weighted confidence
  - [x] `FeedbackHandler` - Process user feedback
- [x] 122 tests for learning components

### 0.8 Enterprise Integration ✅

- [x] Create `core/engine/enterprise.py` with:
  - [x] `EnterpriseConfig` dataclass
  - [x] `EnterpriseResult` dataclass
  - [x] `EnterpriseProcessor` class
  - [x] `create_enterprise_processor()` factory function
- [x] Integration methods:
  - [x] `start_trace()` - Start audit trace
  - [x] `check_certified()` - Check for certified answers
  - [x] `check_protocol()` - Check for ambiguous terms
  - [x] `validate_schema()` - Validate SQL against schema
  - [x] `route_to_view()` - Route to golden views
  - [x] `assess_complexity()` - Assess query complexity
  - [x] `calculate_confidence()` - Calculate final confidence
  - [x] `generate_refusal()` - Generate helpful refusals
  - [x] `complete_trace()` - Complete audit trace
  - [x] `submit_feedback()` - Submit user feedback
- [x] 19 tests for enterprise integration

### Phase 0 Summary

| Module | Tests | Status |
|--------|-------|--------|
| Clinical (Protocol Guard, Certified Answers, Schema Validator, View Router, Helpful Refusal) | 73 | ✅ |
| Learning (Example Store, Complexity, Semantic, Result, Confidence, Feedback) | 122 | ✅ |
| Audit (Trace Logger) | 34 | ✅ |
| Enterprise Integration | 19 | ✅ |
| **Total** | **248** | **✅** |

---

## Phase 1: Foundation (Week 1) ✅ COMPLETE

> **STATUS:** Completed as part of Phase 0 enterprise foundation.

### 1.1 Create Learning Module Structure ✅

- [x] Create directory: `core/engine/learning/`
- [x] Create `core/engine/learning/__init__.py`

```python
# core/engine/learning/__init__.py
from .example_store import ExampleStore, LearningExample
from .complexity_scorer import ComplexityScorer, ComplexityLevel, ComplexityAssessment
from .semantic_validator import SemanticValidator, SemanticValidation, ValidationResult
from .result_validator import ResultValidator, ResultValidation
from .confidence_manager import ConfidenceManager, ConfidenceResult, ResponseAction
from .feedback_handler import FeedbackHandler, FeedbackType, FeedbackResult

__all__ = [
    'ExampleStore', 'LearningExample',
    'ComplexityScorer', 'ComplexityLevel', 'ComplexityAssessment',
    'SemanticValidator', 'SemanticValidation', 'ValidationResult',
    'ResultValidator', 'ResultValidation',
    'ConfidenceManager', 'ConfidenceResult', 'ResponseAction',
    'FeedbackHandler', 'FeedbackType', 'FeedbackResult'
]
```

### 1.2 Implement Example Store ✅

- [x] Create `core/engine/learning/example_store.py`
- [x] Implement `ExampleStore` class with:
  - [x] `__init__()` - Initialize SQLite and ChromaDB
  - [x] `add_example()` - Add new learning example
  - [x] `find_similar()` - Semantic similarity search
  - [x] `get_exact_match()` - Exact query matching
  - [x] `update_usage_stats()` - Track example usage
  - [x] `verify_example()` - Mark example as verified
  - [x] `get_statistics()` - Get store stats

### 1.3 Initialize Database ✅

- [x] Create `data/learning.db` (auto-created by ExampleStore)
- [x] Verify tables created:
  - [x] `learning_examples`
  - [x] `example_embeddings`
  - [x] `query_feedback`
  - [x] `historical_results`

### 1.4 Test Example Store ✅

- [x] Run test script successfully (25 tests)
- [x] Verify ChromaDB collection created

---

## Phase 2: Validation Components (Week 2) ✅ COMPLETE

> **STATUS:** All validation components implemented and tested.

### 2.1 Implement Complexity Scorer ✅

- [x] Create `core/engine/learning/complexity_scorer.py`
- [x] Implement `ComplexityScorer` class with:
  - [x] `assess()` - Assess query complexity
  - [x] `get_threshold_adjustment()` - Get confidence adjustment
- [x] 19 tests passing

### 2.2 Implement Semantic Validator ✅

- [x] Create `core/engine/learning/semantic_validator.py`
- [x] Implement `SemanticValidator` class with:
  - [x] `validate()` - Validate SQL against intent
  - [x] `_check_intent_patterns()` - Check intent patterns
  - [x] `_extract_tables()` - Extract tables from SQL
  - [x] `_needs_grouping()` - Check if grouping needed
  - [x] `_check_dangerous_operations()` - Security check
- [x] 21 tests passing

### 2.3 Implement Result Validator ✅

- [x] Create `core/engine/learning/result_validator.py`
- [x] Implement `ResultValidator` class with:
  - [x] `validate()` - Validate query result
  - [x] `_check_bounds()` - Check value bounds
  - [x] `_check_null_ratio()` - Check null ratio
  - [x] `_hash_query()` - Hash for comparison
  - [x] `_get_historical()` - Get historical result
  - [x] `_compare_historical()` - Compare with history
  - [x] `_store_result()` - Store for future
- [x] 18 tests passing

### 2.4 Implement Confidence Manager ✅

- [x] Create `core/engine/learning/confidence_manager.py`
- [x] Implement `ConfidenceManager` class with:
  - [x] `calculate()` - Calculate final confidence (8-component weighted)
  - [x] `_determine_action()` - Determine response action
  - [x] `_generate_warnings()` - Generate warnings
  - [x] `_generate_explanation()` - Generate explanation
- [x] 21 tests passing

---

## Phase 3: Feedback System (Week 3) ✅ COMPLETE

> **STATUS:** Feedback handler implemented and tested.

### 3.1 Implement Feedback Handler ✅

- [x] Create `core/engine/learning/feedback_handler.py`
- [x] Implement `FeedbackHandler` class with:
  - [x] `submit_feedback()` - Submit user feedback
  - [x] `_process_feedback()` - Process and update store
  - [x] `_store_negative_example()` - Store incorrect examples
  - [x] `get_pending_reviews()` - Get unprocessed feedback
  - [x] `get_statistics()` - Get feedback stats
- [x] 18 tests passing

### 3.2 Create Feedback API Router (Pending)

- [ ] Create `docker/api/routers/feedback.py`

```python
"""Feedback API endpoints."""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from enum import Enum

from core.engine.learning import FeedbackHandler, FeedbackType

router = APIRouter()
feedback_handler = FeedbackHandler()


class FeedbackTypeEnum(str, Enum):
    CORRECT = "CORRECT"
    INCORRECT = "INCORRECT"
    CORRECTED = "CORRECTED"


class FeedbackRequest(BaseModel):
    query_id: str
    question: str
    generated_sql: str
    feedback_type: FeedbackTypeEnum
    corrected_sql: Optional[str] = None


class FeedbackResponse(BaseModel):
    success: bool
    feedback_id: str
    action_taken: str
    example_created: bool
    example_id: Optional[str] = None


@router.post("", response_model=FeedbackResponse)
async def submit_feedback(
    request: FeedbackRequest,
    # current_user = Depends(get_current_user)  # Add auth
):
    """Submit feedback for a query response."""
    try:
        result = feedback_handler.submit_feedback(
            query_id=request.query_id,
            question=request.question,
            generated_sql=request.generated_sql,
            feedback_type=FeedbackType[request.feedback_type.value],
            corrected_sql=request.corrected_sql,
            user_id="admin"  # Use current_user.username
        )
        return FeedbackResponse(
            success=result.success,
            feedback_id=result.feedback_id,
            action_taken=result.action_taken,
            example_created=result.example_created,
            example_id=result.example_id
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_feedback_stats():
    """Get feedback statistics."""
    return feedback_handler.get_feedback_stats()
```

- [ ] Register router in `docker/api/main.py`:

```python
from routers import feedback

app.include_router(
    feedback.router,
    prefix="/api/v1/feedback",
    tags=["Feedback"]
)
```

- [ ] Test API endpoints with curl

### 3.3 Create Frontend Feedback Components

- [ ] Create `docker/admin-ui-react/src/components/chat/FeedbackButtons.tsx`

```typescript
import { useState } from 'react';
import { Check, X, Edit2, Loader2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { useToast } from '@/hooks/use-toast';

interface FeedbackButtonsProps {
  queryId: string;
  question: string;
  generatedSql: string;
  onFeedbackSubmitted?: () => void;
}

type FeedbackType = 'CORRECT' | 'INCORRECT' | 'CORRECTED';

export function FeedbackButtons({
  queryId,
  question,
  generatedSql,
  onFeedbackSubmitted
}: FeedbackButtonsProps) {
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [showEditDialog, setShowEditDialog] = useState(false);
  const [correctedSql, setCorrectedSql] = useState(generatedSql);
  const [submitted, setSubmitted] = useState<FeedbackType | null>(null);
  const { toast } = useToast();

  const submitFeedback = async (type: FeedbackType, sql?: string) => {
    setIsSubmitting(true);
    try {
      const response = await fetch('/api/v1/feedback', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          query_id: queryId,
          question,
          generated_sql: generatedSql,
          feedback_type: type,
          corrected_sql: sql
        })
      });

      if (!response.ok) throw new Error('Failed to submit feedback');

      setSubmitted(type);
      toast({
        title: 'Feedback submitted',
        description: 'Thank you for helping improve the system.'
      });
      onFeedbackSubmitted?.();
    } catch (error) {
      toast({
        title: 'Error',
        description: 'Failed to submit feedback',
        variant: 'destructive'
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  if (submitted) {
    return (
      <div className="text-sm text-muted-foreground">
        Feedback recorded: {submitted.toLowerCase()}
      </div>
    );
  }

  return (
    <div className="flex items-center gap-2">
      <span className="text-xs text-muted-foreground mr-2">
        Was this helpful?
      </span>

      <Button
        variant="ghost"
        size="sm"
        onClick={() => submitFeedback('CORRECT')}
        disabled={isSubmitting}
        className="h-7 text-green-600 hover:text-green-700 hover:bg-green-50"
      >
        {isSubmitting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Check className="h-4 w-4" />}
      </Button>

      <Button
        variant="ghost"
        size="sm"
        onClick={() => setShowEditDialog(true)}
        disabled={isSubmitting}
        className="h-7 text-blue-600 hover:text-blue-700 hover:bg-blue-50"
      >
        <Edit2 className="h-4 w-4" />
      </Button>

      <Button
        variant="ghost"
        size="sm"
        onClick={() => submitFeedback('INCORRECT')}
        disabled={isSubmitting}
        className="h-7 text-red-600 hover:text-red-700 hover:bg-red-50"
      >
        <X className="h-4 w-4" />
      </Button>

      <Dialog open={showEditDialog} onOpenChange={setShowEditDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Correct the SQL</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div>
              <label className="text-sm font-medium">Original Query</label>
              <p className="text-sm text-muted-foreground">{question}</p>
            </div>
            <div>
              <label className="text-sm font-medium">Corrected SQL</label>
              <Textarea
                value={correctedSql}
                onChange={(e) => setCorrectedSql(e.target.value)}
                rows={6}
                className="font-mono text-sm"
              />
            </div>
            <div className="flex justify-end gap-2">
              <Button variant="outline" onClick={() => setShowEditDialog(false)}>
                Cancel
              </Button>
              <Button
                onClick={() => {
                  submitFeedback('CORRECTED', correctedSql);
                  setShowEditDialog(false);
                }}
                disabled={isSubmitting}
              >
                Submit Correction
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
}
```

- [ ] Create `docker/admin-ui-react/src/components/chat/ConfidenceIndicator.tsx`

```typescript
import { AlertCircle, CheckCircle, AlertTriangle, HelpCircle } from 'lucide-react';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';

interface ConfidenceIndicatorProps {
  score: number;
  action: string;
  warnings?: string[];
  components?: Record<string, number>;
}

export function ConfidenceIndicator({
  score,
  action,
  warnings = [],
  components = {}
}: ConfidenceIndicatorProps) {
  const getColor = () => {
    if (score >= 90) return 'text-green-600 bg-green-50';
    if (score >= 75) return 'text-yellow-600 bg-yellow-50';
    if (score >= 60) return 'text-orange-600 bg-orange-50';
    return 'text-red-600 bg-red-50';
  };

  const getIcon = () => {
    if (score >= 90) return <CheckCircle className="h-4 w-4" />;
    if (score >= 75) return <AlertTriangle className="h-4 w-4" />;
    if (score >= 60) return <AlertCircle className="h-4 w-4" />;
    return <HelpCircle className="h-4 w-4" />;
  };

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <div className={`inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs font-medium ${getColor()}`}>
            {getIcon()}
            <span>{score.toFixed(0)}%</span>
          </div>
        </TooltipTrigger>
        <TooltipContent className="max-w-sm">
          <div className="space-y-2">
            <div className="font-medium">Confidence Breakdown</div>

            {Object.entries(components).length > 0 && (
              <div className="space-y-1">
                {Object.entries(components).map(([key, value]) => (
                  <div key={key} className="flex justify-between text-xs">
                    <span className="text-muted-foreground">
                      {key.replace(/_/g, ' ')}:
                    </span>
                    <span>{value.toFixed(0)}%</span>
                  </div>
                ))}
              </div>
            )}

            {warnings.length > 0 && (
              <div className="border-t pt-2">
                <div className="text-xs font-medium text-amber-600">Warnings:</div>
                <ul className="text-xs text-muted-foreground">
                  {warnings.map((w, i) => (
                    <li key={i}>• {w}</li>
                  ))}
                </ul>
              </div>
            )}

            <div className="border-t pt-2 text-xs">
              Action: <span className="font-medium">{action.replace(/_/g, ' ')}</span>
            </div>
          </div>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
```

- [ ] Integrate into `ChatContainer.tsx`

---

## Phase 4: Pipeline Integration (Week 4)

### 4.1 Modify Pipeline to Use Learning Components

- [ ] Edit `core/engine/pipeline.py` to add learning integration

**Add imports:**
```python
from core.engine.learning import (
    ExampleStore,
    ComplexityScorer,
    SemanticValidator,
    ResultValidator,
    ConfidenceManager,
    ResponseAction
)
```

**Add to `InferencePipeline.__init__`:**
```python
# Learning components
self.example_store = ExampleStore()
self.complexity_scorer = ComplexityScorer()
self.semantic_validator = SemanticValidator()
self.result_validator = ResultValidator()
self.confidence_manager = ConfidenceManager()
```

**Add example retrieval step (before SQL generation):**
```python
def _get_similar_examples(self, question: str) -> list:
    """Retrieve similar examples for few-shot learning."""
    try:
        examples = self.example_store.find_similar(
            question,
            n_results=3,
            min_similarity=0.7,
            verified_only=True
        )
        return examples
    except Exception as e:
        logger.warning(f"Example retrieval failed: {e}")
        return []
```

**Modify context building to include examples:**
```python
def _build_prompt_with_examples(self, question: str, examples: list) -> str:
    """Build prompt with few-shot examples."""
    example_section = ""
    if examples:
        example_section = "\n\n## Similar Verified Examples:\n"
        for ex in examples[:3]:
            example_section += f"\nQuestion: {ex['question']}\nSQL: {ex['sql']}\n"

    return f"{self.base_prompt}{example_section}\n\nUser Question: {question}"
```

- [ ] Test modified pipeline

### 4.2 Add Complexity-Based Thresholds

- [ ] Modify confidence calculation to include complexity:

```python
def _calculate_enhanced_confidence(
    self,
    question: str,
    sql: str,
    result: Any,
    examples: list,
    original_confidence: dict
) -> dict:
    """Calculate enhanced confidence with learning signals."""

    # Assess complexity
    complexity = self.complexity_scorer.assess(question)

    # Get example similarity
    example_similarity = examples[0]['similarity'] if examples else 0.0

    # Validate semantically
    semantic = self.semantic_validator.validate(
        sql=sql,
        intent=question,
        expected_tables=self._extract_tables_from_question(question)
    )

    # Validate result
    result_validation = self.result_validator.validate(
        question=question,
        sql=sql,
        result=result
    )

    # Calculate final confidence
    confidence = self.confidence_manager.calculate(
        example_similarity=example_similarity,
        dictionary_match=original_confidence.get('dictionary_match', 0),
        metadata_coverage=original_confidence.get('metadata_coverage', 0),
        semantic_alignment=semantic.score,
        complexity_match=1.0 - self.complexity_scorer.get_threshold_adjustment(complexity.level),
        execution_success=1.0 if result is not None else 0.0,
        result_validation=1.0 if result_validation.is_valid else 0.5,
        result_sanity=1.0 if result_validation.checks.get('value_bounds', True) else 0.5,
        complexity_adjustment=-self.complexity_scorer.get_threshold_adjustment(complexity.level),
        result_adjustment=result_validation.confidence_adjustment
    )

    return {
        'score': confidence.score,
        'action': confidence.action.value,
        'components': confidence.components,
        'warnings': confidence.warnings,
        'explanation': confidence.explanation
    }
```

- [ ] Test with various query complexities

### 4.3 Update Chat Response Format

- [ ] Modify `docker/api/routers/chat.py` to include confidence details:

```python
class ChatResponse(BaseModel):
    query_id: str
    answer: str
    sql: Optional[str]
    confidence: float
    confidence_details: Optional[dict]  # NEW
    action: str  # NEW: RETURN_NORMAL, RETURN_WITH_WARNING, etc.
    warnings: List[str]  # NEW
```

- [ ] Update response construction

---

## Phase 5: Training System (Week 5)

### 5.1 Create Training Manager

- [ ] Create `core/engine/learning/training_manager.py`

```python
"""Training Manager - Admin training interface."""

import uuid
import sqlite3
from datetime import datetime
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from pathlib import Path

from .example_store import ExampleStore


@dataclass
class TrainingSession:
    id: str
    name: str
    description: str
    created_by: str
    created_at: datetime
    status: str
    examples_added: int


@dataclass
class CoverageReport:
    total_examples: int
    verified_examples: int
    categories_covered: Dict[str, int]
    suggested_queries: List[str]
    coverage_score: float


class TrainingManager:
    """Manage admin training sessions."""

    SUGGESTED_QUERIES = [
        # Patient counts
        "How many subjects are in the study?",
        "How many subjects are in the safety population?",
        "How many subjects completed the study?",
        "How many subjects discontinued?",

        # Demographics
        "What is the average age of subjects?",
        "Show age distribution by treatment group",
        "List subjects by gender",
        "Show race breakdown",

        # Adverse events
        "How many adverse events occurred?",
        "List serious adverse events",
        "Show AEs by preferred term",
        "Count AEs by treatment group",

        # Lab values
        "Show baseline lab values",
        "Compare lab values between visits",
        "List abnormal lab results",

        # Medications
        "List concomitant medications",
        "Show prior medications",
        "Count subjects on medication X"
    ]

    def __init__(
        self,
        db_path: str = "data/learning.db",
        example_store: Optional[ExampleStore] = None
    ):
        self.db_path = Path(db_path)
        self.example_store = example_store or ExampleStore()
        self._init_database()

    def _init_database(self):
        """Initialize training tables."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS training_sessions (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    created_by TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    status TEXT DEFAULT 'active',
                    examples_added INTEGER DEFAULT 0
                )
            """)

    def start_session(
        self,
        name: str,
        description: str = "",
        created_by: str = "admin"
    ) -> TrainingSession:
        """Start a new training session."""
        session_id = str(uuid.uuid4())

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO training_sessions
                (id, name, description, created_by)
                VALUES (?, ?, ?, ?)
            """, (session_id, name, description, created_by))

        return TrainingSession(
            id=session_id,
            name=name,
            description=description,
            created_by=created_by,
            created_at=datetime.now(),
            status='active',
            examples_added=0
        )

    def add_example(
        self,
        session_id: str,
        question: str,
        sql: str,
        category: str = "general",
        created_by: str = "admin"
    ) -> str:
        """Add example to training session."""
        example_id = self.example_store.add_example(
            question=question,
            sql=sql,
            category=category,
            source="training",
            verified=True,
            created_by=created_by
        )

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE training_sessions
                SET examples_added = examples_added + 1
                WHERE id = ?
            """, (session_id,))

        return example_id

    def complete_session(self, session_id: str):
        """Complete a training session."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE training_sessions
                SET status = 'completed', completed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (session_id,))

    def get_coverage_report(self) -> CoverageReport:
        """Get training coverage report."""
        stats = self.example_store.get_statistics()

        # Calculate coverage score
        total_suggested = len(self.SUGGESTED_QUERIES)
        covered = 0

        for query in self.SUGGESTED_QUERIES:
            similar = self.example_store.find_similar(query, n_results=1)
            if similar and similar[0]['similarity'] > 0.85:
                covered += 1

        coverage_score = (covered / total_suggested) * 100 if total_suggested > 0 else 0

        # Find uncovered queries
        uncovered = []
        for query in self.SUGGESTED_QUERIES:
            similar = self.example_store.find_similar(query, n_results=1)
            if not similar or similar[0]['similarity'] < 0.85:
                uncovered.append(query)

        return CoverageReport(
            total_examples=stats['total_examples'],
            verified_examples=stats['verified_examples'],
            categories_covered=stats['by_category'],
            suggested_queries=uncovered[:10],  # Top 10 uncovered
            coverage_score=coverage_score
        )

    def get_suggested_queries(self) -> List[str]:
        """Get queries that need training examples."""
        uncovered = []
        for query in self.SUGGESTED_QUERIES:
            similar = self.example_store.find_similar(query, n_results=1)
            if not similar or similar[0]['similarity'] < 0.85:
                uncovered.append(query)
        return uncovered
```

### 5.2 Create Training API Router

- [ ] Create `docker/api/routers/training.py`
- [ ] Register in `main.py`
- [ ] Test endpoints

### 5.3 Create Training Admin UI

- [ ] Create `docker/admin-ui-react/src/features/training/TrainingPage.tsx`
- [ ] Add route to router
- [ ] Add navigation link

---

## Phase 6: Testing & Deployment (Week 6)

### 6.1 Unit Tests

- [ ] Create `tests/learning/test_example_store.py`
- [ ] Create `tests/learning/test_complexity_scorer.py`
- [ ] Create `tests/learning/test_semantic_validator.py`
- [ ] Create `tests/learning/test_result_validator.py`
- [ ] Create `tests/learning/test_confidence_manager.py`
- [ ] Create `tests/learning/test_feedback_handler.py`
- [ ] Run all tests: `pytest tests/learning/ -v`

### 6.2 Integration Tests

- [ ] Test full pipeline with learning components
- [ ] Test feedback loop end-to-end
- [ ] Test training session workflow
- [ ] Test API endpoints

### 6.3 Performance Testing

- [ ] Measure query latency with learning (target: <500ms overhead)
- [ ] Test ChromaDB performance with 1000+ examples
- [ ] Profile memory usage

### 6.4 Initial Training

- [ ] Add 50+ verified examples covering:
  - [ ] Patient counts (10 examples)
  - [ ] Demographics (10 examples)
  - [ ] Adverse events (10 examples)
  - [ ] Lab values (10 examples)
  - [ ] Medications (5 examples)
  - [ ] Study completion (5 examples)

### 6.5 Docker Updates

- [ ] Update `docker-compose.yml` if needed
- [ ] Verify volume mounts for `data/learning.db`
- [ ] Test in Docker environment

### 6.6 Documentation

- [ ] Update API documentation
- [ ] Create training guide for admins
- [ ] Document confidence levels and actions

---

## Configuration Options

### Environment Variables

```env
# Learning System
LEARNING_DB_PATH=data/learning.db
CHROMA_PATH=knowledge/chroma
EXAMPLE_COLLECTION=query_examples

# Confidence Thresholds
CONFIDENCE_HIGH=90
CONFIDENCE_MEDIUM=75
CONFIDENCE_LOW=60
CONFIDENCE_REFUSE=40

# Feature Flags
ENABLE_LEARNING=true
ENABLE_FEEDBACK=true
ENABLE_TRAINING=true
ENABLE_SEMANTIC_VALIDATION=true
ENABLE_RESULT_VALIDATION=true
```

---

## Success Metrics

### Accuracy Targets

| Metric | Before | Target | Measurement |
|--------|--------|--------|-------------|
| Overall Accuracy | 60-70% | 85-95% | User feedback ratio |
| Trained Queries | N/A | 95%+ | Exact/similar match |
| Novel Queries | 50-60% | 75-85% | Semantic validation |
| False Positives | Unknown | <5% | Incorrect high-confidence |

### System Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Query Latency | <2s | 95th percentile |
| Learning Overhead | <500ms | Time added by learning |
| Training Coverage | >80% | Suggested queries covered |
| Feedback Rate | >10% | Users providing feedback |

---

## Rollback Plan

If issues occur:

1. **Disable learning** via environment variable: `ENABLE_LEARNING=false`
2. **Fall back** to original confidence scorer
3. **Preserve data** - learning.db remains for future use
4. **Investigate** issues with logging

---

## Next Steps After Implementation

1. **Monitor** accuracy improvement over time
2. **Collect** feedback and iterate
3. **Expand** training examples based on usage
4. **Tune** confidence thresholds based on false positive rate
5. **Add** more validation rules as patterns emerge
