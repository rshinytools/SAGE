# SAGE API - Golden Test Suite Router
# ====================================
"""
Golden Test Suite endpoints for validating SAGE accuracy.
Admin-only access for running batch tests and viewing results.
"""

import json
import time
import asyncio
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from collections import defaultdict
from dataclasses import dataclass, field, asdict

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Query
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
import requests
import pandas as pd

from routers.auth import get_current_user

router = APIRouter()

# ============================================
# Configuration
# ============================================

# Path to questions file (inside container)
QUESTIONS_PATH = Path("/app/knowledge/golden_suite/questions.json")
DATA_DIR = Path("/app/data/raw")
ADSL_PATH = DATA_DIR / "adsl.parquet"
ADAE_PATH = DATA_DIR / "adae.parquet"

# For local development
if not QUESTIONS_PATH.exists():
    QUESTIONS_PATH = Path(__file__).parent.parent.parent.parent / "knowledge" / "golden_suite" / "questions.json"
if not DATA_DIR.exists():
    DATA_DIR = Path(__file__).parent.parent.parent.parent / "data" / "raw"
    ADSL_PATH = DATA_DIR / "adsl.parquet"
    ADAE_PATH = DATA_DIR / "adae.parquet"

# In-memory store for test runs
TEST_RUNS: Dict[str, Dict] = {}

# ============================================
# Models
# ============================================

class TestQuestion(BaseModel):
    id: int
    question: str
    category: str
    sql_template: str
    answer_type: str
    notes: Optional[str] = ""
    expected_answer: Optional[Any] = None
    flow_id: Optional[str] = None
    turn: Optional[int] = None


class RunRequest(BaseModel):
    categories: Optional[List[str]] = None
    question_ids: Optional[List[int]] = None
    include_flows: bool = True


class TestResult(BaseModel):
    question_id: int
    question: str
    category: str
    expected: Any
    actual: Optional[Any] = None
    match: Optional[bool] = None
    answer_text: str = ""
    sql_executed: str = ""
    confidence: Dict = {}
    execution_time_ms: float = 0
    error: str = ""
    flow_id: Optional[str] = None
    turn: Optional[int] = None


class RunSummary(BaseModel):
    run_id: str
    status: str  # pending, running, completed, failed
    started_at: str
    completed_at: Optional[str] = None
    total_questions: int
    completed_questions: int
    matches: int
    mismatches: int
    manual_check: int
    accuracy: float
    categories_requested: Optional[List[str]] = None
    by_category: Dict[str, Dict] = {}


# ============================================
# Helper Functions
# ============================================

def load_questions() -> List[Dict]:
    """Load questions from JSON file."""
    if not QUESTIONS_PATH.exists():
        raise HTTPException(status_code=404, detail="Questions file not found")

    with open(QUESTIONS_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_categories(questions: List[Dict]) -> List[Dict]:
    """Get unique categories with counts."""
    category_counts = defaultdict(int)
    for q in questions:
        category_counts[q['category']] += 1

    return [
        {"name": cat, "count": count}
        for cat, count in sorted(category_counts.items())
    ]


def calculate_ground_truth(questions: List[Dict]) -> List[Dict]:
    """Calculate expected answers for questions using parquet data."""
    try:
        adsl = pd.read_parquet(ADSL_PATH)
        adae = pd.read_parquet(ADAE_PATH)
    except Exception as e:
        # Return questions without ground truth if data not available
        return questions

    results = []
    for q in questions:
        q_copy = q.copy()

        # If expected_answer already set, use it
        if 'expected_answer' in q and q['expected_answer'] is not None:
            results.append(q_copy)
            continue

        # Calculate from data
        try:
            answer = _calculate_single_answer(adsl, adae, q['sql_template'], q['answer_type'])
            q_copy['expected_answer'] = answer
        except Exception:
            q_copy['expected_answer'] = None

        results.append(q_copy)

    return results


def _calculate_single_answer(adsl: pd.DataFrame, adae: pd.DataFrame,
                             sql_template: str, answer_type: str) -> Any:
    """Calculate a single answer based on SQL template."""
    sql = sql_template.upper()

    # Determine which dataframe to use
    if 'FROM ADSL' in sql:
        df = adsl.copy()
    elif 'FROM ADAE' in sql:
        df = adae.copy()
    else:
        return None

    # Apply WHERE filters
    if 'WHERE' in sql:
        where_clause = sql.split('WHERE')[1]
        for keyword in ['GROUP BY', 'ORDER BY', 'LIMIT']:
            if keyword in where_clause:
                where_clause = where_clause.split(keyword)[0]
        df = _apply_filters(df, where_clause)

    # Calculate result
    if answer_type == 'count':
        if 'COUNT(DISTINCT USUBJID)' in sql:
            return int(df['USUBJID'].nunique())
        return len(df)

    elif answer_type == 'distribution':
        if 'GROUP BY' in sql:
            group_col = sql.split('GROUP BY')[1].strip().split()[0].replace(',', '')
            if 'COUNT(DISTINCT' in sql:
                result = df.groupby(group_col)['USUBJID'].nunique().to_dict()
            else:
                result = df[group_col].value_counts().to_dict()

            if 'LIMIT' in sql:
                limit = int(sql.split('LIMIT')[1].strip().split()[0])
                result = dict(list(result.items())[:limit])
            return result
        return {}

    elif answer_type == 'list':
        return len(df)

    return None


def _apply_filters(df: pd.DataFrame, where_clause: str) -> pd.DataFrame:
    """Apply WHERE clause filters to dataframe."""
    conditions = re.split(r'\s+AND\s+', where_clause)

    for cond in conditions:
        cond = cond.strip()
        if not cond:
            continue

        # Column = 'VALUE'
        match = re.search(r"(\w+)\s*=\s*'([^']+)'", cond)
        if match:
            col, val = match.groups()
            if col in df.columns:
                df = df[df[col] == val]
            continue

        # Column IN (...)
        match = re.search(r"(\w+)\s+IN\s*\(([^)]+)\)", cond)
        if match:
            col = match.group(1)
            vals = [v.strip().strip("'\"") for v in match.group(2).split(',')]
            if col in df.columns:
                df = df[df[col].isin(vals)]
            continue

        # UPPER(Column) = 'VALUE'
        match = re.search(r"UPPER\((\w+)\)\s*=\s*'([^']+)'", cond)
        if match:
            col, val = match.groups()
            if col in df.columns:
                df = df[df[col].str.upper() == val]
            continue

        # UPPER(Column) IN (...)
        match = re.search(r"UPPER\((\w+)\)\s+IN\s*\(([^)]+)\)", cond)
        if match:
            col = match.group(1)
            vals = [v.strip().strip("'\"") for v in match.group(2).split(',')]
            if col in df.columns:
                df = df[df[col].str.upper().isin(vals)]
            continue

        # UPPER(Column) LIKE '%VALUE%'
        match = re.search(r"UPPER\((\w+)\)\s+LIKE\s+'%([^%]+)%'", cond)
        if match:
            col, val = match.groups()
            if col in df.columns:
                df = df[df[col].str.upper().str.contains(val, na=False)]
            continue

        # AGE comparisons
        match = re.search(r"AGE\s*([><=]+)\s*(\d+)", cond)
        if match:
            op, val = match.groups()
            val = int(val)
            if 'AGE' in df.columns:
                if op == '>=':
                    df = df[df['AGE'] >= val]
                elif op == '>':
                    df = df[df['AGE'] > val]
                elif op == '<=':
                    df = df[df['AGE'] <= val]
                elif op == '<':
                    df = df[df['AGE'] < val]
                elif op == '=':
                    df = df[df['AGE'] == val]
            continue

        # ATOXGR filters
        match = re.search(r"ATOXGR\s*=\s*'(\d)'", cond)
        if match:
            grade = match.group(1)
            if 'ATOXGR' in df.columns:
                df = df[df['ATOXGR'] == grade]
            continue

        match = re.search(r"ATOXGR\s+IN\s*\(([^)]+)\)", cond)
        if match:
            grades = [g.strip().strip("'") for g in match.group(1).split(',')]
            if 'ATOXGR' in df.columns:
                df = df[df['ATOXGR'].isin(grades)]
            continue

    return df


def extract_number_from_answer(answer: str) -> Optional[int]:
    """Extract a number from the answer text."""
    if not answer:
        return None

    # Look for bold numbers like **48**
    bold_match = re.search(r'\*\*(\d+(?:,\d{3})*)\*\*', answer)
    if bold_match:
        return int(bold_match.group(1).replace(',', ''))

    # Look for any number
    numbers = re.findall(r'\b(\d+(?:,\d{3})*)\b', answer)
    if numbers:
        return int(numbers[0].replace(',', ''))

    return None


def _make_sync_request(chat_url: str, payload: dict, headers: dict, timeout: int = 300):
    """Synchronous request helper to run in thread pool."""
    response = requests.post(
        chat_url,
        json=payload,
        headers=headers,
        timeout=timeout
    )
    response.raise_for_status()
    return response.json()


async def run_test_batch(run_id: str, questions: List[Dict], token: str):
    """Run tests against the chat API (background task)."""
    import os

    run = TEST_RUNS[run_id]
    run['status'] = 'running'

    results = []
    current_conversation = None
    current_flow = None

    # For self-calls within Docker, use localhost:8000 (internal port)
    # The API container can call itself on its internal port
    api_host = os.getenv("API_HOST", "localhost")
    api_port = os.getenv("API_PORT", "8000")  # Internal container port
    chat_url = f"http://{api_host}:{api_port}/api/v1/chat/message"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    for q in questions:
        # Handle conversation flows
        if q.get('flow_id'):
            if q['flow_id'] != current_flow:
                current_flow = q['flow_id']
                current_conversation = None
        else:
            current_conversation = None
            current_flow = None

        start_time = time.time()

        try:
            payload = {"message": q['question']}
            if current_conversation:
                payload["conversation_id"] = current_conversation

            # Run synchronous request in thread pool to avoid blocking event loop
            data = await asyncio.to_thread(
                _make_sync_request,
                chat_url,
                payload,
                headers,
                300
            )

            elapsed_ms = (time.time() - start_time) * 1000

            answer_text = data.get("content", "")
            metadata = data.get("metadata", {})

            if q.get('flow_id'):
                current_conversation = data.get("conversation_id")

            actual_number = extract_number_from_answer(answer_text)
            expected = q.get('expected_answer')

            # Compare
            match = None
            if isinstance(expected, int):
                match = actual_number == expected
            elif isinstance(expected, dict):
                match = None  # Manual check for distributions

            result = {
                'question_id': q['id'],
                'question': q['question'],
                'category': q['category'],
                'expected': expected,
                'actual': actual_number,
                'match': match,
                'answer_text': answer_text[:500],
                'sql_executed': metadata.get('sql', ''),
                'confidence': metadata.get('confidence', {}),
                'execution_time_ms': elapsed_ms,
                'error': '',
                'flow_id': q.get('flow_id'),
                'turn': q.get('turn')
            }

        except Exception as e:
            result = {
                'question_id': q['id'],
                'question': q['question'],
                'category': q['category'],
                'expected': q.get('expected_answer'),
                'actual': None,
                'match': False,
                'answer_text': '',
                'sql_executed': '',
                'confidence': {},
                'execution_time_ms': (time.time() - start_time) * 1000,
                'error': str(e),
                'flow_id': q.get('flow_id'),
                'turn': q.get('turn')
            }

        results.append(result)
        run['completed_questions'] = len(results)
        run['results'] = results

        # Update running stats
        matches = sum(1 for r in results if r['match'] is True)
        mismatches = sum(1 for r in results if r['match'] is False)
        manual = sum(1 for r in results if r['match'] is None)
        run['matches'] = matches
        run['mismatches'] = mismatches
        run['manual_check'] = manual
        run['accuracy'] = (matches / len(results) * 100) if results else 0

    # Calculate by-category stats
    by_category = defaultdict(lambda: {'total': 0, 'match': 0, 'mismatch': 0, 'manual': 0})
    for r in results:
        cat = r['category']
        by_category[cat]['total'] += 1
        if r['match'] is True:
            by_category[cat]['match'] += 1
        elif r['match'] is False:
            by_category[cat]['mismatch'] += 1
        else:
            by_category[cat]['manual'] += 1

    for cat in by_category:
        total = by_category[cat]['total']
        match = by_category[cat]['match']
        by_category[cat]['accuracy'] = (match / total * 100) if total > 0 else 0

    run['by_category'] = dict(by_category)
    run['status'] = 'completed'
    run['completed_at'] = datetime.now().isoformat()


def require_admin(user: dict = Depends(get_current_user)):
    """Require admin role."""
    if 'admin' not in user.get('roles', []):
        raise HTTPException(
            status_code=403,
            detail={"code": "FORBIDDEN", "message": "Admin access required"}
        )
    return user


# ============================================
# Endpoints
# ============================================

@router.get("/categories")
async def list_categories(user: dict = Depends(require_admin)):
    """
    Get list of test categories with question counts.
    """
    questions = load_questions()
    categories = get_categories(questions)

    return {
        "success": True,
        "data": {
            "categories": categories,
            "total_questions": len(questions)
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.get("/questions")
async def list_questions(
    category: Optional[str] = Query(None, description="Filter by category"),
    include_expected: bool = Query(False, description="Include expected answers"),
    user: dict = Depends(require_admin)
):
    """
    Get test questions with optional filtering.
    """
    questions = load_questions()

    if category:
        questions = [q for q in questions if q['category'] == category]

    if include_expected:
        questions = calculate_ground_truth(questions)

    return {
        "success": True,
        "data": {
            "questions": questions,
            "count": len(questions)
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.get("/questions/{question_id}")
async def get_question(
    question_id: int,
    include_expected: bool = Query(True),
    user: dict = Depends(require_admin)
):
    """
    Get a single question by ID.
    """
    questions = load_questions()
    question = next((q for q in questions if q['id'] == question_id), None)

    if not question:
        raise HTTPException(status_code=404, detail="Question not found")

    if include_expected:
        questions = calculate_ground_truth([question])
        question = questions[0]

    return {
        "success": True,
        "data": question,
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/run")
async def start_test_run(
    request: RunRequest,
    background_tasks: BackgroundTasks,
    user: dict = Depends(require_admin)
):
    """
    Start a new test run. Returns immediately with run_id.
    Use GET /runs/{run_id} to check progress.
    """
    questions = load_questions()
    questions = calculate_ground_truth(questions)

    # Filter questions
    if request.categories:
        questions = [q for q in questions if q['category'] in request.categories]

    if request.question_ids:
        questions = [q for q in questions if q['id'] in request.question_ids]

    if not request.include_flows:
        questions = [q for q in questions if q.get('category') != 'Conversational Flow']

    if not questions:
        raise HTTPException(status_code=400, detail="No questions match the criteria")

    # Create run
    run_id = str(uuid.uuid4())[:8]

    # Get token from the request (we need to pass it to the background task)
    # This is a simplified approach - in production, use a service account
    from fastapi import Request

    run = {
        'run_id': run_id,
        'status': 'pending',
        'started_at': datetime.now().isoformat(),
        'completed_at': None,
        'total_questions': len(questions),
        'completed_questions': 0,
        'matches': 0,
        'mismatches': 0,
        'manual_check': 0,
        'accuracy': 0,
        'categories_requested': request.categories,
        'by_category': {},
        'results': [],
        'questions': questions
    }

    TEST_RUNS[run_id] = run

    # For the background task, we need to re-create a token
    # Since we're calling our own API, use the user's credentials
    from routers.auth import create_token
    from datetime import timedelta

    internal_token = create_token(
        {"sub": user['sub'], "roles": user.get('roles', []), "type": "access"},
        timedelta(hours=2)
    )

    background_tasks.add_task(run_test_batch, run_id, questions, internal_token)

    return {
        "success": True,
        "data": {
            "run_id": run_id,
            "status": "pending",
            "total_questions": len(questions),
            "message": f"Test run started. Use GET /runs/{run_id} to check progress."
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.get("/runs")
async def list_runs(user: dict = Depends(require_admin)):
    """
    List all test runs.
    """
    runs = []
    for run_id, run in TEST_RUNS.items():
        runs.append({
            'run_id': run['run_id'],
            'status': run['status'],
            'started_at': run['started_at'],
            'completed_at': run.get('completed_at'),
            'total_questions': run['total_questions'],
            'completed_questions': run['completed_questions'],
            'matches': run['matches'],
            'accuracy': run['accuracy']
        })

    return {
        "success": True,
        "data": {"runs": sorted(runs, key=lambda x: x['started_at'], reverse=True)},
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.get("/runs/{run_id}")
async def get_run(run_id: str, user: dict = Depends(require_admin)):
    """
    Get details of a specific test run.
    """
    if run_id not in TEST_RUNS:
        raise HTTPException(status_code=404, detail="Run not found")

    run = TEST_RUNS[run_id]

    return {
        "success": True,
        "data": {
            'run_id': run['run_id'],
            'status': run['status'],
            'started_at': run['started_at'],
            'completed_at': run.get('completed_at'),
            'total_questions': run['total_questions'],
            'completed_questions': run['completed_questions'],
            'matches': run['matches'],
            'mismatches': run['mismatches'],
            'manual_check': run['manual_check'],
            'accuracy': run['accuracy'],
            'categories_requested': run.get('categories_requested'),
            'by_category': run.get('by_category', {}),
            'results': run.get('results', [])
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.get("/runs/{run_id}/download")
async def download_run_results(
    run_id: str,
    format: str = Query("json", description="Export format: json, csv, or html"),
    category: Optional[str] = Query(None, description="Filter by category"),
    user: dict = Depends(require_admin)
):
    """
    Download test run results in various formats.
    """
    if run_id not in TEST_RUNS:
        raise HTTPException(status_code=404, detail="Run not found")

    run = TEST_RUNS[run_id]
    results = run.get('results', [])

    if category:
        results = [r for r in results if r['category'] == category]

    if format == "json":
        return JSONResponse(
            content={
                "run_id": run_id,
                "status": run['status'],
                "accuracy": run['accuracy'],
                "results": results
            },
            media_type="application/json",
            headers={
                "Content-Disposition": f"attachment; filename=golden_test_{run_id}.json"
            }
        )

    elif format == "csv":
        import io
        import csv

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=[
            'question_id', 'question', 'category', 'expected', 'actual',
            'match', 'confidence', 'execution_time_ms', 'error'
        ])
        writer.writeheader()

        for r in results:
            writer.writerow({
                'question_id': r['question_id'],
                'question': r['question'],
                'category': r['category'],
                'expected': r['expected'],
                'actual': r['actual'],
                'match': 'PASS' if r['match'] is True else ('FAIL' if r['match'] is False else 'MANUAL'),
                'confidence': r.get('confidence', {}).get('score', ''),
                'execution_time_ms': round(r['execution_time_ms'], 1),
                'error': r.get('error', '')
            })

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=golden_test_{run_id}.csv"
            }
        )

    elif format == "html":
        html = _generate_html_report(run, results)
        return StreamingResponse(
            iter([html]),
            media_type="text/html",
            headers={
                "Content-Disposition": f"attachment; filename=golden_test_{run_id}.html"
            }
        )

    else:
        raise HTTPException(status_code=400, detail="Invalid format. Use json, csv, or html.")


def _generate_html_report(run: Dict, results: List[Dict]) -> str:
    """Generate HTML test report."""
    total = len(results)
    matches = sum(1 for r in results if r['match'] is True)
    mismatches = sum(1 for r in results if r['match'] is False)
    manual = sum(1 for r in results if r['match'] is None)
    accuracy = (matches / total * 100) if total > 0 else 0

    # Group by category
    categories = defaultdict(list)
    for r in results:
        categories[r['category']].append(r)

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>SAGE Golden Test Results - {run['run_id']}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        h1 {{ color: #2c3e50; }}
        h2 {{ color: #34495e; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
        .summary {{ background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
        .summary-stats {{ display: flex; gap: 30px; flex-wrap: wrap; }}
        .stat {{ text-align: center; min-width: 100px; }}
        .stat-value {{ font-size: 32px; font-weight: bold; }}
        .stat-label {{ color: #666; }}
        .pass {{ color: #27ae60; }}
        .fail {{ color: #e74c3c; }}
        .manual {{ color: #f39c12; }}
        .category {{ background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
        table {{ width: 100%; border-collapse: collapse; }}
        th {{ background: #3498db; color: white; padding: 12px; text-align: left; }}
        td {{ padding: 10px; border-bottom: 1px solid #ddd; }}
        tr:hover {{ background: #f8f9fa; }}
        .result-pass {{ background: #d4edda; }}
        .result-fail {{ background: #f8d7da; }}
        .result-manual {{ background: #fff3cd; }}
    </style>
</head>
<body>
    <h1>SAGE Golden Test Results</h1>
    <p>Run ID: {run['run_id']} | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

    <div class="summary">
        <h2>Summary</h2>
        <div class="summary-stats">
            <div class="stat">
                <div class="stat-value">{total}</div>
                <div class="stat-label">Total</div>
            </div>
            <div class="stat">
                <div class="stat-value pass">{matches}</div>
                <div class="stat-label">Passed</div>
            </div>
            <div class="stat">
                <div class="stat-value fail">{mismatches}</div>
                <div class="stat-label">Failed</div>
            </div>
            <div class="stat">
                <div class="stat-value manual">{manual}</div>
                <div class="stat-label">Manual</div>
            </div>
            <div class="stat">
                <div class="stat-value" style="color: {'#27ae60' if accuracy >= 80 else '#f39c12' if accuracy >= 60 else '#e74c3c'};">{accuracy:.1f}%</div>
                <div class="stat-label">Accuracy</div>
            </div>
        </div>
    </div>
"""

    for category in ['Populations', 'Demographics', 'Adverse Events', 'Complex Logic',
                     'Fuzzy Matching', 'Negative Testing', 'Conversational Flow']:
        if category not in categories:
            continue

        cat_results = categories[category]
        cat_matches = sum(1 for r in cat_results if r['match'] is True)
        cat_total = len(cat_results)
        cat_acc = (cat_matches / cat_total * 100) if cat_total > 0 else 0

        html += f"""
    <div class="category">
        <h2>{category} ({cat_matches}/{cat_total} - {cat_acc:.0f}%)</h2>
        <table>
            <tr>
                <th>ID</th>
                <th>Question</th>
                <th>Expected</th>
                <th>Actual</th>
                <th>Result</th>
            </tr>
"""
        for r in cat_results:
            result_class = 'result-pass' if r['match'] is True else ('result-fail' if r['match'] is False else 'result-manual')
            result_text = 'PASS' if r['match'] is True else ('FAIL' if r['match'] is False else 'MANUAL')

            html += f"""
            <tr class="{result_class}">
                <td>{r['question_id']}</td>
                <td>{r['question'][:80]}{'...' if len(r['question']) > 80 else ''}</td>
                <td>{r['expected']}</td>
                <td>{r['actual'] if r['actual'] is not None else 'N/A'}</td>
                <td><strong>{result_text}</strong></td>
            </tr>
"""

        html += """
        </table>
    </div>
"""

    html += """
</body>
</html>
"""
    return html


@router.delete("/runs/{run_id}")
async def delete_run(run_id: str, user: dict = Depends(require_admin)):
    """
    Delete a test run.
    """
    if run_id not in TEST_RUNS:
        raise HTTPException(status_code=404, detail="Run not found")

    del TEST_RUNS[run_id]

    return {
        "success": True,
        "data": {"message": f"Run {run_id} deleted"},
        "meta": {"timestamp": datetime.now().isoformat()}
    }
