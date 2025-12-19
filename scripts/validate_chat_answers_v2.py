"""
SAGE Golden Test Suite v2
=========================
Enterprise-Level Validation Suite with 100 questions across 7 categories:
1. Populations (10): Safety, ITT, efficacy counts
2. Demographics (15): Age, Sex, Race breakdowns
3. Adverse Events (30): High-frequency queries, specific terms
4. Complex Logic (10): Combinations of filters
5. Fuzzy Matching (10): Typos, synonyms, varied phrasings
6. Negative Testing (5): Queries that should return 0
7. Conversational Flows (20): 5 flows x 4 turns each

Usage:
    py scripts/validate_chat_answers_v2.py                    # Calculate ground truth only
    py scripts/validate_chat_answers_v2.py --test-chat        # Test against live API
    py scripts/validate_chat_answers_v2.py --test-chat --html # Generate HTML report
    py scripts/validate_chat_answers_v2.py --flows-only       # Test only conversation flows
"""

import pandas as pd
import json
import requests
import time
import re
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict

# =============================================================================
# CONFIGURATION
# =============================================================================

DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
ADSL_PATH = DATA_DIR / "adsl.parquet"
ADAE_PATH = DATA_DIR / "adae.parquet"

API_BASE_URL = "http://localhost:8002/api/v1"
CHAT_API_URL = f"{API_BASE_URL}/chat/message"
QUERY_API_URL = f"{API_BASE_URL}/chat/query"

DEFAULT_USERNAME = "admin"
DEFAULT_PASSWORD = "sage2024"

QUESTIONS_FILE = Path(__file__).parent / "validation_questions_v2.json"

# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class TestQuestion:
    """A test question with expected answer."""
    id: int
    question: str
    category: str
    sql_template: str
    answer_type: str
    expected_answer: Any = None
    notes: str = ""
    flow_id: Optional[str] = None
    turn: Optional[int] = None


@dataclass
class TestResult:
    """Result of a single test."""
    question_id: int
    question: str
    category: str
    expected: Any
    actual: Any
    match: Optional[bool]
    answer_text: str = ""
    sql_executed: str = ""
    confidence: Dict = field(default_factory=dict)
    execution_time_ms: float = 0
    error: str = ""
    flow_id: Optional[str] = None
    turn: Optional[int] = None


# =============================================================================
# DATA LOADING & GROUND TRUTH CALCULATION
# =============================================================================

def load_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load ADSL and ADAE data."""
    print("Loading data...")
    adsl = pd.read_parquet(ADSL_PATH)
    adae = pd.read_parquet(ADAE_PATH)
    print(f"  ADSL: {len(adsl)} rows, {len(adsl.columns)} columns")
    print(f"  ADAE: {len(adae)} rows, {len(adae.columns)} columns")
    return adsl, adae


def load_questions() -> List[Dict]:
    """Load questions from JSON file."""
    with open(QUESTIONS_FILE, 'r') as f:
        return json.load(f)


def calculate_ground_truth(adsl: pd.DataFrame, adae: pd.DataFrame,
                          questions: List[Dict]) -> List[TestQuestion]:
    """
    Calculate ground truth answers for all test questions.
    """
    results = []

    # Helper functions
    def get_grade(val):
        """Convert grade value to int."""
        if pd.isna(val) or val == '.' or val == '':
            return 0
        try:
            return int(str(val).replace('Grade ', '').strip())
        except:
            return 0

    for q in questions:
        question = TestQuestion(
            id=q['id'],
            question=q['question'],
            category=q['category'],
            sql_template=q['sql_template'],
            answer_type=q['answer_type'],
            notes=q.get('notes', ''),
            flow_id=q.get('flow_id'),
            turn=q.get('turn')
        )

        # If expected_answer is already set (like for negative tests), use it
        if 'expected_answer' in q:
            question.expected_answer = q['expected_answer']
            results.append(question)
            continue

        # Calculate based on answer_type and SQL template
        try:
            answer = calculate_single_answer(adsl, adae, q['sql_template'],
                                            q['answer_type'], get_grade)
            question.expected_answer = answer
        except Exception as e:
            question.expected_answer = f"ERROR: {str(e)}"

        results.append(question)

    return results


def calculate_single_answer(adsl: pd.DataFrame, adae: pd.DataFrame,
                           sql_template: str, answer_type: str,
                           get_grade_func) -> Any:
    """Calculate a single answer based on SQL template."""

    # Parse the SQL to understand what to calculate
    sql = sql_template.upper()

    # Determine which dataframe to use
    if 'FROM ADSL' in sql:
        df = adsl.copy()
    elif 'FROM ADAE' in sql:
        df = adae.copy()
    else:
        return "Unknown table"

    # Apply filters from WHERE clause
    if 'WHERE' in sql:
        where_clause = sql.split('WHERE')[1]
        # Remove GROUP BY, ORDER BY, LIMIT
        for keyword in ['GROUP BY', 'ORDER BY', 'LIMIT']:
            if keyword in where_clause:
                where_clause = where_clause.split(keyword)[0]

        df = apply_filters(df, where_clause, get_grade_func)

    # Handle different query types
    if answer_type == 'count':
        if 'COUNT(DISTINCT USUBJID)' in sql:
            return df['USUBJID'].nunique()
        elif 'COUNT(*)' in sql:
            # Check for subquery aggregation
            if 'SELECT COUNT(*) FROM (' in sql:
                # This is counting groups, handle specially
                return len(df)
            return len(df)
        else:
            return len(df)

    elif answer_type == 'distribution':
        # Parse GROUP BY column
        if 'GROUP BY' in sql:
            group_col = sql.split('GROUP BY')[1].strip().split()[0]
            group_col = group_col.replace(',', '').strip()

            # Check if we need distinct count
            if 'COUNT(DISTINCT' in sql:
                result = df.groupby(group_col)['USUBJID'].nunique().to_dict()
            else:
                result = df[group_col].value_counts().to_dict()

            # Apply LIMIT if present
            if 'LIMIT' in sql:
                limit = int(sql.split('LIMIT')[1].strip().split()[0])
                result = dict(list(result.items())[:limit])

            return result
        return df.iloc[0].to_dict() if len(df) > 0 else {}

    elif answer_type == 'list':
        return len(df)  # Just return count for lists

    return None


def apply_filters(df: pd.DataFrame, where_clause: str, get_grade_func) -> pd.DataFrame:
    """Apply WHERE clause filters to dataframe."""

    # Handle various filter patterns
    filters = []

    # Split by AND (simple approach - doesn't handle nested logic)
    conditions = re.split(r'\s+AND\s+', where_clause)

    for cond in conditions:
        cond = cond.strip()
        if not cond:
            continue

        # Handle various patterns

        # Pattern: COLUMN = 'VALUE'
        match = re.search(r"(\w+)\s*=\s*'([^']+)'", cond)
        if match:
            col, val = match.groups()
            if col in df.columns:
                df = df[df[col] == val]
            continue

        # Pattern: COLUMN IN ('VAL1', 'VAL2')
        match = re.search(r"(\w+)\s+IN\s*\(([^)]+)\)", cond)
        if match:
            col = match.group(1)
            vals = [v.strip().strip("'").strip('"') for v in match.group(2).split(',')]
            if col in df.columns:
                df = df[df[col].isin(vals)]
            elif f'UPPER({col})' in cond:
                df = df[df[col].str.upper().isin(vals)]
            continue

        # Pattern: UPPER(COLUMN) = 'VALUE'
        match = re.search(r"UPPER\((\w+)\)\s*=\s*'([^']+)'", cond)
        if match:
            col, val = match.groups()
            if col in df.columns:
                df = df[df[col].str.upper() == val]
            continue

        # Pattern: UPPER(COLUMN) IN ('VAL1', 'VAL2')
        match = re.search(r"UPPER\((\w+)\)\s+IN\s*\(([^)]+)\)", cond)
        if match:
            col = match.group(1)
            vals = [v.strip().strip("'").strip('"') for v in match.group(2).split(',')]
            if col in df.columns:
                df = df[df[col].str.upper().isin(vals)]
            continue

        # Pattern: UPPER(COLUMN) LIKE '%VALUE%'
        match = re.search(r"UPPER\((\w+)\)\s+LIKE\s+'%([^%]+)%'", cond)
        if match:
            col, val = match.groups()
            if col in df.columns:
                df = df[df[col].str.upper().str.contains(val, na=False)]
            continue

        # Pattern: UPPER(COLUMN) NOT IN (...)
        match = re.search(r"UPPER\((\w+)\)\s+NOT\s+IN\s*\(([^)]+)\)", cond)
        if match:
            col = match.group(1)
            vals = [v.strip().strip("'").strip('"') for v in match.group(2).split(',')]
            if col in df.columns:
                df = df[~df[col].str.upper().isin(vals)]
            continue

        # Pattern: AGE >= N or AGE > N
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

        # Pattern: ATOXGR = 'N' or ATOXGR IN (...)
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

        # Handle subquery for intersection: USUBJID IN (SELECT ...)
        if 'USUBJID IN (SELECT' in cond:
            # Extract the inner condition
            inner_match = re.search(r"WHERE\s+UPPER\(AEDECOD\)\s*=\s*'([^']+)'", cond)
            if inner_match:
                ae_term = inner_match.group(1)
                # Get USUBJIDs with this AE
                ae_subjects = df[df['AEDECOD'].str.upper() == ae_term]['USUBJID'].unique()
                df = df[df['USUBJID'].isin(ae_subjects)]

    return df


# =============================================================================
# API INTERACTION
# =============================================================================

def login(username: str = DEFAULT_USERNAME, password: str = DEFAULT_PASSWORD) -> str:
    """Login to the API and get access token."""
    try:
        response = requests.post(
            f"{API_BASE_URL}/auth/login",
            params={"username": username, "password": password},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        return data.get("data", {}).get("access_token", "")
    except Exception as e:
        print(f"Login failed: {e}")
        return ""


def query_chat_api(question: str, token: str, conversation_id: str = None) -> Dict[str, Any]:
    """Query the chat API."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    payload = {"message": question}
    if conversation_id:
        payload["conversation_id"] = conversation_id

    try:
        response = requests.post(
            CHAT_API_URL,
            json=payload,
            headers=headers,
            timeout=300
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}


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


# =============================================================================
# VALIDATION LOGIC
# =============================================================================

def run_validation(questions: List[TestQuestion], token: str,
                  flows_only: bool = False) -> List[TestResult]:
    """Run validation against the chat API."""
    results = []
    current_conversation = None
    current_flow = None

    for q in questions:
        # Skip non-flow questions if flows_only
        if flows_only and q.category != "Conversational Flow":
            continue

        # Handle conversation flows
        if q.flow_id:
            if q.flow_id != current_flow:
                # Start new conversation for new flow
                current_flow = q.flow_id
                current_conversation = None
                print(f"\n--- Starting Flow: {q.flow_id} ---")
        else:
            current_conversation = None
            current_flow = None

        print(f"\nQ{q.id}: {q.question}")
        print(f"   Expected: {q.expected_answer}")

        start_time = time.time()
        response = query_chat_api(q.question, token, current_conversation)
        elapsed_ms = (time.time() - start_time) * 1000

        if "error" in response:
            print(f"   ERROR: {response['error']}")
            results.append(TestResult(
                question_id=q.id,
                question=q.question,
                category=q.category,
                expected=q.expected_answer,
                actual="ERROR",
                match=False,
                error=response['error'],
                flow_id=q.flow_id,
                turn=q.turn
            ))
            continue

        # Extract answer and metadata
        answer_text = response.get("content", "")
        metadata = response.get("metadata", {})

        # Update conversation ID for flows
        if q.flow_id:
            current_conversation = response.get("conversation_id")

        # Extract number from answer
        actual_number = extract_number_from_answer(answer_text)

        print(f"   Answer: {answer_text[:150]}...")
        print(f"   Extracted: {actual_number}")

        # Get confidence and SQL
        confidence = metadata.get("confidence", {})
        sql_executed = metadata.get("sql", "")

        print(f"   Confidence: {confidence.get('score', 'N/A')}%")

        # Compare answers
        match = None
        if isinstance(q.expected_answer, int):
            match = actual_number == q.expected_answer
        elif isinstance(q.expected_answer, dict):
            match = None  # Manual check for distributions

        status = 'YES' if match else ('NO' if match is False else 'MANUAL')
        print(f"   MATCH: {status}")

        results.append(TestResult(
            question_id=q.id,
            question=q.question,
            category=q.category,
            expected=q.expected_answer,
            actual=actual_number,
            match=match,
            answer_text=answer_text[:500],
            sql_executed=sql_executed,
            confidence=confidence,
            execution_time_ms=elapsed_ms,
            flow_id=q.flow_id,
            turn=q.turn
        ))

    return results


def print_summary(results: List[TestResult]):
    """Print validation summary."""
    print("\n" + "="*80)
    print("VALIDATION SUMMARY")
    print("="*80)

    total = len(results)
    matches = sum(1 for r in results if r.match is True)
    mismatches = sum(1 for r in results if r.match is False)
    manual = sum(1 for r in results if r.match is None)

    print(f"\nOverall Results:")
    print(f"  Total Questions: {total}")
    print(f"  Matches: {matches}")
    print(f"  Mismatches: {mismatches}")
    print(f"  Manual Check: {manual}")
    print(f"  Accuracy: {matches/total*100:.1f}%" if total > 0 else "  Accuracy: N/A")

    # By category
    print("\nBy Category:")
    categories = defaultdict(lambda: {"total": 0, "match": 0, "mismatch": 0, "manual": 0})
    for r in results:
        cat = r.category
        categories[cat]["total"] += 1
        if r.match is True:
            categories[cat]["match"] += 1
        elif r.match is False:
            categories[cat]["mismatch"] += 1
        else:
            categories[cat]["manual"] += 1

    for cat, stats in sorted(categories.items()):
        acc = stats["match"] / stats["total"] * 100 if stats["total"] > 0 else 0
        print(f"  {cat}: {stats['match']}/{stats['total']} ({acc:.0f}%)")

    # Flow results
    flow_results = [r for r in results if r.flow_id]
    if flow_results:
        print("\nConversation Flows:")
        flows = defaultdict(list)
        for r in flow_results:
            flows[r.flow_id].append(r)

        for flow_id, flow_items in sorted(flows.items()):
            flow_matches = sum(1 for r in flow_items if r.match is True)
            flow_total = len(flow_items)
            status = "PASS" if flow_matches == flow_total else "PARTIAL" if flow_matches > 0 else "FAIL"
            print(f"  {flow_id}: {flow_matches}/{flow_total} turns ({status})")


def generate_html_report(questions: List[TestQuestion], results: List[TestResult],
                        output_path: Path):
    """Generate HTML test report."""

    total = len(results)
    matches = sum(1 for r in results if r.match is True)
    mismatches = sum(1 for r in results if r.match is False)
    manual = sum(1 for r in results if r.match is None)
    accuracy = matches / total * 100 if total > 0 else 0

    # Group by category
    categories = defaultdict(list)
    for r in results:
        categories[r.category].append(r)

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>SAGE Golden Test Suite v2 Results</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        h1 {{ color: #2c3e50; }}
        h2 {{ color: #34495e; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
        .summary {{ background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
        .summary-stats {{ display: flex; gap: 30px; }}
        .stat {{ text-align: center; }}
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
        .sql {{ font-family: monospace; font-size: 12px; background: #f8f9fa; padding: 5px; border-radius: 4px; max-width: 400px; overflow: hidden; text-overflow: ellipsis; }}
        .confidence {{ font-weight: bold; }}
        .confidence-high {{ color: #27ae60; }}
        .confidence-medium {{ color: #f39c12; }}
        .confidence-low {{ color: #e74c3c; }}
        .flow-header {{ background: #9b59b6; color: white; padding: 10px; margin-top: 20px; border-radius: 4px; }}
    </style>
</head>
<body>
    <h1>SAGE Golden Test Suite v2 Results</h1>
    <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

    <div class="summary">
        <h2>Summary</h2>
        <div class="summary-stats">
            <div class="stat">
                <div class="stat-value">{total}</div>
                <div class="stat-label">Total Tests</div>
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
                <div class="stat-label">Manual Check</div>
            </div>
            <div class="stat">
                <div class="stat-value" style="color: {'#27ae60' if accuracy >= 80 else '#f39c12' if accuracy >= 60 else '#e74c3c'};">{accuracy:.1f}%</div>
                <div class="stat-label">Accuracy</div>
            </div>
        </div>
    </div>
"""

    # Add category sections
    for category in ['Populations', 'Demographics', 'Adverse Events', 'Complex Logic',
                     'Fuzzy Matching', 'Negative Testing', 'Conversational Flow']:
        if category not in categories:
            continue

        cat_results = categories[category]
        cat_matches = sum(1 for r in cat_results if r.match is True)
        cat_total = len(cat_results)
        cat_acc = cat_matches / cat_total * 100 if cat_total > 0 else 0

        html += f"""
    <div class="category">
        <h2>{category} ({cat_matches}/{cat_total} - {cat_acc:.0f}%)</h2>
        <table>
            <tr>
                <th>ID</th>
                <th>Question</th>
                <th>Expected</th>
                <th>Actual</th>
                <th>Confidence</th>
                <th>Result</th>
            </tr>
"""

        current_flow = None
        for r in cat_results:
            # Add flow header if needed
            if r.flow_id and r.flow_id != current_flow:
                current_flow = r.flow_id
                html += f'<tr><td colspan="6" class="flow-header">{r.flow_id}</td></tr>'

            result_class = 'result-pass' if r.match is True else ('result-fail' if r.match is False else 'result-manual')
            result_text = 'PASS' if r.match is True else ('FAIL' if r.match is False else 'MANUAL')

            conf_score = r.confidence.get('score', 0) if r.confidence else 0
            conf_class = 'confidence-high' if conf_score >= 90 else ('confidence-medium' if conf_score >= 70 else 'confidence-low')

            html += f"""
            <tr class="{result_class}">
                <td>{r.question_id}</td>
                <td>{r.question[:80]}{'...' if len(r.question) > 80 else ''}</td>
                <td>{r.expected}</td>
                <td>{r.actual if r.actual is not None else 'N/A'}</td>
                <td class="confidence {conf_class}">{conf_score}%</td>
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

    with open(output_path, 'w') as f:
        f.write(html)

    print(f"\nHTML report saved to: {output_path}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='SAGE Golden Test Suite v2')
    parser.add_argument('--test-chat', action='store_true', help='Test against live chat API')
    parser.add_argument('--html', action='store_true', help='Generate HTML report')
    parser.add_argument('--flows-only', action='store_true', help='Test only conversation flows')
    parser.add_argument('--verbose', action='store_true', help='Verbose output')
    args = parser.parse_args()

    print("="*80)
    print("SAGE GOLDEN TEST SUITE v2")
    print("="*80)

    # Load data
    adsl, adae = load_data()

    # Load questions
    print(f"\nLoading questions from {QUESTIONS_FILE}...")
    raw_questions = load_questions()
    print(f"Loaded {len(raw_questions)} questions")

    # Calculate ground truth
    print("\nCalculating ground truth answers...")
    questions = calculate_ground_truth(adsl, adae, raw_questions)

    # Print ground truth summary
    print("\n" + "="*80)
    print("GROUND TRUTH SUMMARY")
    print("="*80)

    categories = defaultdict(list)
    for q in questions:
        categories[q.category].append(q)

    for cat, cat_questions in sorted(categories.items()):
        print(f"\n### {cat} ({len(cat_questions)} questions) ###")
        for q in cat_questions[:3]:  # Show first 3
            print(f"  Q{q.id}: {q.question[:50]}... -> {q.expected_answer}")
        if len(cat_questions) > 3:
            print(f"  ... and {len(cat_questions) - 3} more")

    # Export questions with answers
    export_path = Path(__file__).parent / "validation_questions_v2_with_answers.json"
    export_data = []
    for q in questions:
        export_data.append({
            "id": q.id,
            "question": q.question,
            "category": q.category,
            "expected_answer": q.expected_answer if not isinstance(q.expected_answer, dict) else str(q.expected_answer),
            "sql_template": q.sql_template,
            "answer_type": q.answer_type,
            "notes": q.notes,
            "flow_id": q.flow_id,
            "turn": q.turn
        })

    with open(export_path, 'w') as f:
        json.dump(export_data, f, indent=2)
    print(f"\nQuestions with answers exported to: {export_path}")

    # Run chat validation if requested
    if args.test_chat:
        print("\n" + "="*80)
        print("CHAT API VALIDATION")
        print("="*80)

        print("\nLogging in...")
        token = login()
        if not token:
            print("ERROR: Could not login. Aborting.")
            return
        print("Login successful!")

        results = run_validation(questions, token, args.flows_only)
        print_summary(results)

        if args.html:
            report_path = Path(__file__).parent.parent / "tests" / "golden_test_results_v2.html"
            generate_html_report(questions, results, report_path)

    print("\n" + "="*80)
    print("DONE")
    print("="*80)


if __name__ == "__main__":
    main()
