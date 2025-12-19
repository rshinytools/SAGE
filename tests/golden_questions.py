# SAGE Golden Questions Test Suite
# =================================
"""
Comprehensive test suite with golden questions for validating SAGE pipeline.

This includes:
- Population queries
- Demographics queries
- Adverse event queries
- Follow-up/refinement queries
- Multi-step conversation flows
- Edge cases

Run with: py tests/golden_questions.py
Output: tests/golden_test_results.html
"""

import json
import time
import requests
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional
from pathlib import Path

# Configuration
API_BASE = "http://localhost:8002/api/v1"
AUTH_CREDENTIALS = {"username": "admin", "password": "sage2024"}


@dataclass
class TestQuestion:
    """A single test question."""
    id: str
    question: str
    category: str
    description: str
    expected_behavior: str
    is_followup: bool = False
    depends_on: Optional[str] = None  # ID of question this follows up on


@dataclass
class TestResult:
    """Result of running a test question."""
    question_id: str
    question: str
    category: str
    description: str
    expected_behavior: str
    success: bool
    answer: str = ""
    sql: str = ""
    confidence: Dict[str, Any] = field(default_factory=dict)
    row_count: int = 0
    execution_time_ms: float = 0
    error: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConversationFlow:
    """A multi-turn conversation test."""
    id: str
    name: str
    description: str
    questions: List[TestQuestion]


# =============================================================================
# GOLDEN QUESTIONS
# =============================================================================

GOLDEN_QUESTIONS: List[TestQuestion] = [
    # ---------------------------------------------------------------------
    # CATEGORY 1: Population Queries (Basic counts)
    # ---------------------------------------------------------------------
    TestQuestion(
        id="POP-001",
        question="How many subjects are in the Safety Population?",
        category="Population",
        description="Basic safety population count",
        expected_behavior="Should return count with SAFFL = 'Y' filter"
    ),
    TestQuestion(
        id="POP-002",
        question="How many subjects are in the ITT Population?",
        category="Population",
        description="Basic ITT population count",
        expected_behavior="Should return count with ITTFL = 'Y' filter"
    ),
    TestQuestion(
        id="POP-003",
        question="How many patients are in the efficacy population?",
        category="Population",
        description="Efficacy population count using 'patients' instead of 'subjects'",
        expected_behavior="Should recognize 'patients' as subjects and use EFFFL = 'Y'"
    ),
    TestQuestion(
        id="POP-004",
        question="What is the total number of subjects in the study?",
        category="Population",
        description="Total study population without specific filter",
        expected_behavior="Should count all subjects in ADSL"
    ),

    # ---------------------------------------------------------------------
    # CATEGORY 2: Demographics Queries
    # ---------------------------------------------------------------------
    TestQuestion(
        id="DEMO-001",
        question="How many male subjects are in the safety population?",
        category="Demographics",
        description="Gender filter with population",
        expected_behavior="Should filter by SEX = 'M' and SAFFL = 'Y'"
    ),
    TestQuestion(
        id="DEMO-002",
        question="How many female patients are there?",
        category="Demographics",
        description="Simple gender filter",
        expected_behavior="Should filter by SEX = 'F'"
    ),
    TestQuestion(
        id="DEMO-003",
        question="How many subjects are age 65 or older?",
        category="Demographics",
        description="Age threshold query",
        expected_behavior="Should filter by AGE >= 65"
    ),
    TestQuestion(
        id="DEMO-004",
        question="What is the age distribution of subjects?",
        category="Demographics",
        description="Age distribution/statistics query",
        expected_behavior="Should provide age statistics or distribution"
    ),
    TestQuestion(
        id="DEMO-005",
        question="How many subjects are between 50 and 70 years old?",
        category="Demographics",
        description="Age range query",
        expected_behavior="Should filter by AGE >= 50 AND AGE <= 70"
    ),

    # ---------------------------------------------------------------------
    # CATEGORY 3: Adverse Event Queries
    # ---------------------------------------------------------------------
    TestQuestion(
        id="AE-001",
        question="How many subjects had adverse events?",
        category="Adverse Events",
        description="Basic AE count",
        expected_behavior="Should count distinct subjects with any AE"
    ),
    TestQuestion(
        id="AE-002",
        question="How many subjects reported headache?",
        category="Adverse Events",
        description="Specific AE query",
        expected_behavior="Should match 'headache' to AEDECOD or AELLT"
    ),
    TestQuestion(
        id="AE-003",
        question="How many patients had nausea?",
        category="Adverse Events",
        description="Common AE with 'patients' terminology",
        expected_behavior="Should match 'nausea' in AE data"
    ),
    TestQuestion(
        id="AE-004",
        question="How many subjects had serious adverse events?",
        category="Adverse Events",
        description="Serious AE filter",
        expected_behavior="Should filter by AESER = 'Y'"
    ),
    TestQuestion(
        id="AE-005",
        question="How many subjects had Grade 3 or higher adverse events?",
        category="Adverse Events",
        description="Toxicity grade filter",
        expected_behavior="Should use ATOXGR >= 3 (analysis grade, not AETOXGR)"
    ),
    TestQuestion(
        id="AE-006",
        question="What are the most common adverse events?",
        category="Adverse Events",
        description="Top AE query",
        expected_behavior="Should return AE terms with counts, ordered by frequency"
    ),
    TestQuestion(
        id="AE-007",
        question="How many subjects had treatment-related adverse events?",
        category="Adverse Events",
        description="Related AE filter",
        expected_behavior="Should filter by AEREL containing 'RELATED' or similar"
    ),
    TestQuestion(
        id="AE-008",
        question="How many subjects had diarrhea?",
        category="Adverse Events",
        description="Specific AE - diarrhea (tests spelling variations)",
        expected_behavior="Should match diarrhea/diarrhoea in AE data"
    ),

    # ---------------------------------------------------------------------
    # CATEGORY 4: Combined/Complex Queries
    # ---------------------------------------------------------------------
    TestQuestion(
        id="CPLX-001",
        question="How many female subjects over 60 had serious adverse events?",
        category="Complex",
        description="Multiple filter combination",
        expected_behavior="Should combine SEX, AGE, and AESER filters"
    ),
    TestQuestion(
        id="CPLX-002",
        question="How many subjects in the safety population had Grade 3+ headache?",
        category="Complex",
        description="Population + AE + Grade filter",
        expected_behavior="Should combine SAFFL, headache, and ATOXGR >= 3"
    ),
    TestQuestion(
        id="CPLX-003",
        question="Show subjects who had both nausea and vomiting",
        category="Complex",
        description="Multiple AE combination",
        expected_behavior="Should find subjects with both AEs"
    ),

    # ---------------------------------------------------------------------
    # CATEGORY 5: Non-Clinical Queries (Edge Cases)
    # ---------------------------------------------------------------------
    TestQuestion(
        id="NC-001",
        question="Hi",
        category="Non-Clinical",
        description="Simple greeting",
        expected_behavior="Should return instant helpful response without LLM call"
    ),
    TestQuestion(
        id="NC-002",
        question="What can you help me with?",
        category="Non-Clinical",
        description="Help query",
        expected_behavior="Should explain capabilities"
    ),
    TestQuestion(
        id="NC-003",
        question="Thank you",
        category="Non-Clinical",
        description="Farewell/gratitude",
        expected_behavior="Should respond politely"
    ),

    # ---------------------------------------------------------------------
    # CATEGORY 6: Typo/Fuzzy Matching
    # ---------------------------------------------------------------------
    TestQuestion(
        id="FUZZY-001",
        question="How many patients had headake?",
        category="Fuzzy Matching",
        description="Typo in 'headache'",
        expected_behavior="Should fuzzy match 'headake' to 'HEADACHE'"
    ),
    TestQuestion(
        id="FUZZY-002",
        question="How many subjects reported diarhea?",
        category="Fuzzy Matching",
        description="Typo in 'diarrhea'",
        expected_behavior="Should fuzzy match to correct spelling"
    ),
]


# =============================================================================
# CONVERSATION FLOWS (Multi-turn tests)
# =============================================================================

CONVERSATION_FLOWS: List[ConversationFlow] = [
    ConversationFlow(
        id="FLOW-001",
        name="ITT Population Drill-down",
        description="Tests context preservation across ITT population queries",
        questions=[
            TestQuestion(
                id="FLOW-001-Q1",
                question="How many subjects are in the ITT Population?",
                category="Follow-up Flow",
                description="Initial ITT count",
                expected_behavior="Should return ITT count with ITTFL = 'Y'"
            ),
            TestQuestion(
                id="FLOW-001-Q2",
                question="out of these how many are age 65 or above",
                category="Follow-up Flow",
                description="First refinement - age filter",
                expected_behavior="Should preserve ITTFL = 'Y' AND add AGE >= 65",
                is_followup=True,
                depends_on="FLOW-001-Q1"
            ),
            TestQuestion(
                id="FLOW-001-Q3",
                question="Out of these how many reported serious adverse event",
                category="Follow-up Flow",
                description="Second refinement - SAE filter",
                expected_behavior="Should preserve ITTFL = 'Y' AND AGE >= 65 AND add AESER = 'Y'",
                is_followup=True,
                depends_on="FLOW-001-Q2"
            ),
            TestQuestion(
                id="FLOW-001-Q4",
                question="can you list them",
                category="Follow-up Flow",
                description="List subjects from previous filter",
                expected_behavior="Should transform to SELECT and preserve all filters",
                is_followup=True,
                depends_on="FLOW-001-Q3"
            ),
        ]
    ),
    ConversationFlow(
        id="FLOW-002",
        name="Safety Population with Demographics",
        description="Tests demographic refinements on safety population",
        questions=[
            TestQuestion(
                id="FLOW-002-Q1",
                question="How many subjects are in the Safety Population?",
                category="Follow-up Flow",
                description="Initial safety count",
                expected_behavior="Should return safety count with SAFFL = 'Y'"
            ),
            TestQuestion(
                id="FLOW-002-Q2",
                question="how many of them are female",
                category="Follow-up Flow",
                description="Gender refinement",
                expected_behavior="Should preserve SAFFL = 'Y' AND add SEX = 'F'",
                is_followup=True,
                depends_on="FLOW-002-Q1"
            ),
            TestQuestion(
                id="FLOW-002-Q3",
                question="of those, how many had any adverse events",
                category="Follow-up Flow",
                description="AE filter on female safety subjects",
                expected_behavior="Should preserve SAFFL = 'Y', SEX = 'F' AND add AE condition",
                is_followup=True,
                depends_on="FLOW-002-Q2"
            ),
        ]
    ),
    ConversationFlow(
        id="FLOW-003",
        name="Adverse Event Deep Dive",
        description="Tests AE-focused conversation flow",
        questions=[
            TestQuestion(
                id="FLOW-003-Q1",
                question="How many subjects had headache?",
                category="Follow-up Flow",
                description="Initial AE query",
                expected_behavior="Should count subjects with headache"
            ),
            TestQuestion(
                id="FLOW-003-Q2",
                question="how many of these were Grade 3 or higher",
                category="Follow-up Flow",
                description="Grade filter on headache",
                expected_behavior="Should preserve headache filter AND add ATOXGR >= 3",
                is_followup=True,
                depends_on="FLOW-003-Q1"
            ),
            TestQuestion(
                id="FLOW-003-Q3",
                question="show me the details",
                category="Follow-up Flow",
                description="Request details",
                expected_behavior="Should list subjects with headache grade 3+",
                is_followup=True,
                depends_on="FLOW-003-Q2"
            ),
        ]
    ),
]


# =============================================================================
# TEST RUNNER
# =============================================================================

class GoldenTestRunner:
    """Runs golden question tests and generates HTML report."""

    def __init__(self, api_base: str = API_BASE):
        self.api_base = api_base
        self.token = None
        self.results: List[TestResult] = []
        self.flow_results: Dict[str, List[TestResult]] = {}

    def authenticate(self) -> bool:
        """Get authentication token."""
        try:
            resp = requests.post(
                f"{self.api_base}/auth/login",
                params=AUTH_CREDENTIALS
            )
            if resp.status_code == 200:
                data = resp.json()
                self.token = data['data']['access_token']
                print("[OK] Authenticated successfully")
                return True
            else:
                print(f"[FAIL] Authentication failed: {resp.text}")
                return False
        except Exception as e:
            print(f"[FAIL] Authentication error: {e}")
            return False

    def create_conversation(self, title: str) -> Optional[str]:
        """Create a new conversation."""
        try:
            resp = requests.post(
                f"{self.api_base}/chat/conversations",
                headers={"Authorization": f"Bearer {self.token}"},
                json={"title": title}
            )
            if resp.status_code == 200:
                return resp.json()['id']
            return None
        except Exception as e:
            print(f"Error creating conversation: {e}")
            return None

    def send_message(self, conv_id: str, message: str, timeout: int = 180) -> Dict[str, Any]:
        """Send a message and get response."""
        try:
            resp = requests.post(
                f"{self.api_base}/chat/message",
                headers={"Authorization": f"Bearer {self.token}"},
                json={"conversation_id": conv_id, "message": message},
                timeout=timeout
            )
            if resp.status_code == 200:
                return resp.json()
            return {"error": f"HTTP {resp.status_code}: {resp.text}"}
        except requests.Timeout:
            return {"error": "Request timed out after 180 seconds"}
        except Exception as e:
            return {"error": str(e)}

    def run_single_question(self, question: TestQuestion, conv_id: str) -> TestResult:
        """Run a single test question."""
        print(f"  Running: {question.id} - {question.question[:50]}...")

        start_time = time.time()
        response = self.send_message(conv_id, question.question)
        execution_time = (time.time() - start_time) * 1000

        # Parse response
        if "error" in response and not isinstance(response.get("message"), dict):
            return TestResult(
                question_id=question.id,
                question=question.question,
                category=question.category,
                description=question.description,
                expected_behavior=question.expected_behavior,
                success=False,
                error=response.get("error", "Unknown error"),
                execution_time_ms=execution_time
            )

        # Extract message content
        msg = response.get("message", response)
        if isinstance(msg, dict):
            content = msg.get("content", "")
            metadata = msg.get("metadata", {})
        else:
            content = str(msg)
            metadata = response.get("metadata", {})

        return TestResult(
            question_id=question.id,
            question=question.question,
            category=question.category,
            description=question.description,
            expected_behavior=question.expected_behavior,
            success=metadata.get("success", True),
            answer=content,
            sql=metadata.get("sql", ""),
            confidence=metadata.get("confidence", {}),
            row_count=metadata.get("row_count", 0),
            execution_time_ms=execution_time,
            metadata=metadata
        )

    def run_standalone_questions(self) -> None:
        """Run all standalone (non-flow) questions."""
        print("\n" + "="*60)
        print("RUNNING STANDALONE QUESTIONS")
        print("="*60)

        # Create conversation for standalone questions
        conv_id = self.create_conversation("Golden Test - Standalone")
        if not conv_id:
            print("Failed to create conversation")
            return

        for question in GOLDEN_QUESTIONS:
            # Create new conversation for each standalone question to avoid context bleeding
            conv_id = self.create_conversation(f"Test - {question.id}")
            if conv_id:
                result = self.run_single_question(question, conv_id)
                self.results.append(result)
                status = "[OK]" if result.success else "[FAIL]"
                print(f"    {status} {question.id}: {result.execution_time_ms:.0f}ms")

    def run_conversation_flows(self) -> None:
        """Run multi-turn conversation flow tests."""
        print("\n" + "="*60)
        print("RUNNING CONVERSATION FLOWS")
        print("="*60)

        for flow in CONVERSATION_FLOWS:
            print(f"\n--- {flow.name} ({flow.id}) ---")

            # Create conversation for this flow
            conv_id = self.create_conversation(f"Flow Test - {flow.name}")
            if not conv_id:
                print(f"  Failed to create conversation for flow {flow.id}")
                continue

            flow_results = []
            for question in flow.questions:
                result = self.run_single_question(question, conv_id)
                flow_results.append(result)
                self.results.append(result)
                status = "[OK]" if result.success else "[FAIL]"
                followup = " (follow-up)" if question.is_followup else ""
                print(f"    {status} {question.id}{followup}: {result.execution_time_ms:.0f}ms")

            self.flow_results[flow.id] = flow_results

    def run_all(self) -> None:
        """Run all tests."""
        print("\n" + "="*60)
        print("SAGE GOLDEN QUESTIONS TEST SUITE")
        print("="*60)
        print(f"Started at: {datetime.now().isoformat()}")

        if not self.authenticate():
            print("Cannot proceed without authentication")
            return

        self.run_standalone_questions()
        self.run_conversation_flows()

        # Print summary
        self.print_summary()

    def print_summary(self) -> None:
        """Print test summary."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.success)
        failed = total - passed

        print("\n" + "="*60)
        print("TEST SUMMARY")
        print("="*60)
        print(f"Total:  {total}")
        print(f"Passed: {passed} ({100*passed/total:.1f}%)")
        print(f"Failed: {failed}")

        # Category breakdown
        categories = {}
        for r in self.results:
            cat = r.category
            if cat not in categories:
                categories[cat] = {"total": 0, "passed": 0}
            categories[cat]["total"] += 1
            if r.success:
                categories[cat]["passed"] += 1

        print("\nBy Category:")
        for cat, stats in sorted(categories.items()):
            pct = 100 * stats["passed"] / stats["total"]
            print(f"  {cat}: {stats['passed']}/{stats['total']} ({pct:.0f}%)")

        # Average execution time
        avg_time = sum(r.execution_time_ms for r in self.results) / len(self.results)
        print(f"\nAverage execution time: {avg_time:.0f}ms")

    def generate_html_report(self, output_path: str = None) -> str:
        """Generate HTML report."""
        if output_path is None:
            output_path = Path(__file__).parent / "golden_test_results.html"

        # Calculate stats
        total = len(self.results)
        passed = sum(1 for r in self.results if r.success)
        failed = total - passed
        pass_rate = 100 * passed / total if total > 0 else 0
        avg_time = sum(r.execution_time_ms for r in self.results) / total if total > 0 else 0

        # Build results by category
        categories = {}
        for r in self.results:
            if r.category not in categories:
                categories[r.category] = []
            categories[r.category].append(r)

        # Generate HTML
        html = self._generate_html_content(
            total=total,
            passed=passed,
            failed=failed,
            pass_rate=pass_rate,
            avg_time=avg_time,
            categories=categories,
            flows=CONVERSATION_FLOWS,
            flow_results=self.flow_results
        )

        # Write file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)

        print(f"\nHTML report generated: {output_path}")
        return str(output_path)

    def _generate_html_content(self, total, passed, failed, pass_rate, avg_time,
                               categories, flows, flow_results) -> str:
        """Generate the HTML content."""

        # Build category sections
        category_html = ""
        for cat_name, results in sorted(categories.items()):
            cat_passed = sum(1 for r in results if r.success)
            cat_total = len(results)

            rows = ""
            for r in results:
                status_class = "success" if r.success else "failure"
                status_icon = "✓" if r.success else "✗"

                # Escape HTML in SQL and answer
                sql_escaped = (r.sql or "N/A").replace("<", "&lt;").replace(">", "&gt;")
                answer_escaped = (r.answer or "N/A").replace("<", "&lt;").replace(">", "&gt;")

                # Confidence display
                conf = r.confidence
                if isinstance(conf, dict):
                    conf_score = conf.get("score", "N/A")
                    conf_level = conf.get("level", "")
                else:
                    conf_score = "N/A"
                    conf_level = ""

                rows += f"""
                <tr class="{status_class}">
                    <td><span class="status-icon">{status_icon}</span> {r.question_id}</td>
                    <td class="question-cell">{r.question}</td>
                    <td>{r.description}</td>
                    <td class="answer-cell">{answer_escaped[:200]}{'...' if len(answer_escaped) > 200 else ''}</td>
                    <td><pre class="sql-cell">{sql_escaped}</pre></td>
                    <td>{conf_score} <span class="conf-level">{conf_level}</span></td>
                    <td>{r.row_count}</td>
                    <td>{r.execution_time_ms:.0f}ms</td>
                </tr>
                <tr class="expected-row {status_class}">
                    <td colspan="8"><strong>Expected:</strong> {r.expected_behavior}</td>
                </tr>
                """

            category_html += f"""
            <div class="category-section">
                <h2>{cat_name} <span class="category-stats">({cat_passed}/{cat_total})</span></h2>
                <table>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Question</th>
                            <th>Description</th>
                            <th>Answer</th>
                            <th>SQL</th>
                            <th>Confidence</th>
                            <th>Rows</th>
                            <th>Time</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows}
                    </tbody>
                </table>
            </div>
            """

        # Build flow sections
        flow_html = ""
        for flow in flows:
            flow_res = flow_results.get(flow.id, [])
            if not flow_res:
                continue

            flow_passed = sum(1 for r in flow_res if r.success)
            flow_total = len(flow_res)

            steps = ""
            for i, r in enumerate(flow_res):
                status_class = "success" if r.success else "failure"
                status_icon = "✓" if r.success else "✗"
                sql_escaped = (r.sql or "N/A").replace("<", "&lt;").replace(">", "&gt;")
                answer_escaped = (r.answer or "N/A").replace("<", "&lt;").replace(">", "&gt;")

                steps += f"""
                <div class="flow-step {status_class}">
                    <div class="step-header">
                        <span class="step-number">Step {i+1}</span>
                        <span class="status-icon">{status_icon}</span>
                        <span class="step-id">{r.question_id}</span>
                    </div>
                    <div class="step-question"><strong>Q:</strong> {r.question}</div>
                    <div class="step-answer"><strong>A:</strong> {answer_escaped[:300]}{'...' if len(answer_escaped) > 300 else ''}</div>
                    <div class="step-sql"><strong>SQL:</strong><pre>{sql_escaped}</pre></div>
                    <div class="step-expected"><strong>Expected:</strong> {r.expected_behavior}</div>
                    <div class="step-meta">Time: {r.execution_time_ms:.0f}ms | Rows: {r.row_count}</div>
                </div>
                """

            flow_html += f"""
            <div class="flow-section">
                <h3>{flow.name} <span class="flow-stats">({flow_passed}/{flow_total})</span></h3>
                <p class="flow-desc">{flow.description}</p>
                <div class="flow-steps">
                    {steps}
                </div>
            </div>
            """

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SAGE Golden Questions Test Results</title>
    <style>
        * {{
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
            color: #333;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        .summary-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .summary-card.passed {{
            border-left: 4px solid #27ae60;
        }}
        .summary-card.failed {{
            border-left: 4px solid #e74c3c;
        }}
        .summary-card.total {{
            border-left: 4px solid #3498db;
        }}
        .summary-card.time {{
            border-left: 4px solid #f39c12;
        }}
        .summary-value {{
            font-size: 2em;
            font-weight: bold;
            color: #2c3e50;
        }}
        .summary-label {{
            color: #7f8c8d;
            margin-top: 5px;
        }}
        .category-section {{
            background: white;
            margin: 20px 0;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .category-section h2 {{
            background: #34495e;
            color: white;
            margin: 0;
            padding: 15px 20px;
        }}
        .category-stats {{
            font-weight: normal;
            font-size: 0.8em;
            opacity: 0.8;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ecf0f1;
        }}
        th {{
            background: #ecf0f1;
            font-weight: 600;
        }}
        tr.success {{
            background: #f0fff0;
        }}
        tr.failure {{
            background: #fff0f0;
        }}
        tr.expected-row {{
            font-size: 0.85em;
            color: #666;
        }}
        tr.expected-row.success {{
            background: #e8f8e8;
        }}
        tr.expected-row.failure {{
            background: #f8e8e8;
        }}
        .status-icon {{
            font-weight: bold;
            margin-right: 5px;
        }}
        tr.success .status-icon {{
            color: #27ae60;
        }}
        tr.failure .status-icon {{
            color: #e74c3c;
        }}
        .question-cell {{
            max-width: 250px;
        }}
        .answer-cell {{
            max-width: 300px;
            font-size: 0.9em;
        }}
        .sql-cell {{
            max-width: 300px;
            font-size: 0.8em;
            background: #f8f9fa;
            padding: 8px;
            border-radius: 4px;
            overflow-x: auto;
            white-space: pre-wrap;
            word-break: break-word;
            margin: 0;
        }}
        .conf-level {{
            font-size: 0.8em;
            color: #666;
        }}

        /* Flow sections */
        .flows-container {{
            margin-top: 40px;
        }}
        .flows-container h2 {{
            color: #2c3e50;
            border-bottom: 2px solid #9b59b6;
            padding-bottom: 10px;
        }}
        .flow-section {{
            background: white;
            margin: 20px 0;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .flow-section h3 {{
            background: #8e44ad;
            color: white;
            margin: 0;
            padding: 15px 20px;
        }}
        .flow-stats {{
            font-weight: normal;
            font-size: 0.8em;
            opacity: 0.8;
        }}
        .flow-desc {{
            padding: 10px 20px;
            color: #666;
            margin: 0;
            border-bottom: 1px solid #ecf0f1;
        }}
        .flow-steps {{
            padding: 20px;
        }}
        .flow-step {{
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 15px;
        }}
        .flow-step.success {{
            border-left: 4px solid #27ae60;
            background: #f8fff8;
        }}
        .flow-step.failure {{
            border-left: 4px solid #e74c3c;
            background: #fff8f8;
        }}
        .step-header {{
            display: flex;
            align-items: center;
            gap: 10px;
            margin-bottom: 10px;
        }}
        .step-number {{
            background: #3498db;
            color: white;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.85em;
        }}
        .step-id {{
            color: #666;
            font-size: 0.85em;
        }}
        .step-question {{
            font-size: 1.1em;
            margin-bottom: 10px;
        }}
        .step-answer {{
            background: #f8f9fa;
            padding: 10px;
            border-radius: 4px;
            margin-bottom: 10px;
        }}
        .step-sql pre {{
            background: #2c3e50;
            color: #ecf0f1;
            padding: 10px;
            border-radius: 4px;
            overflow-x: auto;
            font-size: 0.85em;
        }}
        .step-expected {{
            color: #666;
            font-size: 0.9em;
            margin-top: 10px;
            padding-top: 10px;
            border-top: 1px dashed #ddd;
        }}
        .step-meta {{
            font-size: 0.85em;
            color: #888;
            margin-top: 10px;
        }}

        /* Timestamp */
        .timestamp {{
            text-align: right;
            color: #888;
            font-size: 0.9em;
            margin-top: 40px;
        }}

        /* Filter controls */
        .filters {{
            margin: 20px 0;
            padding: 15px;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .filters label {{
            margin-right: 20px;
            cursor: pointer;
        }}
        .filters input {{
            margin-right: 5px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🧪 SAGE Golden Questions Test Results</h1>

        <div class="summary">
            <div class="summary-card total">
                <div class="summary-value">{total}</div>
                <div class="summary-label">Total Tests</div>
            </div>
            <div class="summary-card passed">
                <div class="summary-value">{passed}</div>
                <div class="summary-label">Passed</div>
            </div>
            <div class="summary-card failed">
                <div class="summary-value">{failed}</div>
                <div class="summary-label">Failed</div>
            </div>
            <div class="summary-card total">
                <div class="summary-value">{pass_rate:.1f}%</div>
                <div class="summary-label">Pass Rate</div>
            </div>
            <div class="summary-card time">
                <div class="summary-value">{avg_time:.0f}ms</div>
                <div class="summary-label">Avg Time</div>
            </div>
        </div>

        <div class="filters">
            <strong>Filter:</strong>
            <label><input type="checkbox" id="showPassed" checked onchange="filterResults()"> Show Passed</label>
            <label><input type="checkbox" id="showFailed" checked onchange="filterResults()"> Show Failed</label>
        </div>

        {category_html}

        <div class="flows-container">
            <h2>📝 Conversation Flow Tests</h2>
            {flow_html}
        </div>

        <div class="timestamp">
            Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </div>

    <script>
        function filterResults() {{
            const showPassed = document.getElementById('showPassed').checked;
            const showFailed = document.getElementById('showFailed').checked;

            document.querySelectorAll('tr.success, tr.expected-row.success').forEach(el => {{
                el.style.display = showPassed ? '' : 'none';
            }});
            document.querySelectorAll('tr.failure, tr.expected-row.failure').forEach(el => {{
                el.style.display = showFailed ? '' : 'none';
            }});
            document.querySelectorAll('.flow-step.success').forEach(el => {{
                el.style.display = showPassed ? '' : 'none';
            }});
            document.querySelectorAll('.flow-step.failure').forEach(el => {{
                el.style.display = showFailed ? '' : 'none';
            }});
        }}
    </script>
</body>
</html>
"""


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Run the golden questions test suite."""
    runner = GoldenTestRunner()
    runner.run_all()
    runner.generate_html_report()


if __name__ == "__main__":
    main()
