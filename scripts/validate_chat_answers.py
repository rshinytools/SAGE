"""
SAGE Chat Validation Script
============================
This script validates the AI chat responses against actual data.

It:
1. Defines test questions
2. Calculates ground truth answers from the actual data
3. Queries the chat API with the same questions
4. Compares and reports results
"""

import pandas as pd
import json
import requests
from pathlib import Path
from typing import Dict, Any, List
from dataclasses import dataclass

# Paths
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
ADSL_PATH = DATA_DIR / "adsl.parquet"
ADAE_PATH = DATA_DIR / "adae.parquet"

# Chat API configuration
API_BASE_URL = "http://localhost:8002/api/v1"
CHAT_API_URL = f"{API_BASE_URL}/chat/message"
AUTH_TOKEN = ""  # Will be set after login

# Default credentials
DEFAULT_USERNAME = "admin"
DEFAULT_PASSWORD = "sage2024"


@dataclass
class TestQuestion:
    """A test question with expected answer."""
    id: int
    question: str
    expected_answer: Any
    sql_description: str
    category: str


def load_data():
    """Load ADSL and ADAE data."""
    print("Loading data...")
    adsl = pd.read_parquet(ADSL_PATH)
    adae = pd.read_parquet(ADAE_PATH)
    print(f"  ADSL: {len(adsl)} rows")
    print(f"  ADAE: {len(adae)} rows")
    return adsl, adae


def calculate_ground_truth(adsl: pd.DataFrame, adae: pd.DataFrame) -> List[TestQuestion]:
    """
    Calculate ground truth answers for all test questions.

    Returns list of TestQuestion objects with expected answers.
    """
    questions = []

    # ==========================================================================
    # DEMOGRAPHICS QUESTIONS (ADSL)
    # ==========================================================================

    # Q1: How many subjects are in the safety population?
    q1_answer = len(adsl[adsl['SAFFL'] == 'Y'])
    questions.append(TestQuestion(
        id=1,
        question="How many subjects are in the safety population?",
        expected_answer=q1_answer,
        sql_description="SELECT COUNT(*) FROM adsl WHERE SAFFL = 'Y'",
        category="Demographics"
    ))

    # Q2: What is the gender distribution of patients?
    q2_answer = adsl['SEX'].value_counts().to_dict()
    questions.append(TestQuestion(
        id=2,
        question="What is the gender distribution of patients?",
        expected_answer=q2_answer,
        sql_description="SELECT SEX, COUNT(*) FROM adsl GROUP BY SEX",
        category="Demographics"
    ))

    # Q3: How many patients are over 65 years old?
    q3_answer = len(adsl[adsl['AGE'] > 65])
    questions.append(TestQuestion(
        id=3,
        question="How many patients are over 65 years old?",
        expected_answer=q3_answer,
        sql_description="SELECT COUNT(*) FROM adsl WHERE AGE > 65",
        category="Demographics"
    ))

    # ==========================================================================
    # ADVERSE EVENT QUESTIONS (ADAE)
    # ==========================================================================

    # Q4: How many patients had nausea?
    q4_answer = adae[adae['AEDECOD'].str.upper() == 'NAUSEA']['USUBJID'].nunique()
    questions.append(TestQuestion(
        id=4,
        question="How many patients had nausea?",
        expected_answer=q4_answer,
        sql_description="SELECT COUNT(DISTINCT USUBJID) FROM adae WHERE UPPER(AEDECOD) = 'NAUSEA'",
        category="Adverse Events"
    ))

    # Q5: How many patients had serious adverse events?
    q5_answer = adae[adae['AESER'] == 'Y']['USUBJID'].nunique()
    questions.append(TestQuestion(
        id=5,
        question="How many patients had serious adverse events?",
        expected_answer=q5_answer,
        sql_description="SELECT COUNT(DISTINCT USUBJID) FROM adae WHERE AESER = 'Y'",
        category="Adverse Events"
    ))

    # Q6: What are the top 5 most common adverse events?
    q6_answer = adae['AEDECOD'].value_counts().head(5).to_dict()
    questions.append(TestQuestion(
        id=6,
        question="What are the top 5 most common adverse events?",
        expected_answer=q6_answer,
        sql_description="SELECT AEDECOD, COUNT(*) FROM adae GROUP BY AEDECOD ORDER BY COUNT(*) DESC LIMIT 5",
        category="Adverse Events"
    ))

    # Q7: How many patients had anaemia?
    q7_answer = adae[adae['AEDECOD'].str.upper() == 'ANAEMIA']['USUBJID'].nunique()
    questions.append(TestQuestion(
        id=7,
        question="How many patients had anaemia?",
        expected_answer=q7_answer,
        sql_description="SELECT COUNT(DISTINCT USUBJID) FROM adae WHERE UPPER(AEDECOD) = 'ANAEMIA'",
        category="Adverse Events"
    ))

    # Q8: How many adverse event records are there in total?
    q8_answer = len(adae)
    questions.append(TestQuestion(
        id=8,
        question="How many adverse event records are there in total?",
        expected_answer=q8_answer,
        sql_description="SELECT COUNT(*) FROM adae",
        category="Adverse Events"
    ))

    # ==========================================================================
    # COMBINED/POPULATION QUESTIONS
    # ==========================================================================

    # Q9: How many female patients had adverse events?
    q9_answer = adae[adae['SEX'] == 'F']['USUBJID'].nunique()
    questions.append(TestQuestion(
        id=9,
        question="How many female patients had adverse events?",
        expected_answer=q9_answer,
        sql_description="SELECT COUNT(DISTINCT USUBJID) FROM adae WHERE SEX = 'F'",
        category="Combined"
    ))

    # Q10: How many treatment-emergent adverse events occurred?
    q10_answer = len(adae[adae['TRTEMFL'] == 'Y'])
    questions.append(TestQuestion(
        id=10,
        question="How many treatment-emergent adverse events occurred?",
        expected_answer=q10_answer,
        sql_description="SELECT COUNT(*) FROM adae WHERE TRTEMFL = 'Y'",
        category="Combined"
    ))

    # ==========================================================================
    # COMPLEX QUESTIONS (User-added)
    # ==========================================================================

    # Q11: How many patients had anaemia and also had Hypertension?
    anaemia_subjects = set(adae[adae['AEDECOD'].str.upper() == 'ANAEMIA']['USUBJID'])
    hypertension_subjects = set(adae[adae['AEDECOD'].str.upper() == 'HYPERTENSION']['USUBJID'])
    q11_answer = len(anaemia_subjects & hypertension_subjects)
    questions.append(TestQuestion(
        id=11,
        question="How many patients had anaemia and also had Hypertension?",
        expected_answer=q11_answer,
        sql_description="""
            SELECT COUNT(DISTINCT a.USUBJID) FROM adae a
            WHERE a.USUBJID IN (SELECT USUBJID FROM adae WHERE UPPER(AEDECOD) = 'ANAEMIA')
            AND a.USUBJID IN (SELECT USUBJID FROM adae WHERE UPPER(AEDECOD) = 'HYPERTENSION')
        """,
        category="Complex"
    ))

    # Q12: How many subjects reported Grade 3+ Hypertension?
    # Check which grade column exists and has data
    grade_col = 'ATOXGR' if 'ATOXGR' in adae.columns else 'AETOXGR'
    hypertension_df = adae[adae['AEDECOD'].str.upper() == 'HYPERTENSION']

    # Convert grade to numeric for comparison
    def get_grade(val):
        if pd.isna(val):
            return 0
        try:
            return int(str(val).replace('Grade ', '').strip())
        except:
            return 0

    hypertension_df = hypertension_df.copy()
    hypertension_df['grade_num'] = hypertension_df[grade_col].apply(get_grade)
    q12_answer = hypertension_df[hypertension_df['grade_num'] >= 3]['USUBJID'].nunique()
    questions.append(TestQuestion(
        id=12,
        question="How many subjects reported Grade 3+ Hypertension?",
        expected_answer=q12_answer,
        sql_description=f"SELECT COUNT(DISTINCT USUBJID) FROM adae WHERE UPPER(AEDECOD) = 'HYPERTENSION' AND {grade_col} >= 3",
        category="Complex"
    ))

    # Q13: How many subjects in safety population reported Grade 2 Diarrhoea?
    # Note: Check spelling - could be "Diarrhoea" or "Diarrhea"
    safety_subjects = set(adsl[adsl['SAFFL'] == 'Y']['USUBJID'])
    diarrhea_terms = ['DIARRHOEA', 'DIARRHEA']
    diarrhea_df = adae[adae['AEDECOD'].str.upper().isin(diarrhea_terms)]
    diarrhea_df = diarrhea_df.copy()
    diarrhea_df['grade_num'] = diarrhea_df[grade_col].apply(get_grade)
    grade2_diarrhea = diarrhea_df[diarrhea_df['grade_num'] == 2]
    q13_answer = len(set(grade2_diarrhea['USUBJID']) & safety_subjects)
    questions.append(TestQuestion(
        id=13,
        question="How many subjects in safety population reported Grade 2 Diarrhoea?",
        expected_answer=q13_answer,
        sql_description=f"""
            SELECT COUNT(DISTINCT USUBJID) FROM adae
            WHERE UPPER(AEDECOD) IN ('DIARRHOEA', 'DIARRHEA')
            AND {grade_col} = 2
            AND SAFFL = 'Y'
        """,
        category="Complex"
    ))

    # Q14: How many subjects who are male and reported Nausea of Grade 2 or higher?
    nausea_df = adae[adae['AEDECOD'].str.upper() == 'NAUSEA']
    nausea_df = nausea_df.copy()
    nausea_df['grade_num'] = nausea_df[grade_col].apply(get_grade)
    male_nausea_grade2plus = nausea_df[(nausea_df['SEX'] == 'M') & (nausea_df['grade_num'] >= 2)]
    q14_answer = male_nausea_grade2plus['USUBJID'].nunique()
    questions.append(TestQuestion(
        id=14,
        question="How many subjects who are male reported Nausea of Grade 2 or higher?",
        expected_answer=q14_answer,
        sql_description=f"""
            SELECT COUNT(DISTINCT USUBJID) FROM adae
            WHERE UPPER(AEDECOD) = 'NAUSEA'
            AND SEX = 'M'
            AND {grade_col} >= 2
        """,
        category="Complex"
    ))

    return questions


def print_ground_truth(questions: List[TestQuestion]):
    """Print ground truth answers."""
    print("\n" + "="*80)
    print("GROUND TRUTH ANSWERS (from actual data)")
    print("="*80)

    current_category = None
    for q in questions:
        if q.category != current_category:
            current_category = q.category
            print(f"\n### {current_category} ###")

        print(f"\nQ{q.id}: {q.question}")
        print(f"   Answer: {q.expected_answer}")


def login(username: str = DEFAULT_USERNAME, password: str = DEFAULT_PASSWORD) -> str:
    """
    Login to the API and get an access token.

    Returns the access token or raises an exception.
    """
    try:
        response = requests.post(
            f"{API_BASE_URL}/auth/login",
            params={"username": username, "password": password},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        return data.get("data", {}).get("access_token", "")
    except requests.exceptions.RequestException as e:
        print(f"Login failed: {e}")
        return ""


def query_chat_api(question: str, token: str = "", api_url: str = CHAT_API_URL) -> Dict[str, Any]:
    """
    Query the chat API with a question.

    Returns the response including answer and metadata.
    """
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        response = requests.post(
            api_url,
            json={"message": question},
            headers=headers,
            timeout=300  # 5 minutes for slow reasoning models
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}


def extract_number_from_answer(answer: str) -> int:
    """Try to extract a number from the answer text."""
    import re
    # Look for bold numbers like **48**
    bold_match = re.search(r'\*\*(\d+(?:,\d{3})*)\*\*', answer)
    if bold_match:
        return int(bold_match.group(1).replace(',', ''))

    # Look for any number
    numbers = re.findall(r'\b(\d+(?:,\d{3})*)\b', answer)
    if numbers:
        return int(numbers[0].replace(',', ''))

    return None


def run_validation(questions: List[TestQuestion], test_chat: bool = True):
    """
    Run validation against the chat API.
    """
    if not test_chat:
        print("\n[Skipping chat API tests - test_chat=False]")
        return

    print("\n" + "="*80)
    print("CHAT API VALIDATION")
    print("="*80)

    # Login first
    print("\nLogging in to API...")
    token = login()
    if not token:
        print("ERROR: Could not login to API. Aborting validation.")
        return None
    print("Login successful!")

    results = []

    for q in questions:
        print(f"\nQ{q.id}: {q.question}")
        print(f"   Expected: {q.expected_answer}")

        response = query_chat_api(q.question, token=token)

        if "error" in response:
            print(f"   ERROR: {response['error']}")
            results.append({
                "question_id": q.id,
                "question": q.question,
                "expected": q.expected_answer,
                "actual": "ERROR",
                "match": False,
                "error": response['error']
            })
            continue

        # Extract answer from response
        answer_text = response.get("content", "")
        metadata = response.get("metadata", {})

        # Try to extract number from answer
        actual_number = extract_number_from_answer(answer_text)

        print(f"   Chat Answer: {answer_text[:200]}...")
        print(f"   Extracted Number: {actual_number}")

        # Check if pipeline was used
        pipeline_used = metadata.get("pipeline_used", False)
        confidence = metadata.get("confidence", {})

        print(f"   Pipeline Used: {pipeline_used}")
        print(f"   Confidence: {confidence.get('score', 'N/A')}%")

        # Compare answers (for numeric questions)
        if isinstance(q.expected_answer, int):
            match = actual_number == q.expected_answer
        else:
            match = None  # Can't auto-compare complex answers

        print(f"   MATCH: {'YES' if match else 'NO' if match is not None else 'MANUAL CHECK NEEDED'}")

        results.append({
            "question_id": q.id,
            "question": q.question,
            "expected": q.expected_answer,
            "actual": actual_number,
            "answer_text": answer_text[:500],
            "match": match,
            "pipeline_used": pipeline_used,
            "confidence": confidence
        })

    # Summary
    print("\n" + "="*80)
    print("VALIDATION SUMMARY")
    print("="*80)

    total = len(results)
    matches = sum(1 for r in results if r.get("match") is True)
    mismatches = sum(1 for r in results if r.get("match") is False)
    manual = sum(1 for r in results if r.get("match") is None)

    print(f"Total Questions: {total}")
    print(f"Matches: {matches}")
    print(f"Mismatches: {mismatches}")
    print(f"Manual Check Needed: {manual}")
    print(f"Accuracy: {matches/total*100:.1f}%" if total > 0 else "N/A")

    return results


def main():
    """Main function."""
    print("="*80)
    print("SAGE CHAT VALIDATION SCRIPT")
    print("="*80)

    # Load data
    adsl, adae = load_data()

    # Calculate ground truth
    questions = calculate_ground_truth(adsl, adae)

    # Print ground truth
    print_ground_truth(questions)

    # Ask user if they want to test the chat API
    print("\n" + "-"*80)
    print("Ground truth calculations complete.")
    print("To test against the chat API, run with --test-chat flag")
    print("Or call run_validation(questions, test_chat=True)")
    print("-"*80)

    # Export questions for reference
    export_data = []
    for q in questions:
        export_data.append({
            "id": q.id,
            "question": q.question,
            "expected_answer": q.expected_answer if not isinstance(q.expected_answer, dict) else str(q.expected_answer),
            "category": q.category,
            "sql": q.sql_description.strip()
        })

    output_file = Path(__file__).parent / "validation_questions.json"
    with open(output_file, 'w') as f:
        json.dump(export_data, f, indent=2)
    print(f"\nQuestions exported to: {output_file}")

    return questions


if __name__ == "__main__":
    import sys

    questions = main()

    if "--test-chat" in sys.argv:
        run_validation(questions, test_chat=True)
