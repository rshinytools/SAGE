# Your First Query

This tutorial walks you through asking your first question to SAGE.

---

## Prerequisites

Before starting, ensure:

- [ ] SAGE services are running (`docker compose ps`)
- [ ] Clinical data has been loaded (Factory 1)
- [ ] Metadata has been approved (Factory 2)
- [ ] Dictionary indexes are built (Factory 3)

---

## Step 1: Access the Chat Interface

Open your browser and navigate to:

```
http://localhost
```

You'll see the SAGE chat interface.

---

## Step 2: Authenticate

1. Enter your credentials (default: `admin` / check with administrator)
2. Click **Login**

---

## Step 3: Ask a Simple Question

Start with a straightforward query:

```
How many subjects are in the study?
```

**Expected Response:**

```
Based on the ADSL (Subject-Level Analysis Dataset), there are 233 subjects
enrolled in the study.

Confidence: 98% (HIGH)

Methodology:
- Table: ADSL
- Population: Intent-to-Treat (ITTFL='Y')
- Column: USUBJID (count distinct)
```

---

## Step 4: Try More Complex Queries

### Demographics Query

```
What is the average age of female subjects?
```

### Adverse Events Query

```
How many subjects had headache?
```

### Population-Specific Query

```
Count serious adverse events in the safety population
```

### Grade-Based Query

```
How many Grade 3 or higher adverse events occurred?
```

---

## Understanding the Response

Each SAGE response includes:

### 1. Answer

The main result in plain English.

### 2. Confidence Score

| Badge | Meaning |
|-------|---------|
| GREEN (90-100%) | High confidence - result is reliable |
| YELLOW (70-89%) | Medium confidence - verify assumptions |
| ORANGE (50-69%) | Low confidence - review the SQL |
| RED (<50%) | Cannot provide reliable answer |

### 3. Methodology

Transparency about how the answer was derived:

- **Table Used**: Which dataset was queried
- **Population**: Which population filter was applied
- **Columns**: Which columns were used
- **Assumptions**: Any assumptions made

### 4. SQL Query

Click **"View SQL"** to see the actual query executed:

```sql
SELECT COUNT(DISTINCT USUBJID) AS subject_count
FROM ADSL
WHERE ITTFL = 'Y'
```

---

## Query Best Practices

### Be Specific

Instead of:
```
How many AEs?
```

Try:
```
How many subjects had at least one adverse event?
```

### Specify Population

```
In the safety population, how many subjects had nausea?
```

### Use Clinical Terms

SAGE understands:

- Medical terms: `pyrexia` (fever), `dyspnoea` (shortness of breath)
- UK/US spellings: `anaemia` and `anemia`
- Common synonyms: `belly pain` → `ABDOMINAL PAIN`

### Ask Follow-Up Questions

SAGE maintains conversation context:

```
User: How many subjects had headache?
SAGE: 45 subjects had headache.

User: Of those, how many were Grade 3 or higher?
SAGE: Of the 45 headache subjects, 3 had Grade 3 or higher severity.
```

---

## Example Queries by Category

### Subject Counts

```
How many subjects completed the study?
What is the dropout rate?
Count subjects by treatment arm
```

### Demographics

```
What is the age distribution?
How many male vs female subjects?
Average weight by treatment group
```

### Adverse Events

```
Most common adverse events
Serious adverse events by system organ class
Treatment-related AEs in the active arm
```

### Laboratory Data

```
Average hemoglobin at baseline
Subjects with elevated liver enzymes
Lab values outside normal range
```

---

## Troubleshooting

### Low Confidence Score

If you see a YELLOW or ORANGE confidence:

1. Review the SQL query
2. Check if the correct table was used
3. Verify entity matching was correct

### No Results

If SAGE can't answer:

1. Rephrase the question
2. Be more specific about what you're asking
3. Check if the data exists in the loaded datasets

### Term Not Found

If a medical term isn't recognized:

1. Try the standard MedDRA preferred term
2. Try both UK and US spellings
3. Use quotation marks for exact matches

---

## Next Steps

- [Learn effective question techniques](../user-guide/asking-questions.md)
- [Understand confidence scoring](../user-guide/confidence-scores.md)
- [Explore example queries](../user-guide/example-queries.md)
