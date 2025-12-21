# Understanding Confidence Scores

Every SAGE answer includes a confidence score to help you understand reliability.

---

## What is a Confidence Score?

The confidence score (0-100%) indicates how reliable an answer is based on:

- How well terms were matched
- Whether columns are documented
- If the query executed successfully
- If results make sense

---

## Confidence Levels

### HIGH (90-100%) - GREEN

```
Confidence: 95% ████████████░ HIGH
```

**What it means:**
- All terms matched exactly or with high confidence
- All columns are well documented
- Query executed without issues
- Results are reasonable

**What to do:**
- Trust this result for reports and analysis
- Safe to cite in documentation

### MEDIUM (70-89%) - YELLOW

```
Confidence: 78% ████████░░░░ MEDIUM
```

**What it means:**
- Some terms had fuzzy matches
- Minor assumptions were made
- May need verification

**What to do:**
- Review the methodology
- Verify assumptions make sense
- Consider rephrasing for higher confidence

### LOW (50-69%) - ORANGE

```
Confidence: 55% █████░░░░░░░ LOW
```

**What it means:**
- Terms had low-quality matches
- Significant assumptions were made
- Results may not be accurate

**What to do:**
- Carefully review the SQL query
- Check term resolution
- Rephrase your question
- Consult with data team

### VERY LOW (<50%) - RED

```
Confidence: 35% ███░░░░░░░░░ VERY LOW
```

**What it means:**
- Could not reliably answer the question
- Terms were not found
- Query may have failed

**What to do:**
- Do not use this result
- Rephrase completely
- Check if data exists
- Contact support

---

## Score Components

### Entity Resolution (40%)

How well your terms matched the data:

| Scenario | Score |
|----------|-------|
| "headache" matched exactly | 100% |
| "headche" fuzzy matched (95%) | 95% |
| "belly pain" → ABDOMINAL PAIN | 100% |
| Term not found | 0% |

### Metadata Coverage (30%)

How well the queried columns are documented:

| Scenario | Score |
|----------|-------|
| All columns in Golden Metadata | 100% |
| Most columns documented | 90% |
| Some documentation gaps | 70% |

### Execution Success (20%)

Whether the query ran properly:

| Scenario | Score |
|----------|-------|
| Query succeeded with results | 100% |
| Query succeeded, zero rows | 90% |
| Timeout but recovered | 70% |
| Query failed | 0% |

### Result Sanity (10%)

Whether results make sense:

| Scenario | Score |
|----------|-------|
| Count is positive for existence query | 100% |
| Percentage is 0-100% | 100% |
| Zero results when unexpected | 70% |

---

## Viewing Score Details

Click the confidence badge to see breakdown:

```
┌──────────────────────────────────────┐
│ Confidence Score Breakdown           │
├──────────────────────────────────────┤
│ Overall Score: 92%                   │
│ Level: HIGH                          │
│                                      │
│ Components:                          │
│ ├─ Entity Resolution: 95%            │
│ │   "headache" matched exactly       │
│ ├─ Metadata Coverage: 100%           │
│ │   All columns documented           │
│ ├─ Execution Success: 100%           │
│ │   Query completed successfully     │
│ └─ Result Sanity: 85%                │
│     Count of 45 is reasonable        │
└──────────────────────────────────────┘
```

---

## Improving Confidence

### Use Exact Terms

```
Low:  "stomach issues"
High: "ABDOMINAL PAIN"
```

### Be Specific

```
Low:  "How many problems?"
High: "How many adverse events in safety population?"
```

### Use Known Terminology

```
Low:  "high blood pressure medication"
High: "antihypertensive concomitant medications"
```

### Check Spelling

```
Low:  "diarrohea" (typo)
High: "diarrhoea" or "diarrhea"
```

---

## When to Trust Results

| Confidence | Use For |
|------------|---------|
| HIGH (90%+) | Reports, regulatory submissions |
| MEDIUM (70-89%) | Internal analysis, verification needed |
| LOW (50-69%) | Exploratory only, not for decisions |
| VERY LOW (<50%) | Do not use |

---

## FAQ

### Why is my confidence low?

Common reasons:
- Term not found in data
- Typo in query
- Using informal language
- Data doesn't exist

### Can I still use a MEDIUM result?

Yes, but:
1. Review the methodology
2. Verify assumptions
3. Document any caveats

### What if confidence is always low?

Contact your administrator to:
- Check if data is loaded
- Verify metadata is approved
- Rebuild dictionary indexes

---

## Next Steps

- [Asking Questions Effectively](asking-questions.md)
- [Example Queries](example-queries.md)
- [Chat Interface Guide](chat-interface.md)
