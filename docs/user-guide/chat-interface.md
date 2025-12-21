# Chat Interface

Learn how to use the SAGE chat interface to query clinical trial data.

---

## Getting Started

### Accessing the Chat

1. Open your browser to `http://localhost`
2. Log in with your credentials
3. You'll see the chat interface

### Interface Layout

```
┌─────────────────────────────────────────────────────────┐
│  SAGE - Study Analytics Generative Engine               │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Welcome to SAGE!                                 │   │
│  │ Ask questions about your clinical trial data.   │   │
│  └─────────────────────────────────────────────────┘   │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │ How many subjects are in the study?             │   │
│  └─────────────────────────────────────────────────┘   │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │ There are 233 subjects enrolled in the study.   │   │
│  │ Confidence: 98% ████████████░ HIGH              │   │
│  │ [View SQL] [View Methodology]                   │   │
│  └─────────────────────────────────────────────────┘   │
│                                                         │
├─────────────────────────────────────────────────────────┤
│  Type your question...                          [Send]  │
└─────────────────────────────────────────────────────────┘
```

---

## Asking Questions

### Simple Questions

Type your question naturally:

```
How many subjects completed the study?
```

### Specific Questions

Be specific for better results:

```
How many female subjects over 65 had serious adverse events?
```

### Follow-up Questions

SAGE remembers context:

```
You: How many subjects had headache?
SAGE: 45 subjects had headache.

You: Of those, how many were Grade 3 or higher?
SAGE: 3 of the 45 headache subjects had Grade 3 or higher severity.
```

---

## Understanding Responses

### Answer Section

The main result in plain English:

```
45 subjects experienced headache in the safety population.
```

### Confidence Badge

Shows how reliable the answer is:

| Badge | Meaning |
|-------|---------|
| GREEN (90-100%) | High confidence - trust this result |
| YELLOW (70-89%) | Medium confidence - verify assumptions |
| ORANGE (50-69%) | Low confidence - review methodology |
| RED (<50%) | Cannot provide reliable answer |

### View SQL

Click to see the query:

```sql
SELECT COUNT(DISTINCT USUBJID)
FROM ADAE
WHERE SAFFL = 'Y'
  AND UPPER(AEDECOD) = 'HEADACHE'
```

### View Methodology

See how the answer was derived:

- **Table Used**: ADAE
- **Population**: Safety (SAFFL='Y')
- **Entity Resolution**: headache → HEADACHE (100% match)
- **Assumptions**: Using safety population by default

---

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Enter` | Send message |
| `Shift+Enter` | New line |
| `Ctrl+/` | Show help |
| `Esc` | Clear input |

---

## Features

### Export Results

Click the export icon to download:

- CSV format for data tables
- PDF format for reports

### Copy SQL

One-click copy of generated SQL for use in other tools.

### History

View previous conversations using the history panel.

### New Conversation

Start fresh with the "New Chat" button.

---

## Tips for Better Results

### Be Specific About Population

```
❌ How many AEs?
✅ How many adverse events in the safety population?
```

### Use Clinical Terms

SAGE understands medical terminology:

```
✅ Subjects with pyrexia (fever)
✅ Count dyspnoea events (shortness of breath)
✅ Patients with cephalalgia (headache)
```

### Specify Timeframes

```
✅ Treatment-emergent adverse events
✅ AEs during the study period
```

### Ask About Specific Grades

```
✅ Grade 3 or higher adverse events
✅ Serious adverse events (SAE)
```

---

## Troubleshooting

### Low Confidence Score

If you see YELLOW or ORANGE:

1. Check the methodology for assumptions
2. Review the SQL query
3. Rephrase with more specific terms

### Term Not Found

If a term isn't recognized:

1. Try the standard MedDRA preferred term
2. Check spelling (UK vs US)
3. Use quotation marks for exact match

### No Results

If the count is zero:

1. Verify the term exists in the data
2. Check population filter
3. Try broader criteria

---

## Next Steps

- [Asking Questions Effectively](asking-questions.md)
- [Understanding Confidence Scores](confidence-scores.md)
- [Example Queries](example-queries.md)
