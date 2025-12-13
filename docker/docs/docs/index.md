# SAGE Documentation

## Study Analytics Generative Engine

Welcome to the SAGE documentation. SAGE is an on-premise clinical data AI platform that enables natural language queries against SDTM and ADaM clinical trial datasets.

## Overview

SAGE acts as a **translator**, not an expert. It translates your natural language questions into SQL queries, executes them against your clinical data, and returns accurate, auditable results.

### Key Features

- **Natural Language Queries**: Ask questions in plain English
- **110% Accuracy Target**: SQL-based execution, not LLM memorization
- **Complete Privacy**: 100% on-premise, air-gapped deployment
- **Audit Ready**: Full traceability and documentation
- **Fuzzy Matching**: Handles typos and synonyms automatically

## Quick Start

1. **Login** to the Admin Panel at `http://localhost:8501`
2. **Upload** your SAS7BDAT files in Data Management
3. **Approve** metadata in the Metadata Auditor
4. **Query** your data using the Chat Interface at `http://localhost:8000`

## Architecture

SAGE uses a **Four Factories** model:

| Factory | Purpose |
|---------|---------|
| **Data Foundry** | Converts SAS files to DuckDB |
| **Metadata Refinery** | Curates Golden Metadata with human approval |
| **Dictionary Plant** | Builds fuzzy matching indexes |
| **Inference Engine** | Processes queries and generates answers |

## Getting Help

- **User Guide**: Learn how to ask questions and interpret results
- **Admin Guide**: Manage users, data, and configuration
- **Technical Docs**: Understand the architecture and algorithms
- **API Reference**: Programmatic access to SAGE

---

*SAGE - Enabling clinical insights through natural language*
