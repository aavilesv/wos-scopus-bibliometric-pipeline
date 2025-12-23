# Bibliometric Review Pipeline (Scopus & Web of Science)

This repository contains a **reproducible Python pipeline for bibliometric analysis**, designed to unify, clean, and deduplicate records from **Scopus** and **Web of Science (WoS)**, producing a final dataset ready for bibliometric tools and scientific publication.

The pipeline applies systematic validation, metadata normalization, duplicate detection (DOI and fuzzy matching), and generates analytical reports and figures.

---

## Main characteristics

- Automatic validation of input files and folder structure  
- Unification of multiple Scopus (CSV) and WoS (XLS/XLSX) exports  
- Internal and cross-database duplicate removal:
  - Exact DOI matching
  - Fuzzy title matching with year validation (±1 year)
- Metadata normalization:
  - Titles, DOI, ISSN
  - Affiliations and countries
  - Document types
  - Open Access status
- Journal title normalization using **SCImago**
- Export of clean datasets and analytical reports

The final dataset is **fully compatible with Bibliometrix/Biblioshiny, VOSviewer, and ScientoPy**.

---

## Project structure

bibliometric_review/
├── main.py # Single execution entry point
├── loaders.py # Data loading and source-level merging
├── deduplication.py # DOI and fuzzy duplicate detection
├── normalization.py # Metadata normalization
├── scimago_utils.py # Journal title normalization (SCImago)
├── reporting.py # Reports, Excel tables, and figures
├── ui_messages.py # Informative user messages
├── file_validation.py # Input validation and early exit
├── FILES/
│ ├── SCOPUS/ # Scopus CSV exports
│ ├── WOS/ # Web of Science XLS/XLSX exports
│ └── SCIMAGO/
│ └── scimago_unificado.csv
└── RESULTS/ # Automatically generated outputs


---

## Input data structure (required)

Before running the pipeline, place the exported bibliographic files in the following folders:

FILES/
├── SCOPUS/
│ └── *.csv # Scopus exports (CSV format)
├── WOS/
│ └── *.xls / *.xlsx # Web of Science exports
└── SCIMAGO/
└── scimago_unificado.csv


### Important notes

- **Do not modify** the original exported files.
- Multiple files per source are supported (they will be automatically merged).
- If no valid files are found, the pipeline stops automatically (early exit).
- SCImago is optional, but recommended for journal title normalization.

---

## Installation

### Create a virtual environment (recommended)

```bash
python -m venv .venv
source .venv/bin/activate        # macOS / Linux
.venv\Scripts\activate           # Windows

Install dependencies

pip install pandas==2.2.3 numpy==1.26.4 matplotlib==3.10.0 spacy==3.8.4 rapidfuzz==3.11.0 xlrd==2.0.1 openpyxl==3.1.5
python -m spacy download en_core_web_lg

How to run

Only one file must be executed:
python main.py
Requirements

Python ≥ 3.9
