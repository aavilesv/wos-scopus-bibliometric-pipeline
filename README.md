# 📚 Bibliometric Review Pipeline (Scopus & Web of Science)

![Python Version](https://img.shields.io/badge/python-3.9%2B-blue)
![Dependencies](https://img.shields.io/badge/dependencies-pandas%20%7C%20spacy%20%7C%20rapidfuzz-success)

This repository contains an advanced, **reproducible Python pipeline for bibliometric analysis**. It is designed to unify, comprehensively clean, deduplicate, and enrich bibliographic records exported from **Scopus** and **Web of Science (WoS)**. 

The resulting dataset is ready for scientific publication and fully compatible with leading bibliometric tools such as **Bibliometrix/Biblioshiny, VOSviewer, and ScientoPy**.

---

## ✨ Key Features

- **Automated Data Quality Control**: Filters out records missing critical fields (Titles, Authors, Abstracts) and checks DOI syntactic validity, keeping track of removed records for PRISMA abstracting flowcharts.
- **Online DOI Validation**: Connects asynchronously to the **Crossref REST API** to verify the existence and authenticity of DOIs, using local caching to optimize network traffic.
- **Advanced Deduplication Engine**:
  - *Phase 1:* Exact, fast DOI matching.
  - *Phase 2:* Parallelized fuzzy logic matching (using `rapidfuzz` and CPU multiprocessing) alongside NLP title lemmatization (`spaCy`), tolerating up to a ±1 year publication gap.
- **Metadata Normalization**: Standardizes affiliations, country names, document types, and Open Access statuses mapping WoS exports onto a unified Scopus-style schema.
- **SCImago Enrichment**: Automatically merges SCImago Journal Rank (SJR) metrics, Quartiles, and Areas based on ISSN and fuzzy journal title matching. Metrics are prefixed with `scimago__` for clear identification.
- **Comprehensive Reporting**: Generates `.csv` and `.xlsx` final datasets, alongside a detailed PRISMA Excel report (`report.xlsx`) and trend analysis scatter/bar plots automatically exported via `matplotlib`.

---

## 🏗️ Architecture and Workflow

1. **Loaders (`loaders.py`)**: Imports CSV and Excel files, applies SpaCy NLP to titles.
2. **Data Quality (`data_quality.py`)**: Removes invalid or empty entries, logging drops for PRISMA.
3. **Cross-Deduplication (`deduplication.py`)**: Identifies duplicates across WoS and Scopus.
4. **Normalization (`normalization.py`)**: Adapts WoS schema to Scopus rules, cleans country names.
5. **DOI Validation (`doi_validation.py`)**: Online API check via Crossref.
6. **SJR Analysis (`sjr_analysis.py`)**: Cross-references with SCImago dataset.
7. **Reporting (`reporting.py`)**: Exports datasets, metrics, and visualization plots.

---

## 📂 Project Structure

```text
bibliometric_review/
├── config.py              # Centralized configuration (Years, thresholds)
├── main.py                # Main orchestrator script
├── loaders.py             # Data loading and initial prep
├── data_quality.py        # PRISMA data quality filters
├── deduplication.py       # Parallel duplicate detection
├── normalization.py       # WoS to Scopus normalization
├── doi_validation.py      # Crossref online DOI checker
├── sjr_analysis.py        # SCImago metrics integration
├── scimago_utils.py       # SCImago canonical title utils
├── reporting.py           # Exports Excel/CSV and charts
├── file_validation.py     # Input folder scanner
├── logging_utils.py       # Global logging setup
├── FILES/
│   ├── SCOPUS/            # Place Scopus CSV exports here
│   ├── WOS/               # Place Web of Science XLS/XLSX exports here
│   └── SCIMAGO/           # Place scimago_unificado.csv here
├── RESULTS/               # Outputs (Datasets, Excel reports, PNG charts)
└── logs/                  # Execution log files
```

---

## 📥 Input Data Requirements

Before running the pipeline, place the corresponding exported files in the `FILES/` subdirectories:

- **Scopus**: Export as `.csv` with all citation and abstract information. Placed in `FILES/SCOPUS/`.
- **Web of Science**: Export as `.xls` or `.xlsx` (Excel format). Placed in `FILES/WOS/`.
- **SCImago**: Ensure you have a unified file named `scimago_unificado.csv` inside `FILES/SCIMAGO/`.

> **Note**: Do not modify the original exported files before placing them in the folders. The script handles multiple files per source automatically.

---

## 🚀 Installation & Usage

### 1. Create a Virtual Environment (Recommended)

**Windows:**
```cmd
python -m venv .venv
.venv\Scripts\activate
```

**macOS / Linux:**
```bash
python -m venv .venv
source .venv/bin/activate
```

### 2. Install Dependencies

Install the required Python packages and download the SpaCy linguistic model:

```bash
pip install pandas numpy matplotlib spacy rapidfuzz xlrd openpyxl requests
python -m spacy download en_core_web_lg
```

*Note: For performance metrics in DOI validation, you can optionally `pip install tqdm`.*

### 3. Configuration Settings
You can modify `config.py` to adjust:
- `YEAR_START` and `YEAR_FINAL`: To filter the study period.
- `FUZZY_THRESHOLD`: Sensitivity for the title matching algorithm (recommended: `90`).
- `remove_blank_dois`: Boolean to enforce strict DOI filtering at the loading stage.

### 4. Execution

Once your input files are in place, run the main orchestrator (this script processes the files, caches progress, and logs to the console):

```bash
python main.py
```

---

## 📊 Generated Outputs (`RESULTS/`)

- `datawos_scopus.csv` & `.xlsx`: The final cleaned and enriched dataset with combined keywords.
- `datawos_scopus_repeatedstitles.csv` & `.xlsx`: List of exact duplicate titles found.
- `report.xlsx`: Multi-sheet Excel workbook showcasing:
  - PRISMA Flow metric counts.
  - Raw and final document/citation trends.
  - Crossref DOI validation distribution.
  - Overall Data Quality Summaries.
- **Charts**: `distribution_post_dedup.png`, `raw_articles_by_year.png`, `raw_citations_by_year.png`.
