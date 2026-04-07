# Healthcare EDI Analytics Demo

Enterprise-scale synthetic healthcare EDI dataset for a multi-location physician group.
Powers Power BI dashboards across claims performance, payer behaviour, denials, and provider analytics.

---

## What Gets Generated

| Object | Rows |
|---|---|
| dim.Date | 731 (2024-01-01 → 2025-12-31) |
| dim.Location | 8 |
| dim.Payer | 15 |
| dim.Provider | 30 |
| dim.Patient | 40,000 |
| dim.ProcedureCode | 46 CPT codes |
| dim.DiagnosisCode | 42 ICD-10-CM codes |
| fact.Claims | 100,000 |
| fact.ClaimLines | ~305,000 |
| fact.Acknowledgments | ~198,000 (999 + 277CA) |
| fact.Payments | ~88,000 (835 remittances) |

### Simulated Lifecycle

```
Patient Encounter
  └─► 837P Claim Submitted
        └─► 999 Syntax Acknowledgment  (2% rejected)
              └─► 277CA Claim Status   (5% rejected)
                    └─► Adjudication
                          ├─► Fully Paid   (~60%)
                          ├─► Partial Pay  (~20%)
                          ├─► Denied       (~15%)
                          └─► Pending      (~5% — recent claims)
```

### Payer Mix

| Category | Payers | % of patients |
|---|---|---|
| Commercial | BCBS, Aetna, Cigna, UHC, Humana, Anthem | 40% |
| Medicare / MA | Medicare Part B, Humana MA, BCBS MA | 20% |
| Medicaid / Managed | IL Medicaid, Molina, Centene | 20% |
| Regional | Prairie, Heartland, Midwest | 20% |

---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.10+ |
| ODBC Driver 18 for SQL Server | 18.x |
| Azure SQL Database | HealthcareEDIDemo (already provisioned) |

### Installing ODBC Driver 18

**Ubuntu/Debian (with sudo):**
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list \
  | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

**macOS (Homebrew):**
```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew install msodbcsql18
```

**Linux without sudo (local install into `.venv`):**

Download the required packages without installing them:
```bash
apt-get download msodbcsql18 libodbc2 libodbcinst2 unixodbc
```

Extract each into a local directory:
```bash
mkdir -p .venv/odbc_local
for pkg in msodbcsql18*.deb libodbc2*.deb libodbcinst2*.deb unixodbc*.deb; do
    dpkg -x "$pkg" .venv/odbc_local/
done
```

Set environment variables before running the loader (see **Step 2** below).

---

## Installation

```bash
cd healthcare_edi_demo
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## Configuration

`config.py` is excluded from version control. Create it from the provided template:

```bash
cp config.example.py config.py
```

Open `config.py` and set the password:

```python
"password": "YOUR_ACTUAL_PASSWORD_HERE",
```

All other connection parameters are pre-configured for `exactedi.database.windows.net`.

---

## Step 1 — Generate Data

```bash
python generate_demo_data.py
```

Outputs CSV files to `./data/`:

```
data/
  dim_Date.csv
  dim_Location.csv
  dim_Payer.csv
  dim_Provider.csv
  dim_Patient.csv
  dim_ProcedureCode.csv
  dim_DiagnosisCode.csv
  fact_Claims.csv
  fact_ClaimLines.csv
  fact_Acknowledgments.csv
  fact_Payments.csv
```

Expected runtime: ~3–6 minutes on a modern laptop.

---

## Step 2 — Load into Azure SQL

**Standard (system-wide ODBC install):**
```bash
python load_to_azure_sql.py
```

**Without sudo (local ODBC install):**
```bash
ODBC_BASE=".venv/odbc_local"
export LD_LIBRARY_PATH="$ODBC_BASE/usr/lib/x86_64-linux-gnu:$ODBC_BASE/opt/microsoft/msodbcsql18/lib64:${LD_LIBRARY_PATH:-}"
export ODBCSYSINI="$ODBC_BASE/opt/microsoft/msodbcsql18/etc"
export ODBCINI="$ODBC_BASE/etc/odbc.ini"
.venv/bin/python load_to_azure_sql.py
```

The loader will:
1. Connect to `exactedi.database.windows.net`
2. Drop and recreate all schemas and tables (idempotent — safe to re-run)
3. Insert dimension tables first, then fact tables
4. Use `fast_executemany` with batches of 5,000 rows

Expected runtime: ~10–20 minutes depending on network latency.

---

## Step 3 — Connect Power BI

1. Open Power BI Desktop.
2. **Get Data → Azure → Azure SQL Database**.
3. Server: `exactedi.database.windows.net` / Database: `HealthcareEDIDemo`
4. Use **Import** mode for best performance.
5. Select all tables under the `dim` and `fact` schemas.
6. Power BI will auto-detect star schema relationships via primary/foreign keys.

### Suggested Measures

```
Total Billed        = SUM(fact.Claims[TotalBilledAmount])
Total Paid          = SUM(fact.Claims[TotalPaidAmount])
Collection Rate     = [Total Paid] / [Total Billed]
Denial Rate         = DIVIDE(COUNTROWS(FILTER(fact.Claims, Claims[ClaimStatus]="Denied")), COUNTROWS(fact.Claims))
Avg Days to Payment = AVERAGE(fact.Payments[DaysToPayment])
First Pass Rate     = DIVIDE(COUNTROWS(FILTER(fact.Acknowledgments, Acknowledgments[Status]="Accepted")), COUNTROWS(fact.Acknowledgments))
```

---

## Project Structure

```
healthcare_edi_demo/
├── config.example.py           Connection + generation config template (commit this)
├── config.py                   Active config with credentials (gitignored)
├── schema.sql                  DDL for all dim/fact tables, indexes, and constraints
├── generate_demo_data.py       Synthetic data generator — outputs CSV files to data/
├── load_to_azure_sql.py        Bulk loader — CSV → Azure SQL via pyodbc fast_executemany
├── requirements.txt            Python dependencies
├── README.md                   This file
├── images/
│   └── edi-executive-dashboard.png
├── power-bi-dashboard/
│   └── HealthcareEDIDemo.pbids Power BI connection file
├── sql/
│   └── data_extraction_queries.sql   Analysis queries (totals, denial %, days to pay)
└── data/                       Generated CSV files (created at runtime, gitignored)
```

---

## Re-generating Data

Edit `config.py` to change scale or seed, then re-run both scripts:

```python
DATA_CONFIG = {
    "num_patients":   40_000,
    "num_claims":     100_000,
    "random_seed":    99,        # change for a different dataset
    ...
}
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `ODBC Driver 18 not found` | Install per Prerequisites section above |
| `Login failed for user 'exactedi'` | Verify password in `config.py` |
| `Cannot open server` | Ensure your IP is in the Azure SQL firewall rules |
| `libodbc.so.2: cannot open shared object` | Set `LD_LIBRARY_PATH` per the local install instructions |
| `HYT00 Login timeout expired` | Confirm `trust_server_certificate = yes` in `config.py` and check firewall |
| Slow load | Increase `batch_size` in `config.py` to 10,000 (requires more RAM) |
