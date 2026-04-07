# =============================================================================
# load_to_azure_sql.py
# Reads CSV files from the data/ directory and loads them into Azure SQL.
#
# Load order (respects foreign key constraints):
#   dim.Date → dim.Location → dim.Payer → dim.Provider → dim.Patient
#   → dim.ProcedureCode → dim.DiagnosisCode
#   → fact.Claims → fact.ClaimLines → fact.Acknowledgments → fact.Payments
#
# Usage:
#   python load_to_azure_sql.py
# =============================================================================

import os
import re
import logging
import math

import pandas as pd
import pyodbc
from tqdm import tqdm

from config import DB_CONFIG, DATA_CONFIG

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

OUTPUT_DIR  = DATA_CONFIG["output_dir"]
BATCH_SIZE  = DATA_CONFIG["batch_size"]


# =============================================================================
# DATABASE CONNECTION
# =============================================================================

def get_connection() -> pyodbc.Connection:
    """Return a pyodbc connection to Azure SQL."""
    conn_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
        f"Encrypt={DB_CONFIG['encrypt']};"
        f"TrustServerCertificate={DB_CONFIG['trust_server_certificate']};"
        f"Connection Timeout={DB_CONFIG['connection_timeout']};"
    )
    conn = pyodbc.connect(conn_str, autocommit=False)
    conn.timeout = 0          # no statement timeout for bulk ops
    return conn


# =============================================================================
# SCHEMA CREATION
# =============================================================================

def create_schema(conn: pyodbc.Connection, schema_file: str = "schema.sql") -> None:
    """Execute schema.sql, splitting on GO batch separators."""
    log.info(f"Executing {schema_file}…")
    with open(schema_file, "r", encoding="utf-8") as f:
        sql = f.read()

    # Split on bare GO (case-insensitive, on its own line)
    batches = re.split(r"^\s*GO\s*$", sql, flags=re.MULTILINE | re.IGNORECASE)

    cursor = conn.cursor()
    executed = 0
    for batch in batches:
        batch = batch.strip()
        if batch:
            try:
                cursor.execute(batch)
                conn.commit()
                executed += 1
            except pyodbc.Error as e:
                log.warning(f"  Batch skipped ({e.args[1][:80] if e.args else str(e)})")
                conn.rollback()

    log.info(f"  Schema applied ({executed} batches executed).")


# =============================================================================
# BULK LOADER
# =============================================================================

def _to_python_row(row: tuple) -> tuple:
    """Convert pandas NA / NaT / numpy NaN to Python None so pyodbc accepts the value."""
    out = []
    for v in row:
        # Explicit None
        if v is None:
            out.append(None)
        # pandas NA singleton (introduced in pandas 1.0)
        elif v is pd.NA:
            out.append(None)
        # pandas NaT
        elif v is pd.NaT:
            out.append(None)
        # float NaN (covers numpy.nan which is a Python float)
        elif isinstance(v, float) and math.isnan(v):
            out.append(None)
        else:
            # Convert numpy scalar types to native Python
            if hasattr(v, "item"):
                v = v.item()
            out.append(v)
    return tuple(out)


def bulk_insert(
    conn:       pyodbc.Connection,
    table:      str,
    df:         pd.DataFrame,
    identity_insert: bool = True,
    batch_size: int = BATCH_SIZE,
) -> None:
    """
    Insert a DataFrame into an Azure SQL table using fast_executemany.

    Parameters
    ----------
    table           : fully-qualified name, e.g. 'dim.Date'
    identity_insert : if True, wraps the INSERT with SET IDENTITY_INSERT ON/OFF
                      (required when the CSV provides explicit key values)
    """
    if df.empty:
        log.warning(f"  {table}: empty DataFrame, skipping.")
        return

    cols        = list(df.columns)
    placeholders = ", ".join(["?"] * len(cols))
    col_list     = ", ".join(f"[{c}]" for c in cols)
    insert_sql   = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"

    cursor = conn.cursor()
    cursor.fast_executemany = True

    if identity_insert:
        cursor.execute(f"SET IDENTITY_INSERT {table} ON")

    n_rows    = len(df)
    n_batches = math.ceil(n_rows / batch_size)

    with tqdm(total=n_rows, desc=f"  {table}", unit="rows", leave=False) as pbar:
        for i in range(n_batches):
            chunk = df.iloc[i * batch_size : (i + 1) * batch_size]
            # Convert to list of plain Python tuples
            data = [_to_python_row(row) for row in chunk.itertuples(index=False, name=None)]
            try:
                cursor.executemany(insert_sql, data)
                conn.commit()
            except pyodbc.Error as exc:
                conn.rollback()
                log.error(f"  ERROR inserting into {table} (batch {i+1}/{n_batches}): {exc}")
                raise
            pbar.update(len(chunk))

    if identity_insert:
        cursor.execute(f"SET IDENTITY_INSERT {table} OFF")
        conn.commit()

    log.info(f"  {table}: {n_rows:,} rows loaded.")


# =============================================================================
# CSV READERS  (type-cast to match SQL schema)
# =============================================================================

def read_csv(name: str) -> pd.DataFrame:
    path = os.path.join(OUTPUT_DIR, f"{name}.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"{path} not found — run generate_demo_data.py first."
        )
    df = pd.read_csv(path, low_memory=False)
    # Replace pandas NA with None (pyodbc-friendly)
    df = df.where(pd.notna(df), other=None)
    return df


def load_dim_date(conn, df):
    # DateKey is a plain INT (not IDENTITY), but we still supply it explicitly
    bulk_insert(conn, "dim.Date", df, identity_insert=False)


def load_dim_location(conn, df):
    bulk_insert(conn, "dim.Location", df)


def load_dim_payer(conn, df):
    bulk_insert(conn, "dim.Payer", df)


def load_dim_provider(conn, df):
    bulk_insert(conn, "dim.Provider", df)


def load_dim_patient(conn, df):
    bulk_insert(conn, "dim.Patient", df)


def load_dim_procedure_code(conn, df):
    bulk_insert(conn, "dim.ProcedureCode", df)


def load_dim_diagnosis_code(conn, df):
    bulk_insert(conn, "dim.DiagnosisCode", df)


def load_fact_claims(conn, df):
    # Drop the auto-generated CreatedAt column — SQL Server supplies the default
    if "CreatedAt" in df.columns:
        df = df.drop(columns=["CreatedAt"])
    bulk_insert(conn, "fact.Claims", df)


def load_fact_claim_lines(conn, df):
    bulk_insert(conn, "fact.ClaimLines", df)


def load_fact_acknowledgments(conn, df):
    bulk_insert(conn, "fact.Acknowledgments", df)


def load_fact_payments(conn, df):
    bulk_insert(conn, "fact.Payments", df)


# =============================================================================
# MAIN
# =============================================================================

LOAD_PLAN = [
    # (csv_name,              loader_fn,                 description)
    ("dim_Date",              load_dim_date,              "dim.Date"),
    ("dim_Location",          load_dim_location,          "dim.Location"),
    ("dim_Payer",             load_dim_payer,             "dim.Payer"),
    ("dim_Provider",          load_dim_provider,          "dim.Provider"),
    ("dim_Patient",           load_dim_patient,           "dim.Patient"),
    ("dim_ProcedureCode",     load_dim_procedure_code,    "dim.ProcedureCode"),
    ("dim_DiagnosisCode",     load_dim_diagnosis_code,    "dim.DiagnosisCode"),
    ("fact_Claims",           load_fact_claims,           "fact.Claims"),
    ("fact_ClaimLines",       load_fact_claim_lines,      "fact.ClaimLines"),
    ("fact_Acknowledgments",  load_fact_acknowledgments,  "fact.Acknowledgments"),
    ("fact_Payments",         load_fact_payments,         "fact.Payments"),
]


def main():
    log.info("=" * 60)
    log.info("Healthcare EDI — Azure SQL Loader")
    log.info("=" * 60)

    # Validate password
    if DB_CONFIG["password"] in ("REPLACE_WITH_PASSWORD", "", None):
        raise ValueError(
            "Set DB_CONFIG['password'] in config.py before running the loader."
        )

    log.info(f"Connecting to {DB_CONFIG['server']} / {DB_CONFIG['database']}…")
    conn = get_connection()
    log.info("  Connected.")

    # Create / verify schema
    create_schema(conn)

    # Load tables in dependency order
    for csv_name, loader_fn, label in LOAD_PLAN:
        log.info(f"Loading {label}…")
        df = read_csv(csv_name)
        loader_fn(conn, df)

    conn.close()
    log.info("=" * 60)
    log.info("Load complete. Connect Power BI to Azure SQL and enjoy!")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
