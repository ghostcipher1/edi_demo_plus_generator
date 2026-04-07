# Healthcare EDI Analytics Demo

A synthetic enterprise-scale healthcare EDI dataset and Azure SQL data warehouse designed to power real-world Power BI analytics for a multi-location physician group.

The project simulates a complete revenue cycle management (RCM) lifecycle — from **837P** professional claim submission through **999** syntax acknowledgment, **277CA** claim status, adjudication, and **835** remittance advice — producing a fully relational star schema with over 700,000 rows across dimension and fact tables.

---

## What It Covers

- **40,000 patients** across a realistic payer mix: commercial insurers, Medicare/Medicare Advantage, Medicaid/managed care, and regional plans
- **100,000 claims** with ~305,000 claim lines tied to real CPT procedure codes and ICD-10-CM diagnosis codes
- **Adjudication outcomes** distributed across fully paid (~60%), partial pay (~20%), denied (~15%), and pending (~5%) statuses
- **835 remittance records** with CARC/RARC denial codes, days-to-payment, and payment method (EFT/CHK)
- **999 and 277CA acknowledgments** simulating a 2% syntax rejection rate and 5% claim-level rejection rate

---

## How It Works

Two Python scripts handle the full pipeline:

1. `generate_demo_data.py` — builds the dataset using NumPy-vectorised generation and Faker, then writes it to CSV files (~3–6 minutes)
2. `load_to_azure_sql.py` — connects to Azure SQL Database and bulk-loads all tables in foreign-key-safe order using pyodbc `fast_executemany` (~10–20 minutes)

The schema (`schema.sql`) defines a clean `dim`/`fact` star schema optimised for Power BI Import mode, with indexes on the most common filter and join columns. A `.pbids` connection file is included for one-click Power BI connectivity, along with a set of SQL analysis queries covering total claims, claim values, denial percentage, and average days to payment.

---

## Intended Use

Built as a demonstration environment for healthcare revenue cycle analytics. Suitable for developing and testing Power BI dashboards, validating EDI data models, or exploring payer behaviour and denial patterns without using protected health information.
