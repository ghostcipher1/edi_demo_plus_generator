# Copy this file to config.py and fill in the password before running.
# config.py is gitignored.

DB_CONFIG = {
    "server":                   "exactedi.database.windows.net,1433",
    "database":                 "HealthcareEDIDemo",
    "username":                 "exactedi",
    "password":                 "REPLACE_WITH_PASSWORD",
    "driver":                   "{ODBC Driver 18 for SQL Server}",
    "encrypt":                  "yes",
    # Azure SQL on Linux requires this unless the system cert store has the root CA
    "trust_server_certificate": "yes",
    "connection_timeout":       60,
}

DATA_CONFIG = {
    "num_patients":       40_000,
    "num_providers":      30,
    "num_locations":      8,
    "num_payers":         15,
    "num_claims":         100_000,
    "target_claim_lines": 300_000,
    "start_date":         "2024-01-01",
    "end_date":           "2025-12-31",
    "random_seed":        42,
    "output_dir":         "data",
    "batch_size":         5_000,
}
