# =============================================================================
# generate_demo_data.py
# Generates enterprise-scale healthcare EDI analytics demo dataset.
#
# Output: CSV files in DATA_CONFIG['output_dir'] (default: ./data/)
#
# Lifecycle simulated:
#   Patient Encounter → 837P Claim Submitted → 999 Acknowledgment
#   → 277CA Claim Status → Adjudication → 835 Remittance/Payment
# =============================================================================

import os
import logging
import string
import random
from datetime import datetime, date, timedelta

import numpy as np
import pandas as pd
from faker import Faker
from tqdm import tqdm

from config import DATA_CONFIG

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SEED = DATA_CONFIG["random_seed"]
fake = Faker("en_US")
Faker.seed(SEED)
np.random.seed(SEED)
random.seed(SEED)

START_DATE = datetime.strptime(DATA_CONFIG["start_date"], "%Y-%m-%d").date()
END_DATE   = datetime.strptime(DATA_CONFIG["end_date"],   "%Y-%m-%d").date()
DATE_RANGE_DAYS = (END_DATE - START_DATE).days

OUTPUT_DIR = DATA_CONFIG["output_dir"]
os.makedirs(OUTPUT_DIR, exist_ok=True)


# =============================================================================
# STATIC REFERENCE DATA
# =============================================================================

LOCATIONS = [
    {"LocationName": "Primary Care North",
     "AddressLine1": "1200 Northern Pkwy Ste 100", "City": "Springfield",  "State": "IL", "ZipCode": "62701",
     "Phone": "217-555-0101", "TaxID": "36-1234561", "GroupNPI": "1234567890"},
    {"LocationName": "Primary Care South",
     "AddressLine1": "450 South Lake Blvd",         "City": "Springfield",  "State": "IL", "ZipCode": "62702",
     "Phone": "217-555-0202", "TaxID": "36-1234562", "GroupNPI": "1234567891"},
    {"LocationName": "Internal Medicine East",
     "AddressLine1": "875 East Commerce Dr",         "City": "Decatur",      "State": "IL", "ZipCode": "62521",
     "Phone": "217-555-0303", "TaxID": "36-1234563", "GroupNPI": "1234567892"},
    {"LocationName": "Family Practice West",
     "AddressLine1": "2300 West Main St",            "City": "Champaign",    "State": "IL", "ZipCode": "61820",
     "Phone": "217-555-0404", "TaxID": "36-1234564", "GroupNPI": "1234567893"},
    {"LocationName": "Cardiology Central",
     "AddressLine1": "500 Medical Plaza Ste 200",    "City": "Springfield",  "State": "IL", "ZipCode": "62703",
     "Phone": "217-555-0505", "TaxID": "36-1234565", "GroupNPI": "1234567894"},
    {"LocationName": "Orthopedics North",
     "AddressLine1": "3100 North Fifth St",          "City": "Rockford",     "State": "IL", "ZipCode": "61101",
     "Phone": "217-555-0606", "TaxID": "36-1234566", "GroupNPI": "1234567895"},
    {"LocationName": "Urgent Care Downtown",
     "AddressLine1": "100 Main Street",              "City": "Springfield",  "State": "IL", "ZipCode": "62701",
     "Phone": "217-555-0707", "TaxID": "36-1234567", "GroupNPI": "1234567896"},
    {"LocationName": "Multi-Specialty Regional",
     "AddressLine1": "4400 Regional Medical Dr",     "City": "Bloomington",  "State": "IL", "ZipCode": "61701",
     "Phone": "217-555-0808", "TaxID": "36-1234568", "GroupNPI": "1234567897"},
]

PAYERS = [
    # ---- Commercial (6) ----
    {"PayerID": "00192", "PayerName": "Blue Cross Blue Shield",        "PayerType": "Commercial",        "PayerCategory": "Commercial",          "AvgDaysToPayment": 28, "DenialRate": 0.1100},
    {"PayerID": "60054", "PayerName": "Aetna",                         "PayerType": "Commercial",        "PayerCategory": "Commercial",          "AvgDaysToPayment": 30, "DenialRate": 0.1200},
    {"PayerID": "62308", "PayerName": "Cigna",                         "PayerType": "Commercial",        "PayerCategory": "Commercial",          "AvgDaysToPayment": 32, "DenialRate": 0.1000},
    {"PayerID": "87726", "PayerName": "UnitedHealthcare",              "PayerType": "Commercial",        "PayerCategory": "Commercial",          "AvgDaysToPayment": 27, "DenialRate": 0.1300},
    {"PayerID": "61101", "PayerName": "Humana Commercial",             "PayerType": "Commercial",        "PayerCategory": "Commercial",          "AvgDaysToPayment": 31, "DenialRate": 0.1100},
    {"PayerID": "23284", "PayerName": "Anthem",                        "PayerType": "Commercial",        "PayerCategory": "Commercial",          "AvgDaysToPayment": 29, "DenialRate": 0.1200},
    # ---- Medicare (3) ----
    {"PayerID": "00430", "PayerName": "Medicare Part B",               "PayerType": "Medicare",          "PayerCategory": "Government",          "AvgDaysToPayment": 14, "DenialRate": 0.0900},
    {"PayerID": "00610", "PayerName": "Medicare Advantage - Humana",   "PayerType": "Medicare Advantage","PayerCategory": "Government",          "AvgDaysToPayment": 18, "DenialRate": 0.1100},
    {"PayerID": "00720", "PayerName": "Medicare Advantage - BCBS",     "PayerType": "Medicare Advantage","PayerCategory": "Government",          "AvgDaysToPayment": 20, "DenialRate": 0.1000},
    # ---- Medicaid (3) ----
    {"PayerID": "77350", "PayerName": "Illinois Medicaid",             "PayerType": "Medicaid",          "PayerCategory": "Government",          "AvgDaysToPayment": 45, "DenialRate": 0.1800},
    {"PayerID": "82080", "PayerName": "Managed Medicaid - Molina",     "PayerType": "Managed Medicaid",  "PayerCategory": "Government",          "AvgDaysToPayment": 40, "DenialRate": 0.1700},
    {"PayerID": "91070", "PayerName": "Managed Medicaid - Centene",    "PayerType": "Managed Medicaid",  "PayerCategory": "Government",          "AvgDaysToPayment": 42, "DenialRate": 0.1600},
    # ---- Regional (3) ----
    {"PayerID": "34120", "PayerName": "Prairie Health Plan",           "PayerType": "Regional",          "PayerCategory": "Regional Health Plan","AvgDaysToPayment": 35, "DenialRate": 0.1400},
    {"PayerID": "45230", "PayerName": "Heartland Health Alliance",     "PayerType": "Regional",          "PayerCategory": "Regional Health Plan","AvgDaysToPayment": 38, "DenialRate": 0.1500},
    {"PayerID": "56890", "PayerName": "Midwest Community Health Plan", "PayerType": "Regional",          "PayerCategory": "Regional Health Plan","AvgDaysToPayment": 36, "DenialRate": 0.1400},
]

# Provider templates — 30 providers across 8 locations
# LocationIdx maps into LOCATIONS list (0-based)
PROVIDER_TEMPLATES = [
    # Family Medicine (10)
    {"Specialty": "Family Medicine",    "ProviderType": "MD", "LocationIdx": 0},
    {"Specialty": "Family Medicine",    "ProviderType": "MD", "LocationIdx": 0},
    {"Specialty": "Family Medicine",    "ProviderType": "MD", "LocationIdx": 1},
    {"Specialty": "Family Medicine",    "ProviderType": "MD", "LocationIdx": 1},
    {"Specialty": "Family Medicine",    "ProviderType": "MD", "LocationIdx": 3},
    {"Specialty": "Family Medicine",    "ProviderType": "MD", "LocationIdx": 3},
    {"Specialty": "Family Medicine",    "ProviderType": "MD", "LocationIdx": 3},
    {"Specialty": "Family Medicine",    "ProviderType": "MD", "LocationIdx": 7},
    {"Specialty": "Family Medicine",    "ProviderType": "MD", "LocationIdx": 7},
    {"Specialty": "Family Medicine",    "ProviderType": "MD", "LocationIdx": 7},
    # Internal Medicine (5)
    {"Specialty": "Internal Medicine",  "ProviderType": "MD", "LocationIdx": 2},
    {"Specialty": "Internal Medicine",  "ProviderType": "MD", "LocationIdx": 2},
    {"Specialty": "Internal Medicine",  "ProviderType": "MD", "LocationIdx": 2},
    {"Specialty": "Internal Medicine",  "ProviderType": "MD", "LocationIdx": 7},
    {"Specialty": "Internal Medicine",  "ProviderType": "MD", "LocationIdx": 7},
    # Cardiology (4)
    {"Specialty": "Cardiology",         "ProviderType": "MD", "LocationIdx": 4},
    {"Specialty": "Cardiology",         "ProviderType": "MD", "LocationIdx": 4},
    {"Specialty": "Cardiology",         "ProviderType": "MD", "LocationIdx": 4},
    {"Specialty": "Cardiology",         "ProviderType": "MD", "LocationIdx": 7},
    # Orthopedics (4)
    {"Specialty": "Orthopedics",        "ProviderType": "MD", "LocationIdx": 5},
    {"Specialty": "Orthopedics",        "ProviderType": "MD", "LocationIdx": 5},
    {"Specialty": "Orthopedics",        "ProviderType": "MD", "LocationIdx": 5},
    {"Specialty": "Orthopedics",        "ProviderType": "MD", "LocationIdx": 7},
    # Urgent Care (3)
    {"Specialty": "Urgent Care",        "ProviderType": "MD", "LocationIdx": 6},
    {"Specialty": "Urgent Care",        "ProviderType": "MD", "LocationIdx": 6},
    {"Specialty": "Urgent Care",        "ProviderType": "MD", "LocationIdx": 7},
    # Nurse Practitioners (2)
    {"Specialty": "Family Medicine",    "ProviderType": "NP", "LocationIdx": 0},
    {"Specialty": "Internal Medicine",  "ProviderType": "NP", "LocationIdx": 2},
    # Physician Assistants (2)
    {"Specialty": "Family Medicine",    "ProviderType": "PA", "LocationIdx": 1},
    {"Specialty": "Urgent Care",        "ProviderType": "PA", "LocationIdx": 6},
]

CPT_CODES = [
    # E&M — New Patient
    {"CPTCode": "99202", "Description": "Office visit, new patient, straightforward medical decision making",   "Category": "E&M",           "BaseCharge": 125.00, "RVU": 0.93},
    {"CPTCode": "99203", "Description": "Office visit, new patient, low complexity medical decision making",    "Category": "E&M",           "BaseCharge": 195.00, "RVU": 1.60},
    {"CPTCode": "99204", "Description": "Office visit, new patient, moderate complexity medical decision making","Category": "E&M",           "BaseCharge": 295.00, "RVU": 2.60},
    {"CPTCode": "99205", "Description": "Office visit, new patient, high complexity medical decision making",   "Category": "E&M",           "BaseCharge": 395.00, "RVU": 3.50},
    # E&M — Established Patient
    {"CPTCode": "99211", "Description": "Office visit, established patient, minimal medical decision making",   "Category": "E&M",           "BaseCharge":  45.00, "RVU": 0.18},
    {"CPTCode": "99212", "Description": "Office visit, established patient, straightforward medical decision",  "Category": "E&M",           "BaseCharge":  90.00, "RVU": 0.70},
    {"CPTCode": "99213", "Description": "Office visit, established patient, low complexity medical decision",   "Category": "E&M",           "BaseCharge": 150.00, "RVU": 1.30},
    {"CPTCode": "99214", "Description": "Office visit, established patient, moderate complexity medical decision","Category": "E&M",          "BaseCharge": 220.00, "RVU": 1.92},
    {"CPTCode": "99215", "Description": "Office visit, established patient, high complexity medical decision",  "Category": "E&M",           "BaseCharge": 310.00, "RVU": 2.80},
    # Preventive
    {"CPTCode": "99385", "Description": "Preventive visit, new patient, age 18-39",                            "Category": "Preventive",     "BaseCharge": 225.00, "RVU": 1.92},
    {"CPTCode": "99386", "Description": "Preventive visit, new patient, age 40-64",                            "Category": "Preventive",     "BaseCharge": 250.00, "RVU": 2.33},
    {"CPTCode": "99387", "Description": "Preventive visit, new patient, age 65+",                              "Category": "Preventive",     "BaseCharge": 280.00, "RVU": 2.50},
    {"CPTCode": "99395", "Description": "Preventive visit, established patient, age 18-39",                    "Category": "Preventive",     "BaseCharge": 200.00, "RVU": 1.75},
    {"CPTCode": "99396", "Description": "Preventive visit, established patient, age 40-64",                    "Category": "Preventive",     "BaseCharge": 225.00, "RVU": 1.94},
    {"CPTCode": "99397", "Description": "Preventive visit, established patient, age 65+",                      "Category": "Preventive",     "BaseCharge": 250.00, "RVU": 2.08},
    # Urgent Care / ED
    {"CPTCode": "99281", "Description": "Emergency department visit, level 1 — self-limited or minor problem", "Category": "Emergency",      "BaseCharge":  85.00, "RVU": 0.45},
    {"CPTCode": "99282", "Description": "Emergency department visit, level 2 — low complexity",                "Category": "Emergency",      "BaseCharge": 145.00, "RVU": 0.88},
    {"CPTCode": "99283", "Description": "Emergency department visit, level 3 — moderate complexity",           "Category": "Emergency",      "BaseCharge": 240.00, "RVU": 1.34},
    {"CPTCode": "99284", "Description": "Emergency department visit, level 4 — high complexity",               "Category": "Emergency",      "BaseCharge": 380.00, "RVU": 2.56},
    {"CPTCode": "99285", "Description": "Emergency department visit, level 5 — high complexity, high severity","Category": "Emergency",      "BaseCharge": 550.00, "RVU": 3.80},
    # Cardiology
    {"CPTCode": "93000", "Description": "Electrocardiogram, routine ECG with at least 12 leads, with interpretation and report",          "Category": "Cardiology",     "BaseCharge":  95.00, "RVU": 0.61},
    {"CPTCode": "93010", "Description": "Electrocardiogram, routine ECG, interpretation and report only",                                  "Category": "Cardiology",     "BaseCharge":  45.00, "RVU": 0.25},
    {"CPTCode": "93040", "Description": "Rhythm ECG, 1-3 leads, with interpretation and report",                                          "Category": "Cardiology",     "BaseCharge":  75.00, "RVU": 0.50},
    {"CPTCode": "93224", "Description": "External electrocardiographic recording up to 48 hours",                                          "Category": "Cardiology",     "BaseCharge": 550.00, "RVU": 2.85},
    {"CPTCode": "93306", "Description": "Echocardiography, transthoracic, real-time with image documentation, complete",                   "Category": "Cardiology",     "BaseCharge":1200.00, "RVU": 4.90},
    {"CPTCode": "93308", "Description": "Echocardiography, transthoracic, follow-up or limited study",                                    "Category": "Cardiology",     "BaseCharge": 750.00, "RVU": 2.80},
    {"CPTCode": "93350", "Description": "Echocardiography, transthoracic, real-time, during cardiovascular stress test",                   "Category": "Cardiology",     "BaseCharge":1800.00, "RVU": 6.25},
    {"CPTCode": "93015", "Description": "Cardiovascular stress test using maximal or submaximal treadmill or bicycle exercise, complete",   "Category": "Cardiology",     "BaseCharge": 650.00, "RVU": 3.10},
    {"CPTCode": "93017", "Description": "Cardiovascular stress test, tracing and interpretation only",                                     "Category": "Cardiology",     "BaseCharge": 380.00, "RVU": 1.68},
    # Orthopedics
    {"CPTCode": "27447", "Description": "Arthroplasty, knee, condyle and plateau; medial AND lateral compartments with or without patella resurfacing (total knee arthroplasty)",  "Category": "Orthopedics",    "BaseCharge": 9500.00, "RVU": 22.07},
    {"CPTCode": "27130", "Description": "Arthroplasty, acetabular and proximal femoral prosthetic replacement (total hip arthroplasty)",                                           "Category": "Orthopedics",    "BaseCharge": 9000.00, "RVU": 21.12},
    {"CPTCode": "29827", "Description": "Arthroscopy, shoulder, surgical; with rotator cuff repair",                                                                               "Category": "Orthopedics",    "BaseCharge": 4500.00, "RVU": 12.15},
    {"CPTCode": "29881", "Description": "Arthroscopy, knee, surgical; with meniscectomy (medial OR lateral, including any meniscal shaving)",                                      "Category": "Orthopedics",    "BaseCharge": 3200.00, "RVU":  9.48},
    {"CPTCode": "20610", "Description": "Arthrocentesis, aspiration and/or injection, major joint or bursa (eg, shoulder, hip, knee, subacromial bursa)",                          "Category": "Orthopedics",    "BaseCharge":  175.00, "RVU":  1.39},
    {"CPTCode": "27370", "Description": "Injection procedure for knee arthrography",                                                                                               "Category": "Orthopedics",    "BaseCharge":  280.00, "RVU":  2.10},
    {"CPTCode": "73721", "Description": "Magnetic resonance imaging, any joint of lower extremity; without contrast material(s)",                                                  "Category": "Orthopedics",    "BaseCharge": 1400.00, "RVU":  5.20},
    {"CPTCode": "73723", "Description": "Magnetic resonance imaging, any joint of lower extremity; without contrast material(s), followed by contrast material(s) and further sequences", "Category": "Orthopedics", "BaseCharge": 1800.00, "RVU": 6.40},
    # Laboratory
    {"CPTCode": "36415", "Description": "Collection of venous blood by venipuncture",                                                                                              "Category": "Laboratory",     "BaseCharge":  35.00, "RVU": 0.17},
    {"CPTCode": "80053", "Description": "Comprehensive metabolic panel",                                                                                                           "Category": "Laboratory",     "BaseCharge":  85.00, "RVU": 0.00},
    {"CPTCode": "85025", "Description": "Blood count; complete (CBC), automated (Hgb, Hct, RBC, WBC and platelet count) and automated differential WBC count",                    "Category": "Laboratory",     "BaseCharge":  55.00, "RVU": 0.00},
    {"CPTCode": "84443", "Description": "Thyroid stimulating hormone (TSH)",                                                                                                       "Category": "Laboratory",     "BaseCharge":  95.00, "RVU": 0.00},
    {"CPTCode": "83036", "Description": "Hemoglobin; glycosylated (A1C)",                                                                                                         "Category": "Laboratory",     "BaseCharge":  70.00, "RVU": 0.00},
    {"CPTCode": "81001", "Description": "Urinalysis, by dip stick or tablet reagent for bilirubin, glucose, hemoglobin, ketones, leukocytes, nitrite, pH, protein, specific gravity, urobilinogen, any number of these constituents; automated, with microscopy", "Category": "Laboratory", "BaseCharge": 40.00, "RVU": 0.00},
    # Immunization Administration
    {"CPTCode": "90460", "Description": "Immunization administration through 18 years of age via any route of administration, with counseling by physician or other qualified health care professional; first or only component of each vaccine or toxoid administered", "Category": "Immunization", "BaseCharge": 28.00, "RVU": 0.17},
    {"CPTCode": "90471", "Description": "Immunization administration; 1 vaccine (single or combination vaccine/toxoid)",                                                           "Category": "Immunization",   "BaseCharge":  35.00, "RVU": 0.22},
]

DIAGNOSIS_CODES = [
    # Endocrine / Metabolic
    {"ICD10Code": "E11.9",   "Description": "Type 2 diabetes mellitus without complications",                          "Category": "Endocrine"},
    {"ICD10Code": "E11.65",  "Description": "Type 2 diabetes mellitus with hyperglycemia",                             "Category": "Endocrine"},
    {"ICD10Code": "E11.40",  "Description": "Type 2 diabetes mellitus with diabetic neuropathy, unspecified",          "Category": "Endocrine"},
    {"ICD10Code": "E78.5",   "Description": "Hyperlipidemia, unspecified",                                             "Category": "Endocrine"},
    {"ICD10Code": "E66.01",  "Description": "Morbid (severe) obesity due to excess calories",                          "Category": "Endocrine"},
    # Cardiovascular
    {"ICD10Code": "I10",     "Description": "Essential (primary) hypertension",                                        "Category": "Cardiovascular"},
    {"ICD10Code": "I25.10",  "Description": "Atherosclerotic heart disease of native coronary artery without angina pectoris", "Category": "Cardiovascular"},
    {"ICD10Code": "I48.0",   "Description": "Paroxysmal atrial fibrillation",                                          "Category": "Cardiovascular"},
    {"ICD10Code": "I48.11",  "Description": "Longstanding persistent atrial fibrillation",                             "Category": "Cardiovascular"},
    {"ICD10Code": "I50.9",   "Description": "Heart failure, unspecified",                                              "Category": "Cardiovascular"},
    {"ICD10Code": "I73.9",   "Description": "Peripheral vascular disease, unspecified",                                "Category": "Cardiovascular"},
    {"ICD10Code": "I63.9",   "Description": "Cerebral infarction, unspecified",                                        "Category": "Cardiovascular"},
    # Respiratory
    {"ICD10Code": "J06.9",   "Description": "Acute upper respiratory infection, unspecified",                          "Category": "Respiratory"},
    {"ICD10Code": "J18.9",   "Description": "Pneumonia, unspecified organism",                                         "Category": "Respiratory"},
    {"ICD10Code": "J44.1",   "Description": "Chronic obstructive pulmonary disease with (acute) exacerbation",         "Category": "Respiratory"},
    {"ICD10Code": "J45.20",  "Description": "Mild intermittent asthma, uncomplicated",                                 "Category": "Respiratory"},
    {"ICD10Code": "J45.41",  "Description": "Moderate persistent asthma with (acute) exacerbation",                   "Category": "Respiratory"},
    # Musculoskeletal
    {"ICD10Code": "M54.5",   "Description": "Low back pain",                                                           "Category": "Musculoskeletal"},
    {"ICD10Code": "M17.11",  "Description": "Primary osteoarthritis, right knee",                                      "Category": "Musculoskeletal"},
    {"ICD10Code": "M17.12",  "Description": "Primary osteoarthritis, left knee",                                       "Category": "Musculoskeletal"},
    {"ICD10Code": "M25.511", "Description": "Pain in right shoulder",                                                  "Category": "Musculoskeletal"},
    {"ICD10Code": "M25.512", "Description": "Pain in left shoulder",                                                   "Category": "Musculoskeletal"},
    {"ICD10Code": "M48.06",  "Description": "Spinal stenosis, lumbar region",                                          "Category": "Musculoskeletal"},
    {"ICD10Code": "M19.011", "Description": "Primary osteoarthritis, right shoulder",                                  "Category": "Musculoskeletal"},
    {"ICD10Code": "M75.100", "Description": "Unspecified rotator cuff syndrome of unspecified shoulder",               "Category": "Musculoskeletal"},
    # Gastrointestinal
    {"ICD10Code": "K21.0",   "Description": "Gastro-esophageal reflux disease with esophagitis",                       "Category": "Gastrointestinal"},
    {"ICD10Code": "K21.9",   "Description": "Gastro-esophageal reflux disease without esophagitis",                   "Category": "Gastrointestinal"},
    {"ICD10Code": "K57.30",  "Description": "Diverticulosis of large intestine without perforation or abscess without bleeding", "Category": "Gastrointestinal"},
    # Mental Health
    {"ICD10Code": "F32.9",   "Description": "Major depressive disorder, single episode, unspecified",                  "Category": "Mental Health"},
    {"ICD10Code": "F41.1",   "Description": "Generalized anxiety disorder",                                            "Category": "Mental Health"},
    {"ICD10Code": "F10.10",  "Description": "Alcohol abuse, uncomplicated",                                            "Category": "Mental Health"},
    # Preventive / Wellness
    {"ICD10Code": "Z00.00",  "Description": "Encounter for general adult medical examination without abnormal findings","Category": "Preventive"},
    {"ICD10Code": "Z00.01",  "Description": "Encounter for general adult medical examination with abnormal findings",  "Category": "Preventive"},
    {"ICD10Code": "Z12.11",  "Description": "Encounter for screening for malignant neoplasm of colon",                "Category": "Preventive"},
    {"ICD10Code": "Z23",     "Description": "Encounter for immunization",                                              "Category": "Preventive"},
    {"ICD10Code": "Z13.88",  "Description": "Encounter for screening for disorder due to exposure to contaminants",   "Category": "Preventive"},
    # Genitourinary
    {"ICD10Code": "N39.0",   "Description": "Urinary tract infection, site not specified",                             "Category": "Genitourinary"},
    {"ICD10Code": "N18.3",   "Description": "Chronic kidney disease, stage 3 (moderate)",                             "Category": "Genitourinary"},
    # Neurological
    {"ICD10Code": "G43.909", "Description": "Migraine, unspecified, not intractable, without status migrainosus",     "Category": "Neurological"},
    {"ICD10Code": "G89.29",  "Description": "Other chronic pain",                                                     "Category": "Neurological"},
    # Symptoms
    {"ICD10Code": "R05.9",   "Description": "Cough, unspecified",                                                     "Category": "Symptoms"},
    {"ICD10Code": "R51.9",   "Description": "Headache, unspecified",                                                  "Category": "Symptoms"},
    {"ICD10Code": "R10.9",   "Description": "Unspecified abdominal pain",                                             "Category": "Symptoms"},
    {"ICD10Code": "R73.09",  "Description": "Other abnormal glucose",                                                 "Category": "Symptoms"},
]

# CPT codes available per provider specialty (by CPTCode string)
SPECIALTY_CPT_CODES = {
    "Family Medicine":    ["99202","99203","99204","99212","99213","99214","99215",
                           "99385","99386","99395","99396","90460","90471","36415","80053","85025","81001"],
    "Internal Medicine":  ["99213","99214","99215","99202","99203","99204",
                           "80053","85025","84443","83036","36415","93000"],
    "Cardiology":         ["99213","99214","99215","93000","93010","93040",
                           "93224","93306","93308","93350","93015","93017"],
    "Orthopedics":        ["99213","99214","27447","27130","29827","29881",
                           "20610","27370","73721","73723"],
    "Urgent Care":        ["99281","99282","99283","99284","99285",
                           "36415","80053","85025","81001"],
    "Nurse Practitioner": ["99211","99212","99213","99214","99385","99395",
                           "90460","90471","36415"],
    "Physician Assistant":["99211","99212","99213","99214","99281","99282","99283",
                           "36415","80053"],
}

# ICD-10 code weights per specialty
SPECIALTY_DIAG_CATEGORIES = {
    "Family Medicine":    ["Endocrine","Cardiovascular","Respiratory","Preventive","Symptoms","Mental Health","Gastrointestinal"],
    "Internal Medicine":  ["Endocrine","Cardiovascular","Respiratory","Genitourinary","Mental Health","Gastrointestinal"],
    "Cardiology":         ["Cardiovascular","Symptoms","Endocrine"],
    "Orthopedics":        ["Musculoskeletal","Symptoms"],
    "Urgent Care":        ["Respiratory","Symptoms","Musculoskeletal","Genitourinary"],
    "Nurse Practitioner": ["Endocrine","Preventive","Symptoms","Mental Health","Respiratory"],
    "Physician Assistant":["Symptoms","Respiratory","Endocrine","Musculoskeletal"],
}

# Denial reasons with ANSI X12 CARC / RARC codes
DENIAL_CATALOG = {
    "Eligibility":               {"CARC": "27",  "RARC": "N30",  "Desc": "Expenses incurred after coverage terminated"},
    "Prior Authorization":       {"CARC": "167", "RARC": "N4",   "Desc": "Claim requires prior authorization/precertification"},
    "Duplicate Claim":           {"CARC": "18",  "RARC": "MA01", "Desc": "Exact duplicate claim or service"},
    "Medical Necessity":         {"CARC": "50",  "RARC": "M1",   "Desc": "Not medically necessary per payor criteria"},
    "Coding/Bundling":           {"CARC": "97",  "RARC": "M86",  "Desc": "Payment adjusted because service included in global service payment"},
    "Filing Limit Exceeded":     {"CARC": "29",  "RARC": "MA01", "Desc": "Timely filing requirement not met"},
    "Member Not Covered":        {"CARC": "96",  "RARC": "N30",  "Desc": "Non-covered charge(s)"},
    "Invalid Modifier":          {"CARC": "4",   "RARC": "N19",  "Desc": "Procedure code inconsistent with modifier used or required"},
    "Missing Information":       {"CARC": "16",  "RARC": "N3",   "Desc": "Claim lacks information needed for adjudication"},
    "Coordination of Benefits":  {"CARC": "22",  "RARC": "MA04", "Desc": "This care may be covered by another payer per coordination of benefits"},
}

DENIAL_CATEGORIES = list(DENIAL_CATALOG.keys())

# Denial weight distributions by payer category
DENIAL_WEIGHTS = {
    "Commercial":        [0.10, 0.22, 0.08, 0.15, 0.12, 0.07, 0.08, 0.07, 0.07, 0.04],
    "Medicare":          [0.05, 0.18, 0.10, 0.22, 0.16, 0.12, 0.05, 0.05, 0.04, 0.03],
    "Medicare Advantage":[0.07, 0.20, 0.09, 0.20, 0.14, 0.09, 0.07, 0.06, 0.05, 0.03],
    "Medicaid":          [0.18, 0.15, 0.05, 0.14, 0.08, 0.08, 0.13, 0.07, 0.07, 0.05],
    "Managed Medicaid":  [0.16, 0.16, 0.06, 0.14, 0.09, 0.09, 0.12, 0.07, 0.07, 0.04],
    "Regional":          [0.12, 0.18, 0.07, 0.15, 0.12, 0.09, 0.10, 0.07, 0.06, 0.04],
}

# Allowed ratio range (allowed / billed) per payer type
ALLOWED_RATIO_RANGE = {
    "Commercial":        (0.45, 0.68),
    "Medicare":          (0.32, 0.48),
    "Medicare Advantage":(0.38, 0.55),
    "Medicaid":          (0.24, 0.42),
    "Managed Medicaid":  (0.27, 0.44),
    "Regional":          (0.38, 0.58),
}

# Patient responsibility as fraction of allowed amount
PATIENT_RESP_RANGE = {
    "Commercial":        (0.15, 0.30),
    "Medicare":          (0.18, 0.22),
    "Medicare Advantage":(0.10, 0.25),
    "Medicaid":          (0.00, 0.04),
    "Managed Medicaid":  (0.00, 0.06),
    "Regional":          (0.12, 0.28),
}

# CPT modifiers (applied stochastically)
MODIFIERS = ["25", "59", "26", "TC", "GT", "95", "51", "52", "76", "77"]

# 999 rejection reason codes (EDI functional acknowledgment)
ACK999_REJECT_CODES = {
    "001": "Transaction Set Not Supported",
    "004": "Groups Not Used",
    "005": "Invalid Interchange Content",
    "012": "Invalid ISA Segment",
    "022": "Invalid Control Number",
    "024": "Invalid Interchange Content — Agreed Upon Length Exceeded",
}

# 277CA rejection reason codes
ACK277_REJECT_CODES = {
    "A7": "Rejected for Invalid Information — Missing or invalid required field",
    "A8": "Rejected for Related Causes — Authorization not on file",
    "R1": "Rejected for Missing Information — NPI not on file",
    "R3": "Rejected for Invalid Information — Duplicate transaction",
    "E0": "Response Not Possible — System error",
}

# US state pool (patients distributed across IL + neighboring states)
US_STATES = ["IL"] * 60 + ["IN"] * 12 + ["MO"] * 10 + ["WI"] * 10 + ["IA"] * 8


# =============================================================================
# HELPER UTILITIES
# =============================================================================

def date_to_key(d: date) -> int:
    """Convert a date to YYYYMMDD integer DateKey."""
    return int(d.strftime("%Y%m%d"))


def random_npi() -> str:
    """Generate a fake 10-digit NPI."""
    return str(np.random.randint(1_000_000_000, 9_999_999_999))


def random_edi_icn() -> str:
    """Generate a 9-digit Interchange Control Number."""
    return str(np.random.randint(100_000_000, 999_999_999))


def random_member_id(payer_id: str) -> str:
    """Generate a payer-formatted member ID."""
    prefix_map = {
        "00192": "BCB", "60054": "AET", "62308": "CIG",
        "87726": "UHC", "61101": "HUM", "23284": "ANT",
        "00430": "MCR", "00610": "MAH", "00720": "MAB",
        "77350": "MCD", "82080": "MOL", "91070": "CEN",
        "34120": "PRA", "45230": "HRT", "56890": "MID",
    }
    prefix = prefix_map.get(payer_id, "INS")
    return f"{prefix}{np.random.randint(100_000_000, 999_999_999)}"


# =============================================================================
# DIMENSION GENERATORS
# =============================================================================

def gen_date_dim() -> pd.DataFrame:
    """Build a complete date spine from START_DATE to END_DATE."""
    log.info("Generating dim.Date…")
    dates = pd.date_range(START_DATE, END_DATE, freq="D")
    df = pd.DataFrame({
        "DateKey":    dates.strftime("%Y%m%d").astype(int),
        "FullDate":   dates.strftime("%Y-%m-%d"),
        "Year":       dates.year.astype("int16"),
        "Quarter":    dates.quarter.astype("int8"),
        "Month":      dates.month.astype("int8"),
        "MonthName":  dates.strftime("%B"),
        "Day":        dates.day.astype("int8"),
        "DayName":    dates.strftime("%A"),
        "WeekOfYear": dates.isocalendar().week.astype("int8"),
        "IsWeekend":  (dates.dayofweek >= 5).astype("int8"),
        "IsHoliday":  0,
    })
    # Mark common US holidays (approximate)
    holidays = {
        "2024-01-01","2024-07-04","2024-11-28","2024-12-25",
        "2025-01-01","2025-07-04","2025-11-27","2025-12-25",
    }
    df.loc[df["FullDate"].isin(holidays), "IsHoliday"] = 1
    log.info(f"  {len(df):,} date rows generated.")
    return df


def gen_location_dim() -> pd.DataFrame:
    """Return the 8 clinic locations."""
    log.info("Generating dim.Location…")
    rows = []
    for i, loc in enumerate(LOCATIONS, start=1):
        rows.append({"LocationKey": i, **loc})
    df = pd.DataFrame(rows)
    log.info(f"  {len(df)} locations.")
    return df


def gen_payer_dim() -> pd.DataFrame:
    """Return the 15 payers."""
    log.info("Generating dim.Payer…")
    rows = []
    for i, p in enumerate(PAYERS, start=1):
        rows.append({"PayerKey": i, **p})
    df = pd.DataFrame(rows)
    log.info(f"  {len(df)} payers.")
    return df


def gen_provider_dim(locations_df: pd.DataFrame) -> pd.DataFrame:
    """Generate 30 providers across the 8 locations."""
    log.info("Generating dim.Provider…")
    rows = []
    npi_pool = set()

    for i, tmpl in enumerate(PROVIDER_TEMPLATES, start=1):
        loc_key = tmpl["LocationIdx"] + 1   # 1-indexed
        loc_row = locations_df.loc[locations_df["LocationKey"] == loc_key].iloc[0]

        # Unique NPI
        npi = random_npi()
        while npi in npi_pool:
            npi = random_npi()
        npi_pool.add(npi)

        ptype = tmpl["ProviderType"]
        title = {"MD": "Dr.", "NP": "NP", "PA": "PA-C"}[ptype]
        first = fake.first_name()
        last  = fake.last_name()

        rows.append({
            "ProviderKey":  i,
            "ProviderNPI":  npi,
            "FirstName":    first,
            "LastName":     last,
            "Specialty":    tmpl["Specialty"],
            "ProviderType": ptype,
            "LocationKey":  loc_key,
            "DEANumber":    f"B{fake.bothify('?#######')}" if ptype == "MD" else None,
            "TaxID":        loc_row["TaxID"],
        })

    df = pd.DataFrame(rows)
    log.info(f"  {len(df)} providers.")
    return df


def gen_patient_dim(payers_df: pd.DataFrame, n: int = 40_000) -> pd.DataFrame:
    """Generate n patients with realistic demographic distribution."""
    log.info(f"Generating dim.Patient ({n:,})…")

    # Payer assignment weights:  40% commercial, 20% Medicare, 20% Medicaid, 20% regional
    payer_type_weights = []
    payer_keys = payers_df["PayerKey"].tolist()
    for _, row in payers_df.iterrows():
        pt = row["PayerType"]
        if pt in ("Commercial",):
            w = 0.40 / 6
        elif pt in ("Medicare", "Medicare Advantage"):
            w = 0.20 / 3
        elif pt in ("Medicaid", "Managed Medicaid"):
            w = 0.20 / 3
        else:  # Regional
            w = 0.20 / 3
        payer_type_weights.append(w)

    total_w = sum(payer_type_weights)
    payer_type_weights = [w / total_w for w in payer_type_weights]

    assigned_payer_keys = np.random.choice(payer_keys, size=n, p=payer_type_weights)

    # Build payer lookup for member ID generation
    payer_id_map = payers_df.set_index("PayerKey")["PayerID"].to_dict()

    # Generate DOBs — skewed toward working-age adults + elderly (for Medicare)
    # ~25% under 18, ~50% 18-64, ~25% 65+
    child_offsets  = np.random.randint(0,      18 * 365, size=n // 4)
    adult_offsets  = np.random.randint(18 * 365, 65 * 365, size=n // 2)
    senior_offsets = np.random.randint(65 * 365, 90 * 365,
                                       size=n - len(child_offsets) - len(adult_offsets))
    all_offsets = np.concatenate([child_offsets, adult_offsets, senior_offsets])
    np.random.shuffle(all_offsets)
    reference = date(2026, 1, 1)
    dobs = [(reference - timedelta(days=int(d))).isoformat() for d in all_offsets]

    genders = np.random.choice(["M", "F"], size=n, p=[0.48, 0.52])

    log.info("  Generating patient names (this may take ~10 s)…")
    first_names = [fake.first_name() for _ in tqdm(range(n), desc="  first names", leave=False)]
    last_names  = [fake.last_name()  for _ in tqdm(range(n), desc="  last names",  leave=False)]

    states = np.random.choice(US_STATES, size=n)
    zip_codes = [fake.zipcode_in_state(state) if hasattr(fake, "zipcode_in_state") else fake.postcode() for state in states]

    df = pd.DataFrame({
        "PatientKey":        range(1, n + 1),
        "PatientID":         [f"PAT{i:08d}" for i in range(1, n + 1)],
        "FirstName":         first_names,
        "LastName":          last_names,
        "DateOfBirth":       dobs,
        "Gender":            genders,
        "AddressLine1":      [fake.street_address() for _ in range(n)],
        "City":              [fake.city() for _ in range(n)],
        "State":             states,
        "ZipCode":           zip_codes,
        "Phone":             [fake.phone_number()[:20] for _ in range(n)],
        "InsuranceMemberID": [random_member_id(payer_id_map[k]) for k in assigned_payer_keys],
        "PrimaryPayerKey":   assigned_payer_keys,
    })

    log.info(f"  {len(df):,} patients generated.")
    return df


def gen_procedure_code_dim() -> pd.DataFrame:
    """Return CPT code dimension."""
    log.info("Generating dim.ProcedureCode…")
    rows = [{"ProcedureCodeKey": i + 1, **cpt} for i, cpt in enumerate(CPT_CODES)]
    df = pd.DataFrame(rows)
    log.info(f"  {len(df)} procedure codes.")
    return df


def gen_diagnosis_code_dim() -> pd.DataFrame:
    """Return ICD-10 diagnosis code dimension."""
    log.info("Generating dim.DiagnosisCode…")
    rows = [{"DiagnosisCodeKey": i + 1, **dx} for i, dx in enumerate(DIAGNOSIS_CODES)]
    df = pd.DataFrame(rows)
    log.info(f"  {len(df)} diagnosis codes.")
    return df


# =============================================================================
# FACT GENERATORS
# =============================================================================

def _build_specialty_cpt_key_map(proc_codes_df: pd.DataFrame) -> dict:
    """Map each specialty to a list of ProcedureCodeKey integers."""
    cpt_to_key = proc_codes_df.set_index("CPTCode")["ProcedureCodeKey"].to_dict()
    return {
        specialty: [cpt_to_key[c] for c in codes if c in cpt_to_key]
        for specialty, codes in SPECIALTY_CPT_CODES.items()
    }


def _build_specialty_diag_key_map(diag_codes_df: pd.DataFrame) -> dict:
    """Map each specialty to a list of DiagnosisCodeKey integers (filtered by category)."""
    result = {}
    for specialty, categories in SPECIALTY_DIAG_CATEGORIES.items():
        mask = diag_codes_df["Category"].isin(categories)
        result[specialty] = diag_codes_df.loc[mask, "DiagnosisCodeKey"].tolist()
        if not result[specialty]:
            result[specialty] = diag_codes_df["DiagnosisCodeKey"].tolist()
    return result


def gen_claims_and_lines(
    patients_df:    pd.DataFrame,
    providers_df:   pd.DataFrame,
    payers_df:      pd.DataFrame,
    proc_codes_df:  pd.DataFrame,
    diag_codes_df:  pd.DataFrame,
    dates_df:       pd.DataFrame,
    n_claims:       int = 100_000,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generate fact.Claims and fact.ClaimLines together so that
    TotalBilledAmount on claims equals the sum of its line ChargeAmounts.

    Returns (claims_df, lines_df).
    """
    log.info(f"Generating {n_claims:,} claims…")

    # ------------------------------------------------------------------
    # Build lookup structures
    # ------------------------------------------------------------------
    specialty_cpt_keys  = _build_specialty_cpt_key_map(proc_codes_df)
    specialty_diag_keys = _build_specialty_diag_key_map(diag_codes_df)
    cpt_charge_map      = proc_codes_df.set_index("ProcedureCodeKey")["BaseCharge"].to_dict()

    patient_payer_map   = patients_df.set_index("PatientKey")["PrimaryPayerKey"].to_dict()
    provider_loc_map    = providers_df.set_index("ProviderKey")["LocationKey"].to_dict()
    provider_spec_map   = providers_df.set_index("ProviderKey")["Specialty"].to_dict()
    payer_type_map      = payers_df.set_index("PayerKey")["PayerType"].to_dict()
    payer_denial_map    = payers_df.set_index("PayerKey")["DenialRate"].to_dict()
    payer_days_map      = payers_df.set_index("PayerKey")["AvgDaysToPayment"].to_dict()

    valid_date_keys     = dates_df["DateKey"].values
    date_key_to_date    = dates_df.set_index("DateKey")["FullDate"].to_dict()

    n_dates = len(valid_date_keys)

    # ------------------------------------------------------------------
    # Assign patients → payers → providers → locations
    # ------------------------------------------------------------------
    patient_keys   = np.random.randint(1, len(patients_df) + 1, size=n_claims)
    payer_keys     = np.array([patient_payer_map[pk] for pk in patient_keys])
    payer_types    = np.array([payer_type_map[pk]    for pk in payer_keys])
    payer_denials  = np.array([payer_denial_map[pk]  for pk in payer_keys], dtype=float)
    payer_days     = np.array([payer_days_map[pk]    for pk in payer_keys], dtype=int)

    provider_keys  = np.random.randint(1, len(providers_df) + 1, size=n_claims)
    location_keys  = np.array([provider_loc_map[pk] for pk in provider_keys])
    specialties    = np.array([provider_spec_map[pk] for pk in provider_keys])

    # ------------------------------------------------------------------
    # Service dates + submission dates
    # ------------------------------------------------------------------
    svc_offsets = np.random.randint(0, DATE_RANGE_DAYS, size=n_claims)
    svc_dates   = [START_DATE + timedelta(days=int(d)) for d in svc_offsets]
    svc_dkeys   = np.array([date_to_key(d) for d in svc_dates])

    sub_offsets = np.random.randint(1, 8, size=n_claims)
    sub_dates   = [min(svc_dates[i] + timedelta(days=int(sub_offsets[i])), END_DATE)
                   for i in range(n_claims)]
    sub_dkeys   = np.array([date_to_key(d) for d in sub_dates])

    # ------------------------------------------------------------------
    # Claim outcomes (vectorised)
    # ------------------------------------------------------------------
    end_ts = pd.Timestamp(END_DATE)
    sub_ts = pd.to_datetime(sub_dates)
    days_since_sub = (end_ts - sub_ts).days.values

    rand1 = np.random.random(n_claims)
    rand2 = np.random.random(n_claims)
    rand3 = np.random.random(n_claims)
    rand4 = np.random.random(n_claims)

    outcomes = np.full(n_claims, "Paid", dtype=object)

    # Pending (very recent claims — within 45 days of end date, 70 % still pending)
    pending_mask = (days_since_sub < 45) & (rand1 < 0.70)
    outcomes[pending_mask] = "Pending"

    active = ~pending_mask

    # 999 Rejected (2%)
    rej999_mask = active & (rand2 < 0.02)
    outcomes[rej999_mask] = "999 Rejected"
    active = active & ~rej999_mask

    # 277CA Rejected (5% of remaining)
    rej277_mask = active & (rand3 < 0.05)
    outcomes[rej277_mask] = "277CA Rejected"
    active = active & ~rej277_mask

    # Denied (payer-specific denial rate)
    denied_mask = active & (rand4 < payer_denials)
    outcomes[denied_mask] = "Denied"
    active = active & ~denied_mask

    # Partial Pay (20% of remaining)
    partial_mask = active & (np.random.random(n_claims) < 0.20)
    outcomes[partial_mask] = "Partial Pay"
    # remainder stays "Paid"

    # ------------------------------------------------------------------
    # EDI identifiers
    # ------------------------------------------------------------------
    edi_icns  = [f"ISA{random_edi_icn()}" for _ in range(n_claims)]
    fg_ids    = [f"GRP{np.random.randint(100_000, 999_999)}" for _ in range(n_claims)]
    claim_ids = [f"CLM{i:08d}" for i in range(1, n_claims + 1)]

    _pa_mask   = np.random.random(n_claims) < 0.28
    prior_auth = np.full(n_claims, None, dtype=object)
    prior_auth[_pa_mask] = [
        f"PA{np.random.randint(10_000_000, 99_999_999)}"
        for _ in range(int(_pa_mask.sum()))
    ]

    status_code_map = {
        "Paid": "1", "Partial Pay": "2", "Denied": "4",
        "277CA Rejected": "A7", "999 Rejected": "R", "Pending": "19",
    }
    claim_status_codes = [status_code_map[o] for o in outcomes]

    # ------------------------------------------------------------------
    # Build claims skeleton (amounts = 0 for now)
    # ------------------------------------------------------------------
    claims_df = pd.DataFrame({
        "ClaimKey":             range(1, n_claims + 1),
        "ClaimID":              claim_ids,
        "PatientKey":           patient_keys,
        "ProviderKey":          provider_keys,
        "LocationKey":          location_keys,
        "PayerKey":             payer_keys,
        "ServiceDateKey":       svc_dkeys,
        "SubmissionDateKey":    sub_dkeys,
        "TransactionType":      "837P",
        "ClaimStatus":          outcomes,
        "EDIInterchangeID":     edi_icns,
        "FunctionalGroupID":    fg_ids,
        "TotalBilledAmount":    0.0,
        "TotalAllowedAmount":   0.0,
        "TotalPaidAmount":      0.0,
        "TotalAdjustmentAmount":0.0,
        "PatientResponsibility":0.0,
        "ClaimStatusCode":      claim_status_codes,
        "PriorAuthNumber":      prior_auth,
        "ReferralNumber":       None,
        # Temporary columns for downstream use (dropped before save)
        "_PayerType":           payer_types,
        "_AvgDays":             payer_days,
        "_Specialty":           specialties,
        "_ServiceDate":         [d.isoformat() for d in svc_dates],
        "_SubDate":             [d.isoformat() for d in sub_dates],
    })

    log.info(f"  Claims skeleton built. Outcome breakdown:\n"
             f"    Paid={( outcomes == 'Paid').sum():,}  "
             f"Partial={(outcomes == 'Partial Pay').sum():,}  "
             f"Denied={(outcomes == 'Denied').sum():,}  "
             f"Pending={(outcomes == 'Pending').sum():,}  "
             f"277CA_Rej={(outcomes == '277CA Rejected').sum():,}  "
             f"999_Rej={(outcomes == '999 Rejected').sum():,}")

    # ------------------------------------------------------------------
    # Generate claim lines
    # ------------------------------------------------------------------
    log.info("Generating claim lines…")

    # Lines per claim: distribution gives avg ~3.05 → ~305 k lines for 100 k claims
    line_counts = np.random.choice(
        [1, 2, 3, 4, 5], size=n_claims, p=[0.15, 0.20, 0.25, 0.25, 0.15]
    )

    # Expand claim keys
    expanded_claim_keys = np.repeat(claims_df["ClaimKey"].values, line_counts)
    expanded_specialties = np.repeat(claims_df["_Specialty"].values, line_counts)
    expanded_svc_dkeys   = np.repeat(claims_df["ServiceDateKey"].values, line_counts)

    n_lines = len(expanded_claim_keys)
    log.info(f"  Total claim lines: {n_lines:,}")

    # Line numbers within each claim
    line_numbers = np.concatenate([np.arange(1, c + 1, dtype="int8") for c in line_counts])

    # Assign ProcedureCodeKeys per specialty (vectorised loop over 7 specialties)
    proc_code_keys = np.zeros(n_lines, dtype=int)
    for spec, keys in specialty_cpt_keys.items():
        if not keys:
            continue
        mask = expanded_specialties == spec
        cnt  = mask.sum()
        if cnt:
            proc_code_keys[mask] = np.random.choice(keys, size=cnt)
    # Fill any unmatched
    unmatched = proc_code_keys == 0
    if unmatched.any():
        proc_code_keys[unmatched] = np.random.choice(
            proc_codes_df["ProcedureCodeKey"].values, size=unmatched.sum()
        )

    # Assign DiagnosisCodeKeys per specialty
    diag_code_keys = np.zeros(n_lines, dtype=int)
    for spec, keys in specialty_diag_keys.items():
        if not keys:
            continue
        mask = expanded_specialties == spec
        cnt  = mask.sum()
        if cnt:
            diag_code_keys[mask] = np.random.choice(keys, size=cnt)
    unmatched = diag_code_keys == 0
    if unmatched.any():
        diag_code_keys[unmatched] = np.random.choice(
            diag_codes_df["DiagnosisCodeKey"].values, size=unmatched.sum()
        )

    # Units (mostly 1, labs can be 1-3)
    units = np.ones(n_lines, dtype="int8")
    lab_mask = np.isin(
        proc_code_keys,
        proc_codes_df.loc[proc_codes_df["Category"] == "Laboratory", "ProcedureCodeKey"].values,
    )
    units[lab_mask] = np.random.choice([1, 2, 3], size=lab_mask.sum(), p=[0.80, 0.15, 0.05])

    # Charge amounts = BaseCharge × units × variance (±20%)
    base_charges = np.array([cpt_charge_map.get(k, 150.0) for k in proc_code_keys])
    charge_variance = np.random.uniform(0.82, 1.20, size=n_lines)
    charge_amounts = np.round(base_charges * units * charge_variance, 2)

    # Modifiers (30% of lines get modifier1, 8% get modifier2)
    mod1_mask = np.random.random(n_lines) < 0.30
    mod2_mask = np.random.random(n_lines) < 0.08
    modifier1 = np.where(mod1_mask, np.random.choice(MODIFIERS, size=n_lines), None)
    modifier2 = np.where(mod2_mask, np.random.choice(MODIFIERS, size=n_lines), None)

    lines_df = pd.DataFrame({
        "ClaimLineKey":      range(1, n_lines + 1),
        "ClaimKey":          expanded_claim_keys,
        "LineNumber":        line_numbers,
        "ProcedureCodeKey":  proc_code_keys,
        "DiagnosisCodeKey":  diag_code_keys,
        "ServiceDateKey":    expanded_svc_dkeys,
        "Units":             units,
        "ChargeAmount":      charge_amounts,
        "AllowedAmount":     0.0,   # filled after adjudication
        "PaidAmount":        0.0,
        "AdjustmentAmount":  0.0,
        "Modifier1":         modifier1,
        "Modifier2":         modifier2,
        "RevenueCode":       None,
    })

    # ------------------------------------------------------------------
    # Aggregate line charges to claim totals
    # ------------------------------------------------------------------
    log.info("  Applying adjudication logic…")

    claim_billed = lines_df.groupby("ClaimKey")["ChargeAmount"].sum().rename("TotalBilledAmount")
    claims_df = claims_df.set_index("ClaimKey")
    claims_df["TotalBilledAmount"] = claim_billed
    claims_df = claims_df.reset_index()

    # Adjudication: compute allowed / paid amounts per claim
    payer_type_series = claims_df["_PayerType"]

    # Allowed ratio
    allowed_lo = payer_type_series.map(lambda pt: ALLOWED_RATIO_RANGE.get(pt, (0.40, 0.60))[0])
    allowed_hi = payer_type_series.map(lambda pt: ALLOWED_RATIO_RANGE.get(pt, (0.40, 0.60))[1])
    allowed_ratios = np.random.uniform(
        allowed_lo.values.astype(float),
        allowed_hi.values.astype(float),
    )

    # Patient responsibility ratio
    pr_lo = payer_type_series.map(lambda pt: PATIENT_RESP_RANGE.get(pt, (0.10, 0.20))[0])
    pr_hi = payer_type_series.map(lambda pt: PATIENT_RESP_RANGE.get(pt, (0.10, 0.20))[1])
    pr_ratios = np.random.uniform(pr_lo.values.astype(float), pr_hi.values.astype(float))

    # Partial pay ratio for partial claims
    partial_ratios = np.random.uniform(0.50, 0.90, size=n_claims)

    billed = claims_df["TotalBilledAmount"].values.astype(float)
    allowed = np.round(billed * allowed_ratios, 2)

    # TotalPaidAmount by outcome
    paid = np.zeros(n_claims)
    patient_resp = np.zeros(n_claims)

    paid_mask    = (outcomes == "Paid")
    partial_mask = (outcomes == "Partial Pay")
    denied_mask  = (outcomes == "Denied")

    # Fully paid
    patient_resp_amt = np.round(allowed * pr_ratios, 2)
    paid[paid_mask]         = np.round((allowed - patient_resp_amt)[paid_mask], 2)
    patient_resp[paid_mask] = patient_resp_amt[paid_mask]

    # Partially paid
    adj_allowed = np.round(allowed * partial_ratios, 2)
    paid[partial_mask]         = np.round((adj_allowed - patient_resp_amt)[partial_mask], 2)
    patient_resp[partial_mask] = patient_resp_amt[partial_mask]

    # Denied — all zeros (already zero)

    # Rejected claims — billed and allowed = 0
    rej_mask = np.isin(outcomes, ["999 Rejected", "277CA Rejected"])
    allowed[rej_mask] = 0.0
    billed[rej_mask]  = 0.0   # no claim amounts for rejected/not adjudicated

    # Adjustment = billed - paid - patient_resp  (clamp all to ≥ 0)
    paid       = np.maximum(paid, 0.0)
    patient_resp = np.maximum(patient_resp, 0.0)
    adjustment = np.maximum(np.round(billed - paid - patient_resp, 2), 0.0)

    claims_df["TotalBilledAmount"]     = billed
    claims_df["TotalAllowedAmount"]    = allowed
    claims_df["TotalPaidAmount"]       = paid
    claims_df["PatientResponsibility"] = patient_resp
    claims_df["TotalAdjustmentAmount"] = adjustment

    # Store ratios for line-level calculations
    # Avoid division by zero
    billed_safe = np.where(billed > 0, billed, 1.0)
    claims_df["_AllowedRatio"] = np.round(allowed / billed_safe, 6)
    claims_df["_PaidRatio"]    = np.where(
        allowed > 0,
        np.round(paid / np.where(allowed > 0, allowed, 1.0), 6),
        0.0,
    )

    # ------------------------------------------------------------------
    # Back-fill line amounts from claim ratios
    # ------------------------------------------------------------------
    log.info("  Back-filling line amounts…")

    claim_ratios = claims_df[["ClaimKey", "_AllowedRatio", "_PaidRatio"]].copy()
    lines_df = lines_df.merge(claim_ratios, on="ClaimKey", how="left")

    lines_df["AllowedAmount"]    = np.maximum(np.round(lines_df["ChargeAmount"] * lines_df["_AllowedRatio"], 2), 0.0)
    lines_df["PaidAmount"]       = np.maximum(np.round(lines_df["AllowedAmount"] * lines_df["_PaidRatio"], 2), 0.0)
    lines_df["AdjustmentAmount"] = np.maximum(np.round(lines_df["ChargeAmount"] - lines_df["AllowedAmount"], 2), 0.0)

    lines_df = lines_df.drop(columns=["_AllowedRatio", "_PaidRatio"])

    # Clean up temporary columns from claims before returning
    temp_cols = [c for c in claims_df.columns if c.startswith("_")]
    claims_df = claims_df.drop(columns=temp_cols)

    log.info(f"  Claims: {len(claims_df):,} | Lines: {len(lines_df):,}")
    return claims_df, lines_df


def gen_acknowledgments(claims_df: pd.DataFrame, dates_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate fact.Acknowledgments.
    Every claim → 1 x 999 acknowledgment.
    Claims not 999-rejected → 1 x 277CA acknowledgment.
    Total ≈ 198 k for 100 k claims.
    """
    log.info("Generating fact.Acknowledgments…")

    date_key_to_date = dates_df.set_index("DateKey")["FullDate"].to_dict()
    all_date_keys    = dates_df["DateKey"].values

    rows = []
    ack_key = 1

    sub_date_map = claims_df.set_index("ClaimKey")["SubmissionDateKey"].to_dict()

    for _, claim in tqdm(claims_df.iterrows(), total=len(claims_df), desc="  Acks", leave=False):
        ck       = claim["ClaimKey"]
        status   = claim["ClaimStatus"]
        sub_dkey = claim["SubmissionDateKey"]
        sub_date = date.fromisoformat(date_key_to_date[sub_dkey])

        # ---- 999 Acknowledgment ----
        ack999_date = sub_date + timedelta(days=int(np.random.randint(1, 4)))
        ack999_date = min(ack999_date, END_DATE)
        ack999_dkey = date_to_key(ack999_date)

        is_999_rejected = (status == "999 Rejected")
        if is_999_rejected:
            rej_code = random.choice(list(ACK999_REJECT_CODES.keys()))
            rej_desc = ACK999_REJECT_CODES[rej_code]
        else:
            rej_code = None
            rej_desc = None

        rows.append({
            "AcknowledgmentKey":            ack_key,
            "ClaimKey":                     ck,
            "TransactionType":              "999",
            "AcknowledgmentDateKey":        ack999_dkey,
            "AcknowledgmentDate":           ack999_date.isoformat(),
            "Status":                       "Rejected" if is_999_rejected else "Accepted",
            "RejectionReasonCode":          rej_code,
            "RejectionReasonDescription":   rej_desc,
            "InterchangeControlNumber":     random_edi_icn(),
            "FunctionalGroupControlNumber": str(np.random.randint(100_000, 999_999)),
            "TransactionSetControlNumber":  str(np.random.randint(1000, 9999)),
        })
        ack_key += 1

        # ---- 277CA Acknowledgment (skip for 999-rejected) ----
        if is_999_rejected:
            continue

        ack277_date = sub_date + timedelta(days=int(np.random.randint(3, 11)))
        ack277_date = min(ack277_date, END_DATE)
        ack277_dkey = date_to_key(ack277_date)

        is_277_rejected = (status == "277CA Rejected")
        if is_277_rejected:
            rej_code = random.choice(list(ACK277_REJECT_CODES.keys()))
            rej_desc = ACK277_REJECT_CODES[rej_code]
        else:
            rej_code = None
            rej_desc = None

        rows.append({
            "AcknowledgmentKey":            ack_key,
            "ClaimKey":                     ck,
            "TransactionType":              "277CA",
            "AcknowledgmentDateKey":        ack277_dkey,
            "AcknowledgmentDate":           ack277_date.isoformat(),
            "Status":                       "Rejected" if is_277_rejected else "Accepted",
            "RejectionReasonCode":          rej_code,
            "RejectionReasonDescription":   rej_desc,
            "InterchangeControlNumber":     random_edi_icn(),
            "FunctionalGroupControlNumber": str(np.random.randint(100_000, 999_999)),
            "TransactionSetControlNumber":  str(np.random.randint(1000, 9999)),
        })
        ack_key += 1

    df = pd.DataFrame(rows)
    log.info(f"  {len(df):,} acknowledgment rows (999 + 277CA).")
    return df


def gen_payments(
    claims_df:  pd.DataFrame,
    payers_df:  pd.DataFrame,
    dates_df:   pd.DataFrame,
) -> pd.DataFrame:
    """
    Generate fact.Payments (835 Remittance Advice).
    One payment record per adjudicated claim (Paid, Partial Pay, Denied).
    """
    log.info("Generating fact.Payments…")

    date_key_to_date = dates_df.set_index("DateKey")["FullDate"].to_dict()
    payer_type_map   = payers_df.set_index("PayerKey")["PayerType"].to_dict()
    payer_days_map   = payers_df.set_index("PayerKey")["AvgDaysToPayment"].to_dict()

    payable_statuses = {"Paid", "Partial Pay", "Denied"}
    payable = claims_df[claims_df["ClaimStatus"].isin(payable_statuses)].copy()

    rows = []
    for idx, claim in tqdm(payable.iterrows(), total=len(payable), desc="  Payments", leave=False):
        payer_type   = payer_type_map.get(claim["PayerKey"], "Commercial")
        avg_days     = payer_days_map.get(claim["PayerKey"], 30)
        sub_dkey     = claim["SubmissionDateKey"]
        sub_date     = date.fromisoformat(date_key_to_date[sub_dkey])
        claim_status = claim["ClaimStatus"]

        # Days to payment: gaussian around avg_days with std=7
        days_to_pay = max(5, int(np.random.normal(avg_days, 7)))
        pay_date    = sub_date + timedelta(days=days_to_pay)
        pay_date    = min(pay_date, END_DATE)  # cap within dim.Date boundary (2025-12-31)
        pay_dkey    = date_to_key(pay_date)

        # Denial category and CARC/RARC
        if claim_status == "Denied":
            denial_cat = random.choices(
                DENIAL_CATEGORIES,
                weights=DENIAL_WEIGHTS.get(payer_type, DENIAL_WEIGHTS["Commercial"]),
            )[0]
            carc = DENIAL_CATALOG[denial_cat]["CARC"]
            rarc = DENIAL_CATALOG[denial_cat]["RARC"]
        elif claim_status in ("Paid", "Partial Pay"):
            denial_cat = None
            # Paid claims still show contractual adjustment CARC
            carc = random.choice(["45", "3", "2", "1"])
            rarc = random.choice(["N1", "N10", "MA01"])
        else:
            denial_cat = None
            carc = None
            rarc = None

        # Payment method: EFT preferred (80%), some checks
        method = "EFT" if np.random.random() < 0.82 else "CHK"
        check_eft = f"{method}{np.random.randint(1_000_000_000, 9_999_999_999)}"

        rows.append({
            "PaymentKey":            idx + 1,   # use position-based key
            "ClaimKey":              claim["ClaimKey"],
            "RemittanceNumber":      f"ERA{np.random.randint(100_000_000, 999_999_999)}",
            "PaymentDateKey":        pay_dkey,
            "PaymentDate":           pay_date.isoformat(),
            "BilledAmount":          claim["TotalBilledAmount"],
            "AllowedAmount":         claim["TotalAllowedAmount"],
            "PaidAmount":            claim["TotalPaidAmount"],
            "AdjustmentAmount":      claim["TotalAdjustmentAmount"],
            "PatientResponsibility": claim["PatientResponsibility"],
            "CARC":                  carc,
            "RARC":                  rarc,
            "DenialCategory":        denial_cat,
            "DaysToPayment":         days_to_pay,
            "PaymentMethod":         method,
            "CheckEFTNumber":        check_eft,
        })

    # Re-key from 1
    df = pd.DataFrame(rows)
    df["PaymentKey"] = range(1, len(df) + 1)
    log.info(f"  {len(df):,} payment rows.")
    return df


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================

def main():
    log.info("=" * 60)
    log.info("Healthcare EDI Demo Data Generator")
    log.info("=" * 60)

    # 1. Dimension tables
    dates_df     = gen_date_dim()
    locations_df = gen_location_dim()
    payers_df    = gen_payer_dim()
    providers_df = gen_provider_dim(locations_df)
    patients_df  = gen_patient_dim(payers_df, n=DATA_CONFIG["num_patients"])
    proc_df      = gen_procedure_code_dim()
    diag_df      = gen_diagnosis_code_dim()

    # 2. Fact tables
    claims_df, lines_df = gen_claims_and_lines(
        patients_df, providers_df, payers_df, proc_df, diag_df, dates_df,
        n_claims=DATA_CONFIG["num_claims"],
    )
    acks_df     = gen_acknowledgments(claims_df, dates_df)
    payments_df = gen_payments(claims_df, payers_df, dates_df)

    # 2b. Referential integrity assertions
    assert claims_df["ClaimID"].is_unique, "ClaimIDs are not unique"
    _valid_claim_keys = set(claims_df["ClaimKey"].tolist())
    assert set(lines_df["ClaimKey"].tolist()).issubset(_valid_claim_keys), \
        "fact.ClaimLines references ClaimKeys not in fact.Claims"
    assert set(acks_df["ClaimKey"].tolist()).issubset(_valid_claim_keys), \
        "fact.Acknowledgments references ClaimKeys not in fact.Claims"
    assert set(payments_df["ClaimKey"].tolist()).issubset(_valid_claim_keys), \
        "fact.Payments references ClaimKeys not in fact.Claims"
    log.info("  Referential integrity checks: PASSED")

    # 3. Save to CSV
    log.info(f"Saving CSV files to ./{OUTPUT_DIR}/")

    def save(df: pd.DataFrame, name: str):
        path = os.path.join(OUTPUT_DIR, f"{name}.csv")
        df.to_csv(path, index=False)
        log.info(f"  Saved {name}.csv  ({len(df):,} rows, {os.path.getsize(path)/1_048_576:.1f} MB)")

    save(dates_df,     "dim_Date")
    save(locations_df, "dim_Location")
    save(payers_df,    "dim_Payer")
    save(providers_df, "dim_Provider")
    save(patients_df,  "dim_Patient")
    save(proc_df,      "dim_ProcedureCode")
    save(diag_df,      "dim_DiagnosisCode")
    save(claims_df,    "fact_Claims")
    save(lines_df,     "fact_ClaimLines")
    save(acks_df,      "fact_Acknowledgments")
    save(payments_df,  "fact_Payments")

    log.info("=" * 60)
    log.info("Generation complete. Run load_to_azure_sql.py to load data.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
