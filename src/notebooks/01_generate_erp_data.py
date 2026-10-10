"""
01_generate_erp_data.py
Generates synthetic Healthcare ERP extracts and drops them as JSON files into the
UC landing volume, exactly like a real ERP/HIS export would:

  /Volumes/<catalog>/landing/raw_files/
      patients/      CDC feed (INSERT / UPDATE / DELETE with sequence number)
      encounters/    admissions, OPD visits, ER visits
      claims/        insurance / TPA claims
      inventory/     pharmacy & supply stock movements
      staff/         doctors, nurses, admin staff

~2% of rows are deliberately dirty so the SDP expectations have something to catch.
All data is fake (Faker) — no real PHI.
"""
import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--batch-size", type=int, default=500)
args = parser.parse_args()

fake = Faker("en_IN")
Faker.seed(random.randint(0, 10_000))
BASE = f"/Volumes/{args.catalog}/landing/raw_files"
NOW = datetime.utcnow()
BATCH = NOW.strftime("%Y%m%d_%H%M%S")
N = args.batch_size

FACILITIES = ["FAC-CHN", "FAC-BLR", "FAC-HYD", "FAC-MUM"]
DEPTS = ["Cardiology", "Orthopedics", "Oncology", "Pediatrics", "General Medicine",
         "Neurology", "Emergency", "Obstetrics"]
ICD10 = {"I21.9": "Acute myocardial infarction", "E11.9": "Type 2 diabetes",
         "J18.9": "Pneumonia", "S72.0": "Fracture of femur neck", "C50.9": "Breast cancer",
         "O80": "Normal delivery", "I63.9": "Cerebral infarction", "A09": "Gastroenteritis",
         "J45.9": "Asthma", "N39.0": "Urinary tract infection"}
PAYERS = ["Star Health", "HDFC ERGO", "ICICI Lombard", "CGHS", "PM-JAY", "Self-Pay", "Niva Bupa"]
ITEMS = [("MED-001", "Paracetamol 500mg", "Pharmacy"), ("MED-002", "Amoxicillin 250mg", "Pharmacy"),
         ("MED-003", "Insulin Glargine", "Pharmacy"), ("MED-004", "Atorvastatin 20mg", "Pharmacy"),
         ("SUP-001", "Surgical Gloves (box)", "Supplies"), ("SUP-002", "IV Cannula 20G", "Supplies"),
         ("SUP-003", "N95 Mask", "Supplies"), ("SUP-004", "Syringe 5ml", "Supplies")]


def write(entity: str, rows: list[dict]) -> None:
    """UC Volumes are FUSE-mounted, so plain Python file IO works on serverless."""
    folder = f"{BASE}/{entity}"
    os.makedirs(folder, exist_ok=True)
    path = f"{folder}/{entity}_{BATCH}.json"
    with open(path, "w") as fh:
        fh.write("\n".join(json.dumps(r, default=str) for r in rows))
    print(f"wrote {len(rows):>5} -> {path}")


def maybe_dirty(value, bad):
    return bad if random.random() < 0.02 else value


# ---------- patients (CDC) -------------------------------------------------
patients = []
for i in range(N):
    pid = f"PAT-{random.randint(1, N * 4):06d}"
    patients.append({
        "patient_id": pid,
        "mrn": f"MRN{random.randint(10**7, 10**8 - 1)}",
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "dob": str(fake.date_of_birth(minimum_age=0, maximum_age=95)),
        "gender": random.choice(["M", "F", "O"]),
        "phone": maybe_dirty(fake.phone_number(), "N/A"),
        "email": maybe_dirty(fake.email(), "not-an-email"),
        "aadhaar_no": f"{random.randint(1000,9999)} {random.randint(1000,9999)} {random.randint(1000,9999)}",
        "address": fake.address().replace("\n", ", "),
        "city": fake.city(),
        "blood_group": random.choice(["A+", "A-", "B+", "B-", "O+", "O-", "AB+", "AB-"]),
        "home_facility_id": random.choice(FACILITIES),
        "operation": random.choices(["INSERT", "UPDATE", "DELETE"], [0.75, 0.22, 0.03])[0],
        "sequence_num": int(NOW.timestamp() * 1000) + i,
        "source_system": "HIS-PatientMaster",
        "extracted_at": NOW.isoformat(),
    })
write("patients", patients)

# ---------- staff -----------------------------------------------------------
staff = [{
    "staff_id": f"STF-{i:05d}",
    "full_name": fake.name(),
    "role": random.choice(["Doctor", "Nurse", "Technician", "Admin", "Pharmacist"]),
    "department": random.choice(DEPTS),
    "facility_id": random.choice(FACILITIES),
    "monthly_salary_inr": random.randint(25_000, 450_000),
    "pan_no": fake.bothify("?????####?").upper(),
    "joined_on": str(fake.date_between("-10y", "today")),
    "extracted_at": NOW.isoformat(),
} for i in range(1, max(60, N // 8))]
write("staff", staff)
doctor_ids = [s["staff_id"] for s in staff if s["role"] == "Doctor"] or ["STF-00001"]

# ---------- encounters ------------------------------------------------------
encounters = []
for _ in range(N * 2):
    etype = random.choices(["INPATIENT", "OUTPATIENT", "EMERGENCY"], [0.35, 0.5, 0.15])[0]
    admit = NOW - timedelta(days=random.randint(0, 120), hours=random.randint(0, 23))
    los = random.randint(1, 15) if etype == "INPATIENT" else 0
    discharge = None if (etype == "INPATIENT" and random.random() < 0.15) else admit + timedelta(days=los, hours=random.randint(1, 8))
    code = random.choice(list(ICD10))
    encounters.append({
        "encounter_id": str(uuid.uuid4()),
        "patient_id": random.choice(patients)["patient_id"],
        "facility_id": maybe_dirty(random.choice(FACILITIES), None),
        "department": random.choice(DEPTS),
        "attending_doctor_id": random.choice(doctor_ids),
        "encounter_type": etype,
        "admit_ts": admit.isoformat(),
        "discharge_ts": discharge.isoformat() if discharge else None,
        "icd10_code": code,
        "diagnosis_desc": ICD10[code],
        "bed_no": f"B-{random.randint(100, 499)}" if etype == "INPATIENT" else None,
        "total_charges_inr": maybe_dirty(round(random.uniform(800, 450_000), 2), -1.0),
        "readmission_flag": random.random() < 0.08,
        "extracted_at": NOW.isoformat(),
    })
write("encounters", encounters)

# ---------- claims ----------------------------------------------------------
claims = []
for e in random.sample(encounters, k=int(len(encounters) * 0.7)):
    status = random.choices(["SUBMITTED", "APPROVED", "PARTIALLY_APPROVED", "DENIED", "PAID"],
                            [0.15, 0.25, 0.15, 0.1, 0.35])[0]
    billed = e["total_charges_inr"] if e["total_charges_inr"] > 0 else 1000.0
    approved = {"DENIED": 0, "SUBMITTED": None, "PARTIALLY_APPROVED": billed * random.uniform(0.4, 0.9)}.get(status, billed)
    claims.append({
        "claim_id": f"CLM-{uuid.uuid4().hex[:10].upper()}",
        "encounter_id": e["encounter_id"],
        "patient_id": e["patient_id"],
        "payer": random.choice(PAYERS),
        "policy_no": fake.bothify("POL-########"),
        "billed_amount_inr": billed,
        "approved_amount_inr": round(approved, 2) if approved is not None else None,
        "claim_status": status,
        "denial_reason": random.choice(["Pre-auth missing", "Non-covered service", "Documentation incomplete", "Policy lapsed"]) if status == "DENIED" else None,
        "submitted_at": (datetime.fromisoformat(e["admit_ts"]) + timedelta(days=random.randint(1, 10))).isoformat(),
        "extracted_at": NOW.isoformat(),
    })
write("claims", claims)

# ---------- inventory movements ---------------------------------------------
inventory = []
for _ in range(N):
    code, name, cat = random.choice(ITEMS)
    inventory.append({
        "txn_id": str(uuid.uuid4()),
        "item_code": code,
        "item_name": name,
        "category": cat,
        "facility_id": random.choice(FACILITIES),
        "txn_type": random.choices(["RECEIPT", "ISSUE", "ADJUSTMENT", "EXPIRED"], [0.3, 0.6, 0.05, 0.05])[0],
        "quantity": maybe_dirty(random.randint(1, 500), 0),
        "unit_cost_inr": round(random.uniform(2, 1800), 2),
        "batch_no": fake.bothify("BT-####??").upper(),
        "expiry_date": str(fake.date_between("-30d", "+2y")),
        "reorder_level": random.choice([50, 100, 200]),
        "txn_ts": (NOW - timedelta(days=random.randint(0, 60))).isoformat(),
        "extracted_at": NOW.isoformat(),
    })
write("inventory", inventory)

print("✅ synthetic ERP batch", BATCH, "landed")
