"""
Synthetic Data Generator — Azure Databricks Customer 360 Lakehouse
Generates realistic banking datasets for local development and portfolio demos.
Run: python generate_synthetic_data.py
Output: ./synthetic/*.csv (5 files, ~5,000 total rows)
"""

import csv
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)
OUT = Path(__file__).parent / "synthetic"
OUT.mkdir(exist_ok=True)

# ── helpers ──────────────────────────────────────────────────────────────────

FIRST_NAMES = [
    "James","Mary","Robert","Patricia","John","Jennifer","Michael","Linda",
    "William","Barbara","David","Elizabeth","Richard","Susan","Joseph","Jessica",
    "Thomas","Sarah","Charles","Karen","Wei","Priya","Amir","Sofia","Yuki",
    "Fatima","Carlos","Ingrid","Dmitri","Amara",
]
LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
    "Wilson","Taylor","Anderson","Thomas","Jackson","White","Harris","Martin",
    "Thompson","Robinson","Lewis","Walker","Patel","Kim","Nguyen","Chen","Singh",
]
STATES = ["NY","CA","TX","FL","IL","PA","OH","GA","NC","MI","NJ","VA","WA","AZ","MA"]
CITIES = {
    "NY":"New York","CA":"Los Angeles","TX":"Houston","FL":"Miami",
    "IL":"Chicago","PA":"Philadelphia","OH":"Columbus","GA":"Atlanta",
    "NC":"Charlotte","MI":"Detroit","NJ":"Newark","VA":"Richmond",
    "WA":"Seattle","AZ":"Phoenix","MA":"Boston",
}
SEGMENTS = ["Retail","Premier","Private","SMB","Corporate"]
SEGMENT_WEIGHTS = [0.50, 0.25, 0.10, 0.10, 0.05]
PRODUCTS = ["Checking","Savings","Mortgage","Auto Loan","Credit Card","Investment","HELOC","CD"]
ACCT_STATUS = ["Active","Dormant","Closed","Suspended"]
TXN_TYPES = ["Purchase","ATM Withdrawal","Transfer","Bill Payment","Direct Deposit",
             "Wire Transfer","Refund","Fee","Interest Credit"]
CHANNELS = ["Mobile","Web","Branch","ATM","Phone","API"]
TICKET_CATEGORIES = ["Account Access","Fraud Alert","Transaction Dispute",
                     "Card Issue","Loan Inquiry","Fee Waiver","General Inquiry"]
TICKET_STATUS = ["Open","In Progress","Resolved","Closed","Escalated"]
RESOLUTIONS = ["Issue Resolved","Credited Account","Referred to Branch",
               "No Action Required","Escalated to Manager","Pending Customer Response"]
BRANCH_NAMES = [
    "Downtown Main","Midtown North","Westside Financial","Airport Hub",
    "University District","Harbor View","Uptown Wealth Center","Eastside Community",
    "Tech Corridor","Suburban South","Green Hills","Lakeshore","Metro Central",
    "Commerce Park","Riverside",
]


def rand_date(start: datetime, end: datetime) -> datetime:
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))


def rand_ssn_last4() -> str:
    return str(random.randint(1000, 9999))


def phone() -> str:
    return f"+1-{random.randint(200,999)}-{random.randint(200,999)}-{random.randint(1000,9999)}"


def email(first: str, last: str) -> str:
    domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com"]
    return f"{first.lower()}.{last.lower()}{random.randint(1,99)}@{random.choice(domains)}"


# ── 1. customers (500 rows) ───────────────────────────────────────────────────

NUM_CUSTOMERS = 500
customers = []
customer_ids = [str(uuid.uuid4()) for _ in range(NUM_CUSTOMERS)]
DOB_START = datetime(1950, 1, 1)
DOB_END   = datetime(2000, 12, 31)
JOIN_START = datetime(2010, 1, 1)
JOIN_END   = datetime(2024, 6, 30)

for cid in customer_ids:
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)
    state = random.choice(STATES)
    dob = rand_date(DOB_START, DOB_END).strftime("%Y-%m-%d")
    joined = rand_date(JOIN_START, JOIN_END).strftime("%Y-%m-%d")
    segment = random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS)[0]
    credit_score = random.randint(580, 850)
    annual_income = random.randint(30_000, 500_000)
    customers.append({
        "customer_id": cid,
        "first_name": fn,
        "last_name": ln,
        "email": email(fn, ln),
        "phone": phone(),
        "date_of_birth": dob,
        "ssn_last4": rand_ssn_last4(),
        "address_line1": f"{random.randint(100,9999)} {random.choice(['Main','Oak','Elm','Park','Maple'])} St",
        "city": CITIES[state],
        "state": state,
        "zip_code": str(random.randint(10000, 99999)),
        "country": "US",
        "segment": segment,
        "credit_score": credit_score,
        "annual_income": annual_income,
        "kyc_status": random.choices(["Verified","Pending","Failed"], weights=[0.88,0.09,0.03])[0],
        "is_pep": random.choices([True, False], weights=[0.02, 0.98])[0],
        "preferred_channel": random.choice(CHANNELS),
        "customer_since": joined,
        "source_system": random.choice(["CRM","Branch","Online","Partner"]),
        "record_created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    })

with open(OUT / "customers.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=customers[0].keys())
    w.writeheader(); w.writerows(customers)
print(f"customers.csv          → {len(customers):,} rows")


# ── 2. accounts (800 rows) ────────────────────────────────────────────────────

NUM_ACCOUNTS = 800
accounts = []
account_ids = [str(uuid.uuid4()) for _ in range(NUM_ACCOUNTS)]

for aid in account_ids:
    owner = random.choice(customer_ids)
    product = random.choice(PRODUCTS)
    opened = rand_date(JOIN_START, JOIN_END)
    balance = round(random.uniform(0, 500_000), 2)
    credit_limit = round(random.uniform(1_000, 50_000), 2) if product in ("Credit Card","HELOC") else None
    interest_rate = round(random.uniform(0.01, 0.18), 4) if product not in ("Checking","Savings") else round(random.uniform(0.001, 0.05), 4)
    status = random.choices(ACCT_STATUS, weights=[0.80, 0.08, 0.09, 0.03])[0]
    closed_date = rand_date(opened, datetime(2024, 6, 30)).strftime("%Y-%m-%d") if status == "Closed" else None
    accounts.append({
        "account_id": aid,
        "customer_id": owner,
        "account_number": f"ACCT{random.randint(10_000_000, 99_999_999)}",
        "product_type": product,
        "account_status": status,
        "open_date": opened.strftime("%Y-%m-%d"),
        "close_date": closed_date,
        "balance": balance,
        "available_balance": round(balance * random.uniform(0.7, 1.0), 2),
        "credit_limit": credit_limit,
        "interest_rate": interest_rate,
        "branch_code": f"BR{random.randint(100, 999)}",
        "currency": "USD",
        "overdraft_protection": random.choice([True, False]),
        "paperless_enrolled": random.choice([True, False]),
        "last_activity_date": rand_date(opened, datetime(2024, 9, 30)).strftime("%Y-%m-%d"),
        "source_system": "Core Banking",
        "record_created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    })

with open(OUT / "accounts.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=accounts[0].keys())
    w.writeheader(); w.writerows(accounts)
print(f"accounts.csv           → {len(accounts):,} rows")


# ── 3. transactions (3,000 rows) ──────────────────────────────────────────────

TXN_START = datetime(2023, 1, 1)
TXN_END   = datetime(2024, 9, 30)
transactions = []

for _ in range(3000):
    acct = random.choice(accounts)
    txn_type = random.choice(TXN_TYPES)
    amount = round(random.uniform(1, 15_000), 2)
    if txn_type in ("Fee","Interest Credit"):
        amount = round(random.uniform(1, 50), 2)
    elif txn_type == "Wire Transfer":
        amount = round(random.uniform(1_000, 100_000), 2)
    txn_date = rand_date(TXN_START, TXN_END)
    transactions.append({
        "transaction_id": str(uuid.uuid4()),
        "account_id": acct["account_id"],
        "customer_id": acct["customer_id"],
        "transaction_type": txn_type,
        "amount": amount,
        "currency": "USD",
        "direction": "Debit" if txn_type in ("Purchase","ATM Withdrawal","Transfer","Bill Payment","Wire Transfer","Fee") else "Credit",
        "channel": random.choice(CHANNELS),
        "merchant_name": f"Merchant_{random.randint(1,200)}" if txn_type == "Purchase" else None,
        "merchant_category": random.choice(["Retail","Grocery","Travel","Dining","Healthcare","Utilities","Entertainment"]) if txn_type == "Purchase" else None,
        "transaction_date": txn_date.strftime("%Y-%m-%d"),
        "transaction_time": txn_date.strftime("%H:%M:%S"),
        "posting_date": (txn_date + timedelta(days=random.randint(0, 2))).strftime("%Y-%m-%d"),
        "balance_after": round(random.uniform(0, 500_000), 2),
        "is_flagged": random.choices([True, False], weights=[0.03, 0.97])[0],
        "fraud_score": round(random.uniform(0, 1), 4),
        "reference_number": f"REF{random.randint(1_000_000, 9_999_999)}",
        "description": f"{txn_type} — {random.randint(10000,99999)}",
        "source_system": "Transaction Processing",
        "record_created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    })

with open(OUT / "transactions.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=transactions[0].keys())
    w.writeheader(); w.writerows(transactions)
print(f"transactions.csv       → {len(transactions):,} rows")


# ── 4. branches (15 rows) ─────────────────────────────────────────────────────

branches = []
for i, name in enumerate(BRANCH_NAMES):
    state = random.choice(STATES)
    branches.append({
        "branch_id": f"BR{100 + i}",
        "branch_name": name,
        "branch_type": random.choice(["Full Service","Drive-Through","Wealth Center","Express"]),
        "address": f"{random.randint(100, 9999)} {random.choice(['Main','Commerce','Financial','Park'])} Ave",
        "city": CITIES[state],
        "state": state,
        "zip_code": str(random.randint(10000, 99999)),
        "phone": phone(),
        "manager_name": f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
        "open_date": rand_date(datetime(1990, 1, 1), datetime(2020, 1, 1)).strftime("%Y-%m-%d"),
        "is_active": random.choices([True, False], weights=[0.93, 0.07])[0],
        "atm_count": random.randint(1, 6),
        "sq_footage": random.randint(1_000, 8_000),
        "record_created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    })

with open(OUT / "branches.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=branches[0].keys())
    w.writeheader(); w.writerows(branches)
print(f"branches.csv           → {len(branches):,} rows")


# ── 5. support tickets (700 rows) ─────────────────────────────────────────────

tickets = []
TICKET_START = datetime(2023, 1, 1)
TICKET_END   = datetime(2024, 9, 30)

for _ in range(700):
    cid = random.choice(customer_ids)
    created = rand_date(TICKET_START, TICKET_END)
    status = random.choice(TICKET_STATUS)
    resolved_date = (created + timedelta(days=random.randint(1, 14))).strftime("%Y-%m-%d") if status in ("Resolved","Closed") else None
    csat = random.randint(1, 5) if status in ("Resolved","Closed") else None
    tickets.append({
        "ticket_id": str(uuid.uuid4()),
        "customer_id": cid,
        "category": random.choice(TICKET_CATEGORIES),
        "sub_category": f"Sub_{random.randint(1,5)}",
        "channel": random.choice(CHANNELS),
        "priority": random.choices(["Low","Medium","High","Critical"], weights=[0.35,0.40,0.20,0.05])[0],
        "status": status,
        "created_date": created.strftime("%Y-%m-%d"),
        "created_time": created.strftime("%H:%M:%S"),
        "resolved_date": resolved_date,
        "resolution": random.choice(RESOLUTIONS) if resolved_date else None,
        "agent_id": f"AGENT{random.randint(100,199)}",
        "branch_id": random.choice([b["branch_id"] for b in branches]) if random.random() < 0.3 else None,
        "csat_score": csat,
        "is_escalated": random.choices([True, False], weights=[0.08, 0.92])[0],
        "notes": f"Customer contacted regarding {random.choice(TICKET_CATEGORIES).lower()}.",
        "source_system": "CRM",
        "record_created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    })

with open(OUT / "support_tickets.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=tickets[0].keys())
    w.writeheader(); w.writerows(tickets)
print(f"support_tickets.csv    → {len(tickets):,} rows")

print(f"\nAll datasets written to {OUT.resolve()}")
print(f"Total rows generated: {sum([NUM_CUSTOMERS, NUM_ACCOUNTS, 3000, len(branches), len(tickets)]):,}")
