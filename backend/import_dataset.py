"""Safely import a bounded dataset sample into Supabase.

The source files are streamed in small batches so they do not consume the
memory used by the API or machine-learning models.
"""

import csv
import os
from pathlib import Path

from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv(Path(__file__).parent.parent / ".env")

DATA_DIR = Path(r"C:\Users\ayush\Downloads\Data")
DATASET = os.environ.get("FINGUARD_DATASET", "LI-Small")
MAX_TRANSACTIONS = 25_000
BATCH_SIZE = 250


def account_id(account_number: str) -> str:
    prefix = DATASET.replace("-", "").upper()
    return f"{prefix}-{account_number.strip()}"


def transaction_id(index: int) -> str:
    prefix = DATASET.replace("-", "").upper()
    return f"{prefix}-TX-{index:08d}"


def read_transactions():
    path = DATA_DIR / f"{DATASET}_Trans.csv"
    selected = []
    account_numbers = set()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        next(reader)
        for index, row in enumerate(reader):
            if index >= MAX_TRANSACTIONS:
                break
            selected.append(row)
            account_numbers.add(row[2].strip())
            account_numbers.add(row[4].strip())
    return selected, account_numbers


def read_accounts(account_numbers):
    path = DATA_DIR / f"{DATASET}_accounts.csv"
    accounts = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            number = row["Account Number"].strip()
            if number not in account_numbers:
                continue
            accounts.append(
                {
                    "account_id": account_id(number),
                    "holder_name": row["Entity Name"].strip(),
                    "account_number": number,
                    "ifsc_code": row["Bank ID"].strip(),
                    "region": DATASET,
                    "city": row["Bank Name"].strip(),
                    "age_months": 0,
                    "risk_score": 10,
                    "risk_level": "LOW",
                    "credit_amount": 0,
                    "debit_amount": 0,
                    "payment_methods": [],
                }
            )
    return accounts


def chunks(rows):
    for start in range(0, len(rows), BATCH_SIZE):
        yield rows[start : start + BATCH_SIZE]


def import_data(client: Client):
    transactions, account_numbers = read_transactions()
    accounts = read_accounts(account_numbers)
    known_accounts = {row["account_number"] for row in accounts}

    missing = account_numbers - known_accounts
    for number in missing:
        accounts.append(
            {
                "account_id": account_id(number),
                "holder_name": "Imported dataset account",
                "account_number": number,
                "ifsc_code": "UNKNOWN",
                "region": DATASET,
                "city": "Unknown",
                "age_months": 0,
                "risk_score": 10,
                "risk_level": "LOW",
                "credit_amount": 0,
                "debit_amount": 0,
                "payment_methods": [],
            }
        )

    for batch in chunks(accounts):
        client.table("accounts").upsert(batch, on_conflict="account_id").execute()

    converted = []
    for index, row in enumerate(transactions, start=1):
        laundering = row[10].strip() == "1"
        sender = row[2].strip()
        receiver = row[4].strip()
        amount = float(row[5])
        converted.append(
            {
                "transaction_id": transaction_id(index),
                "timestamp": row[0].strip(),
                "sender_id": account_id(sender),
                "sender_name": "Imported dataset account",
                "receiver_id": account_id(receiver),
                "receiver_name": "Imported dataset account",
                "amount": amount,
                "transaction_type": "Dataset transfer",
                "payment_method": row[9].strip(),
                "region": DATASET,
                "city": "Imported",
                "risk_score": 95 if laundering else 10,
                "risk_level": "CRITICAL" if laundering else "LOW",
                "status": "Flagged" if laundering else "Cleared",
                "reasons": ["Dataset marked as laundering"] if laundering else [],
                "from_bank": int(row[1]),
                "to_bank": int(row[3]),
                "receiving_currency": row[6].strip(),
                "payment_currency": row[8].strip(),
                "payment_format": row[9].strip(),
            }
        )

    for batch in chunks(converted):
        client.table("transactions").upsert(batch, on_conflict="transaction_id").execute()

    print(f"Imported {len(accounts)} accounts and {len(converted)} transactions from {DATASET}.")


if __name__ == "__main__":
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be configured in .env")
    import_data(create_client(url, key))
