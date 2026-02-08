import os
import requests
from dotenv import load_dotenv

# =====================================================
# Load ERP credentials from .env (no hardcoded values)
# =====================================================
load_dotenv()
ERP_URL = os.getenv("ERP_URL")
ERP_API_KEY = os.getenv("API_KEY")
ERP_API_SECRET = os.getenv("API_SECRET")

HEADERS = {}
if ERP_API_KEY and ERP_API_SECRET:
    HEADERS = {
        "Authorization": f"token {ERP_API_KEY}:{ERP_API_SECRET}",
        "Content-Type": "application/json"
    }

# =====================================================
# CREATE WORK ORDER IN ERP
# =====================================================
def create_work_order(machine_id, qty):
    if not ERP_URL or not HEADERS:
        return {"success": False, "message": "ERP credentials missing"}

    payload = {"machine_id": machine_id, "qty": qty}
    try:
        res = requests.post(f"{ERP_URL}/api/method/create_work_order", json=payload, headers=HEADERS, timeout=10)
        res.raise_for_status()
        return res.json()
    except requests.RequestException as e:
        return {"success": False, "message": str(e)}

# =====================================================
# UPDATE WORK ORDER STATUS
# =====================================================
def update_work_order_status(machine_id, status):
    if not ERP_URL or not HEADERS:
        return {"success": False, "message": "ERP credentials missing"}

    payload = {"machine_id": machine_id, "status": status}
    try:
        res = requests.post(f"{ERP_URL}/api/method/update_work_order_status", json=payload, headers=HEADERS, timeout=10)
        res.raise_for_status()
        return res.json()
    except requests.RequestException as e:
        return {"success": False, "message": str(e)}