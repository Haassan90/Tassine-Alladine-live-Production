# =====================================================
# erpnext_sync.py
# ERPNext Production Integration (FINAL) - FULL
# Safe Auto-Assignment + Real Sync + No Demo Logic
# =====================================================

import os
import requests
import asyncio
from typing import List, Dict
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError

from database import SessionLocal
from models import Machine, ERPNextMetadata

# =====================================================
# ERP CONFIG
# =====================================================
load_dotenv()

ERP_URL = os.getenv("ERP_URL")
API_KEY = os.getenv("ERP_API_KEY")
API_SECRET = os.getenv("ERP_API_SECRET")
TIMEOUT = 10

HEADERS = {
    "Authorization": f"token {API_KEY}:{API_SECRET}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# =====================================================
# FETCH ACTIVE WORK ORDERS FROM ERPNext
# =====================================================
def get_work_orders() -> List[Dict]:
    """
    Fetch ONLY actionable ERPNext Work Orders
    Allowed statuses:
    - Not Started
    - In Process
    """
    if not ERP_URL or not API_KEY or not API_SECRET:
        return []

    url = f"{ERP_URL}/api/resource/Work Order"
    params = {
        "fields": (
            '["name","qty","produced_qty","status",'
            '"custom_machine_id","custom_pipe_size","custom_location"]'
        ),
        "filters": '[["status","in",["Not Started","In Process"]]]'
    }

    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json().get("data", []) or []
    except Exception as e:
        print("❌ ERP fetch error:", e)
        return []

# =====================================================
# UPDATE ERP WORK ORDER STATUS
# =====================================================
def update_work_order_status(erp_work_order_id: str, status: str):
    """
    Update ERPNext Work Order status:
    - In Process
    - Completed
    """
    try:
        url = f"{ERP_URL}/api/resource/Work Order/{erp_work_order_id}"
        requests.put(
            url,
            json={"status": status},
            headers=HEADERS,
            timeout=TIMEOUT
        ).raise_for_status()
        print(f"✅ ERP Work Order {erp_work_order_id} → {status}")
    except Exception as e:
        print("❌ ERP status update failed:", e)

# =====================================================
# AUTO ASSIGN ERP WORK ORDERS TO MACHINES (SAFE)
# =====================================================
def auto_assign_work_orders():
    """
    Assign ERPNext Work Orders to free machines based on:
    - Location
    - Pipe size preference
    - Machine availability
    """
    db = SessionLocal()
    try:
        work_orders = get_work_orders()
        if not work_orders:
            return

        for wo in work_orders:
            wo_name = wo["name"]
            wo_status = wo["status"]

            # Do not reassign running orders
            if wo_status == "In Process":
                continue

            # ERP already assigned machine
            if wo.get("custom_machine_id"):
                continue

            location = wo.get("custom_location")
            pipe_size = wo.get("custom_pipe_size")
            qty = wo.get("qty", 0)
            produced = wo.get("produced_qty", 0)

            # Already assigned locally
            existing = db.query(Machine).filter(
                Machine.erpnext_work_order_id == wo_name
            ).first()
            if existing:
                continue

            # Find free machines
            free_machines = db.query(Machine).filter(
                Machine.location == location,
                Machine.is_locked == False,
                Machine.status.in_(["free", "paused", "stopped"])
            ).all()

            if not free_machines:
                continue

            # Prefer pipe size match
            selected_machine = None
            for m in free_machines:
                if m.pipe_size == pipe_size:
                    selected_machine = m
                    break

            if not selected_machine:
                selected_machine = free_machines[0]

            # Assign work order
            selected_machine.erpnext_work_order_id = wo_name
            selected_machine.work_order = wo_name
            selected_machine.pipe_size = pipe_size
            selected_machine.target_qty = qty
            selected_machine.produced_qty = produced
            selected_machine.status = "paused"
            selected_machine.is_locked = True

            # Metadata update
            meta = db.query(ERPNextMetadata).filter(
                ERPNextMetadata.work_order == wo_name
            ).first()

            if not meta:
                meta = ERPNextMetadata(
                    machine_id=selected_machine.id,
                    work_order=wo_name,
                    erp_status="Assigned",
                    last_synced=datetime.now()
                )
                db.add(meta)
            else:
                meta.machine_id = selected_machine.id
                meta.erp_status = "Assigned"
                meta.last_synced = datetime.now()

            db.commit()
            print(f"🟢 Assigned ERP WO {wo_name} → Machine {selected_machine.name}")

    except SQLAlchemyError as e:
        db.rollback()
        print("❌ DB error:", e)
    except Exception as e:
        db.rollback()
        print("❌ Auto-assign error:", e)
    finally:
        db.close()

# =====================================================
# ERPNext SYNC LOOP (PRODUCTION)
# =====================================================
async def erpnext_sync_loop(interval: int = 10):
    """
    Continuous ERPNext sync loop
    - Fetch work orders
    - Auto-assign to machines
    """
    print("🚀 ERPNext Production Sync Loop Started")

    while True:
        try:
            auto_assign_work_orders()
        except Exception as e:
            print("❌ ERP Sync Loop error:", e)

        await asyncio.sleep(interval)
