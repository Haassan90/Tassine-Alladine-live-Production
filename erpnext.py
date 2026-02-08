# =====================================================
# erpnext_sync_safe.py
# Full Project-Ready Version – Taco Group HDPE
# =====================================================
import os
import requests
from typing import List, Dict
from database import SessionLocal
from models import Machine, ERPNextMetadata
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import asyncio
from dotenv import load_dotenv

# =====================================================
# LOAD ERP CREDENTIALS (Demo-Safe)
# =====================================================
load_dotenv()
ERP_URL = os.getenv("ERP_URL")
API_KEY = os.getenv("ERP_API_KEY")
API_SECRET = os.getenv("ERP_API_SECRET")
TIMEOUT = 10  # seconds

# =====================================================
# FETCH ACTIVE WORK ORDERS FROM ERPNext
# =====================================================
def get_work_orders() -> List[Dict]:
    """Fetch active Work Orders from ERPNext. Safe for background loop."""
    if not ERP_URL or not API_KEY or not API_SECRET:
        print("⚠ ERP credentials not configured")
        return []

    url = f"{ERP_URL}/api/resource/Work Order"
    headers = {
        "Authorization": f"token {API_KEY}:{API_SECRET}",
        "Accept": "application/json"
    }
    params = {
        "fields": (
            '["name","qty","produced_qty","status",'
            '"custom_machine_id","custom_pipe_size","custom_location"]'
        ),
        "filters": (
            '[["status","in",["In Process","Not Started"]]]'
        )
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
        response.raise_for_status()
        payload = response.json()

        if not isinstance(payload, dict):
            print("⚠ ERP response invalid format")
            return []

        return payload.get("data", []) or []

    except requests.exceptions.Timeout:
        print("⏱ ERP request timeout")
    except requests.exceptions.RequestException as e:
        print("❌ ERP request failed:", e)
    except Exception as e:
        print("❌ ERP unknown error:", e)

    return []

# =====================================================
# SMART AUTO-ASSIGN WORK ORDERS TO MACHINES
# =====================================================
def auto_assign_work_orders(work_orders: List[Dict]):
    """Assign free machines to pending ERPNext work orders based on location & pipe size."""
    db = SessionLocal()
    try:
        for wo in work_orders:
            wo_name = wo.get("name")
            location = wo.get("custom_location")
            pipe_size = wo.get("custom_pipe_size")
            qty = wo.get("qty", 0)
            produced = wo.get("produced_qty", 0)

            # Skip already assigned
            if wo.get("custom_machine_id"):
                continue

            # Free machines
            free_machines = db.query(Machine).filter(
                Machine.location == location,
                Machine.status.in_(["free", "paused", "stopped"])
            ).all()

            assigned = False
            for m in free_machines:
                if m.pipe_size == pipe_size or not m.work_order:
                    m.work_order = wo_name
                    m.pipe_size = pipe_size
                    m.erpnext_work_order_id = wo_name
                    m.target_qty = qty
                    m.produced_qty = produced
                    m.status = "paused"

                    # Update metadata
                    meta = db.query(ERPNextMetadata).filter(ERPNextMetadata.work_order == wo_name).first()
                    if not meta:
                        meta = ERPNextMetadata(machine_id=m.id, work_order=wo_name, erp_status="Assigned")
                        db.add(meta)
                    else:
                        meta.machine_id = m.id
                        meta.erp_status = "Assigned"

                    db.commit()
                    print(f"🟢 Assigned WO {wo_name} → Machine {m.name} ({location})")
                    assigned = True
                    break

            # Fallback: first free machine
            if not assigned and free_machines:
                m = free_machines[0]
                m.work_order = wo_name
                m.pipe_size = pipe_size
                m.erpnext_work_order_id = wo_name
                m.target_qty = qty
                m.produced_qty = produced
                m.status = "paused"

                meta = db.query(ERPNextMetadata).filter(ERPNextMetadata.work_order == wo_name).first()
                if not meta:
                    meta = ERPNextMetadata(machine_id=m.id, work_order=wo_name, erp_status="Assigned")
                    db.add(meta)
                else:
                    meta.machine_id = m.id
                    meta.erp_status = "Assigned"

                db.commit()
                print(f"🟢 Assigned WO {wo_name} → Machine {m.name} ({location}) [fallback]")

    except SQLAlchemyError as e:
        db.rollback()
        print("❌ DB error during auto-assign:", e)
    finally:
        db.close()

# =====================================================
# ERPNext SYNC LOOP (ASYNC, BACKGROUND)
# =====================================================
async def erpnext_sync_loop(interval: int = 10):
    """
    Background loop:
    1. Fetch ERPNext Work Orders
    2. Auto-assign free machines
    """
    print("🚀 ERPNext Sync Loop started (Demo-Safe)")

    while True:
        try:
            if not ERP_URL or not API_KEY or not API_SECRET:
                print("⚠ ERP credentials missing, skipping iteration")
                await asyncio.sleep(interval)
                continue

            work_orders = get_work_orders()
            if work_orders:
                print(f"📝 {len(work_orders)} Work Orders fetched")
                auto_assign_work_orders(work_orders)
            else:
                print("ℹ️ No pending Work Orders to assign")

        except Exception as e:
            print("❌ ERP Sync Loop error:", e)

        await asyncio.sleep(interval)