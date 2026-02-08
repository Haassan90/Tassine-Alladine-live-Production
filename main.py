# =====================================================
# main.py – Taco Group Live Production Dashboard
# FULLY UPDATED: ERPNext Integration + Auto-Assign + Alerts + WebSocket
# =====================================================

from fastapi import FastAPI, Depends, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from datetime import datetime, timezone
import asyncio
from pydantic import BaseModel

from database import engine, SessionLocal, init_db
from models import Machine, ProductionLog, ScheduledJob, ERPNextMetadata
from erpnext_sync import (
    update_work_order_status,
    get_work_orders,
    auto_assign_work_orders
)
from report import router as report_router  # Production Report Router

# =====================================================
# APP INIT
# =====================================================
app = FastAPI(title="Taco Group Live Production")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =====================================================
# INCLUDE PRODUCTION REPORT ROUTER
# =====================================================
app.include_router(report_router)

# =====================================================
# DB INIT
# =====================================================
init_db()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# =====================================================
# WEBSOCKET MANAGER
# =====================================================
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active_connections.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active_connections:
            self.active_connections.remove(ws)

    async def broadcast(self, data: dict):
        for ws in list(self.active_connections):
            try:
                await ws.send_json(data)
            except Exception:
                self.disconnect(ws)

manager = ConnectionManager()

# =====================================================
# DASHBOARD DATA WITH ETA & METADATA
# =====================================================
def get_dashboard_data(db: Session):
    response = []
    machines = db.query(Machine).all()
    metadata_map = {m.work_order: m for m in db.query(ERPNextMetadata).all()}
    locations = {}
    next_jobs = {}

    for m in machines:
        remaining_qty = (m.target_qty - m.produced_qty) if m.target_qty else 0
        remaining_time = remaining_qty * m.seconds_per_meter if m.seconds_per_meter else None
        progress_percent = (m.produced_qty / m.target_qty) * 100 if m.target_qty else 0
        erp_meta = metadata_map.get(m.work_order)

        # Determine next job per location
        if m.status in ["free", "stopped"] and m.work_order:
            if m.location not in next_jobs:
                next_jobs[m.location] = {
                    "machine_id": m.id,
                    "work_order": m.work_order,
                    "pipe_size": m.pipe_size,
                    "total_qty": m.target_qty,
                    "produced_qty": m.produced_qty,
                    "remaining_time": remaining_time
                }

        locations.setdefault(m.location, []).append({
            "id": m.id,
            "name": m.name,
            "status": m.status,
            "job": {
                "work_order": m.work_order,
                "size": m.pipe_size,
                "total_qty": m.target_qty,
                "completed_qty": m.produced_qty,
                "remaining_qty": remaining_qty,
                "remaining_time": remaining_time,
                "progress_percent": progress_percent,
                "erp_status": erp_meta.erp_status if erp_meta else None,
                "erp_comments": erp_meta.erp_comments if erp_meta else None
            } if m.work_order else None,
            "next_job": next_jobs.get(m.location)
        })

    for loc, machines_list in locations.items():
        response.append({"name": loc, "machines": machines_list})

    return response

# =====================================================
# HTTP API
# =====================================================
@app.get("/api/dashboard")
def dashboard(db: Session = Depends(get_db)):
    return {"locations": get_dashboard_data(db)}

@app.get("/api/job_queue")
def job_queue(db: Session = Depends(get_db)):
    work_orders = get_work_orders()
    queue = []
    for wo in work_orders:
        if wo.get("status") == "Completed":
            continue
        queue.append({
            "id": wo.get("name"),
            "pipe_size": wo.get("custom_pipe_size"),
            "qty": wo.get("qty"),
            "produced_qty": wo.get("produced_qty", 0),
            "location": wo.get("custom_location"),
            "machine_id": wo.get("custom_machine_id")
        })
    return {"queue": queue}

@app.get("/api/production_logs")
def production_logs(db: Session = Depends(get_db), limit: int = 50):
    logs = db.query(ProductionLog).order_by(ProductionLog.timestamp.desc()).limit(limit).all()
    return {"logs": [{
        "machine_id": l.machine_id,
        "work_order": l.work_order,
        "pipe_size": l.pipe_size,
        "produced_qty": l.produced_qty,
        "timestamp": l.timestamp.isoformat()
    } for l in logs]}

# =====================================================
# Pydantic Models for JSON POST
# =====================================================
class MachineAction(BaseModel):
    location: str
    machine_id: int

class MachineRename(MachineAction):
    new_name: str

# =====================================================
# MACHINE HELPERS
# =====================================================
def get_machine(db: Session, location: str, machine_id: int):
    return db.query(Machine).filter(Machine.id == machine_id, Machine.location == location).first()

# =====================================================
# MACHINE CONTROLS + ERP STATUS UPDATE
# =====================================================
async def update_machine_status(db: Session, m: Machine, new_status: str):
    m.status = new_status
    if new_status == "running":
        m.is_locked = True
        m.last_tick_time = datetime.now(timezone.utc)
        update_work_order_status(m.erpnext_work_order_id, "In Process")
    elif new_status == "completed":
        m.is_locked = False
        update_work_order_status(m.erpnext_work_order_id, "Completed")

    db.commit()
    await manager.broadcast({"locations": get_dashboard_data(db)})

@app.post("/api/machine/start")
async def start_machine(data: MachineAction, db: Session = Depends(get_db)):
    m = get_machine(db, data.location, data.machine_id)
    if not m or not m.work_order:
        return {"ok": False}
    await update_machine_status(db, m, "running")
    return {"ok": True}

@app.post("/api/machine/pause")
async def pause_machine(data: MachineAction, db: Session = Depends(get_db)):
    m = get_machine(db, data.location, data.machine_id)
    if not m:
        return {"ok": False}
    m.status = "paused"
    db.commit()
    await manager.broadcast({"locations": get_dashboard_data(db)})
    return {"ok": True}

@app.post("/api/machine/stop")
async def stop_machine(data: MachineAction, db: Session = Depends(get_db)):
    m = get_machine(db, data.location, data.machine_id)
    if not m:
        return {"ok": False}
    await update_machine_status(db, m, "stopped")
    return {"ok": True}

@app.post("/api/machine/rename")
async def rename_machine(data: MachineRename, db: Session = Depends(get_db)):
    m = get_machine(db, data.location, data.machine_id)
    if not m:
        return {"ok": False}
    m.name = data.new_name
    db.commit()
    await manager.broadcast({"locations": get_dashboard_data(db)})
    return {"ok": True}

# =====================================================
# AUTOMATIC METER COUNTER + ERP STATUS
# =====================================================
async def automatic_meter_counter():
    while True:
        db = SessionLocal()
        try:
            machines = db.query(Machine).filter(Machine.status == "running").all()
            now = datetime.now(timezone.utc)
            updated = False

            for m in machines:
                if not m.seconds_per_meter or not m.work_order:
                    continue
                if not m.last_tick_time:
                    m.last_tick_time = now
                    continue

                diff = (now - m.last_tick_time).total_seconds()
                if diff >= m.seconds_per_meter and m.produced_qty < m.target_qty:
                    m.produced_qty += 1
                    m.last_tick_time = now
                    updated = True

                    db.add(ProductionLog(
                        machine_id=m.id,
                        work_order=m.work_order,
                        pipe_size=m.pipe_size,
                        produced_qty=1,
                        timestamp=now
                    ))

                    meta = db.query(ERPNextMetadata).filter(ERPNextMetadata.work_order == m.work_order).first()
                    if meta:
                        meta.erp_status = "In Progress"
                        meta.last_synced = now

                    if m.produced_qty >= m.target_qty:
                        m.produced_qty = m.target_qty
                        await update_machine_status(db, m, "completed")
                        if meta:
                            meta.erp_status = "Completed"

            if updated:
                db.commit()
                await manager.broadcast({"locations": get_dashboard_data(db)})

        except Exception as e:
            print("AUTO METER ERROR:", e)
        finally:
            db.close()
        await asyncio.sleep(1)

# =====================================================
# PRODUCTION ALERTS
# =====================================================
alert_history = {}

async def production_alerts():
    while True:
        db = SessionLocal()
        try:
            machines = db.query(Machine).filter(Machine.target_qty > 0).all()
            for m in machines:
                if not m.work_order or m.status != "running":
                    continue
                percent = (m.produced_qty / m.target_qty) * 100
                last_level = alert_history.get(m.id, 0)
                alert_level = 0
                message = None

                if percent >= 100:
                    alert_level = 3
                    message = f"✅ Machine {m.name} COMPLETED"
                elif percent >= 90:
                    alert_level = 2
                    message = f"⚠ {m.name} CRITICAL {percent:.1f}%"
                elif percent >= 75:
                    alert_level = 1
                    message = f"⚠ {m.name} Warning {percent:.1f}%"

                if alert_level > 0 and alert_level != last_level:
                    alert_history[m.id] = alert_level
                    await manager.broadcast({"alert": message, "machine_id": m.id, "level": alert_level})
                elif percent < 75:
                    alert_history[m.id] = 0

        except Exception as e:
            print("ALERT LOOP ERROR:", e)
        finally:
            db.close()
        await asyncio.sleep(5)

# =====================================================
# ERPNext SYNC LOOP
# =====================================================
async def erpnext_sync_loop(interval: int = 10):
    print("🚀 ERPNext Sync Loop started")
    while True:
        try:
            auto_assign_work_orders()
        except Exception as e:
            print("❌ ERP Sync Loop error:", e)
        await asyncio.sleep(interval)

# =====================================================
# WEBSOCKET
# =====================================================
@app.websocket("/ws/dashboard")
async def ws_dashboard(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)

# =====================================================
# STARTUP TASKS
# =====================================================
@app.on_event("startup")
async def startup_event():
    db = SessionLocal()

    # Seed machines if none exist
    if db.query(Machine).count() == 0:
        locations = {"Modan": 1, "Baldeya": 100, "Al-Khraj": 200}
        for loc, start_id in locations.items():
            for i in range(12):
                db.add(Machine(
                    id=start_id + i,
                    location=loc,
                    name=f"Machine {i + 1}",
                    status="free",
                    target_qty=100,
                    produced_qty=0,
                    pipe_size="20",
                    seconds_per_meter=20
                ))
        db.commit()

    db.close()

    # Startup tasks
    asyncio.create_task(automatic_meter_counter())
    asyncio.create_task(production_alerts())
    asyncio.create_task(erpnext_sync_loop())
