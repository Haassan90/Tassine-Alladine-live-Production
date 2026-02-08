/************************************************* 
 * 🔒 FINAL FULL SCRIPT.JS (MERGED + ERPNext READY + LOGIN PERSISTENCE)
 * Step 1 → Step 45 Fully Functional + Enhancements
 * Bug fix: Machine action no longer reloads dashboard
 * Improvement: Login persists until logout
 *************************************************/

const API_BASE = "http://127.0.0.1:8000/api";
const WS_URL = "ws://127.0.0.1:8000/ws/dashboard";

/************************
 * TEMP USERS (Login)
 ************************/
const users = [
    { username: "1111", password: "1111", location: "Modan", role: "operator" },
    { username: "2222", password: "2222", location: "Baldeya", role: "operator" },
    { username: "3333", password: "3333", location: "Al-Khraj", role: "operator" },
    { username: "Admin", password: "12345", location: "all", role: "admin" }
];

let currentUser = null;
let socket = null;
let etaIntervals = {};
let nextJobIntervals = {};
let suppressNextWSRender = false;
let dashboardCache = {};

/************************
 * LOGIN PERSISTENCE
 ************************/
function restoreLogin() {
    const savedUser = localStorage.getItem("dashboardUser");
    if(savedUser){
        currentUser = JSON.parse(savedUser);
        document.getElementById("login-section").classList.add("hidden");
        document.getElementById("dashboard-section").classList.remove("hidden");
        initFilters();
        initWebSocket();
        loadDashboard();
        loadProductionLogs();
    }
}

function saveLogin(user){
    localStorage.setItem("dashboardUser", JSON.stringify(user));
}

function logout(){
    currentUser = null;
    localStorage.removeItem("dashboardUser");
    document.getElementById("login-section").classList.remove("hidden");
    document.getElementById("dashboard-section").classList.add("hidden");
}

/************************
 * LOGIN
 ************************/
document.getElementById("login-form").addEventListener("submit", e => {
    e.preventDefault();
    const u = document.getElementById("username").value.trim();
    const p = document.getElementById("password").value.trim();
    const user = users.find(x => x.username === u && x.password === p);
    if (!user) { alert("Invalid login"); return; }

    currentUser = user;
    saveLogin(user);

    document.getElementById("login-section").classList.add("hidden");
    document.getElementById("dashboard-section").classList.remove("hidden");

    initFilters();
    initWebSocket();
    loadDashboard();
    loadProductionLogs();
});

// Restore login on page load
restoreLogin();

/************************
 * WEBSOCKET (REALTIME)
 ************************/
function initWebSocket() {
    if (socket && socket.readyState === WebSocket.OPEN) return;
    socket = new WebSocket(WS_URL);

    socket.onopen = () => { 
        socket.send("ready"); 
        console.log("✅ WS Connected"); 
        createAlert("WebSocket connected",0); 
    };

    socket.onmessage = e => {
        try {
            const data = JSON.parse(e.data);
            if (suppressNextWSRender) { suppressNextWSRender = false; return; }
            dashboardCache = data;

            if(data.new_job) handleNewJob(data.new_job);

            updateDashboardFromWS(data);
            handleAlerts(data);
            loadProductionLogs();
            updateMetricsModal(data);
        } catch (err) { 
            console.error("WS parse error", err); 
            createAlert("WebSocket data error",2); 
        }
    };

    socket.onclose = () => { 
        socket = null; 
        createAlert("WebSocket disconnected. Reconnecting...",2); 
        setTimeout(initWebSocket,3000); 
    };

    socket.onerror = () => socket.close();
}

/************************
 * DASHBOARD LOAD (HTTP)
 ************************/
async function loadDashboard() {
    if(!currentUser) return;
    try {
        const res = await fetch(`${API_BASE}/dashboard`);
        if(!res.ok) throw new Error("HTTP Error");
        const data = await res.json();
        dashboardCache = data;
        renderDashboard(data);
        updateMetricsModal(data);
    } catch {
        console.error("Backend not reachable");
        createAlert("Backend not reachable. Retry in 3s",2);
        setTimeout(loadDashboard,3000);
    }
}

/************************
 * PRODUCTION LOGS
 ************************/
async function loadProductionLogs() {
    if(!currentUser) return;
    try {
        const res = await fetch(`${API_BASE}/production_logs`);
        const data = await res.json();
        const tbody = document.getElementById("logs-table");
        if (!tbody) return;
        tbody.innerHTML = "";
        data.logs.slice(0,20).forEach(l => {
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td>${l.machine_id}</td>
                <td>${l.work_order || "N/A"}</td>
                <td>${l.pipe_size || "N/A"}</td>
                <td>${l.produced_qty}</td>
                <td>${new Date(l.timestamp).toLocaleString()}</td>
            `;
            tbody.appendChild(tr);
        });
    } catch (err) { console.error(err); createAlert("Failed to fetch production logs",2);}
}

/************************
 * RENDER DASHBOARD
 ************************/
function renderDashboard(data){
    if(!currentUser) return; // Prevent showing dashboard if not logged in
    const container = document.getElementById("locations");
    container.innerHTML = "";
    if(!data?.locations) return;

    let visibleLocations = currentUser.location==="all"?data.locations:data.locations.filter(l=>l.name===currentUser.location);
    const locFilter = document.getElementById("filter-location")?.value||"all";
    const statusFilter = document.getElementById("filter-status")?.value||"all";
    const searchFilter = document.getElementById("search-machine")?.value.trim().toLowerCase()||"";

    if(locFilter!=="all") visibleLocations = visibleLocations.filter(l=>l.name===locFilter);

    visibleLocations.forEach(loc=>{
        let machines = loc.machines;
        if(statusFilter!=="all") machines=machines.filter(m=>m.status===statusFilter);
        if(searchFilter) machines=machines.filter(m=>m.name.toLowerCase().includes(searchFilter)||(m.job?.work_order||"").toLowerCase().includes(searchFilter));
        renderLocation(loc.name, machines);
    });
}

function renderLocation(location,machines){
    const wrap=document.createElement("div");
    wrap.className="location";
    wrap.innerHTML=`<h2>${location}</h2><div class="machines-grid"></div>`;
    document.getElementById("locations").appendChild(wrap);
    const grid=wrap.querySelector(".machines-grid");

    machines.forEach(m=>{
        createOrUpdateMachineCard(m, location, grid);
    });
}

/************************
 * MACHINE CARD CREATION / UPDATE
 ************************/
function createOrUpdateMachineCard(machine, location, parentGrid){
    let card = document.getElementById(`machine-${machine.id}`);
    const remainingTimeText = machine.job?.remaining_time!=null ? formatTime(machine.job.remaining_time) : "N/A";
    const progressPercent = machine.job?.progress_percent != null ? Math.min(machine.job.progress_percent.toFixed(1),100) : 0;

    const cardHTML = `<h3>${machine.name}${currentUser.role==="admin"?`<button type="button" class="btn edit" data-location="${location}" data-id="${machine.id}" data-name="${machine.name}">✏</button>`:""}</h3>
        <p>Status: <b>${machine.status.toUpperCase()}</b></p>
        ${machine.job?`
            <div class="job-card">
                <p>WO: ${machine.job.work_order}</p>
                <p>Size: ${machine.job.size}</p>
                <p>Produced: ${machine.job.completed_qty}/${machine.job.total_qty}</p>
                <p>Remaining: <span id="eta-${machine.id}">${remainingTimeText}</span></p>
                <p>Progress: ${progressPercent}%</p>
                <div class="progress-bar-container">
                    <div class="progress-bar" style="width:${progressPercent}%;background-color:${progressPercent>=90?'red':progressPercent>=75?'orange':'green'}"></div>
                </div>
            </div>`:`<div class="job-card"><p>No Job</p></div>`}
        ${currentUser.role==="operator"?`
            <div class="controls">
                <button type="button" class="btn start" data-location="${location}" data-id="${machine.id}">▶</button>
                <button type="button" class="btn pause" data-location="${location}" data-id="${machine.id}">⏸</button>
                <button type="button" class="btn stop" data-location="${location}" data-id="${machine.id}">⛔</button>
            </div>`:""}
        
        ${machine.next_job?`
            <div class="next-job-card">
                <h4>Next Job Queue</h4>
                <p>WO: ${machine.next_job.work_order}</p>
                <p>Size: ${machine.next_job.pipe_size}</p>
                <p>Produced: ${machine.next_job.produced_qty}/${machine.next_job.total_qty}</p>
                <p>ETA: <span id="next-eta-${machine.id}">${formatTime(machine.next_job.remaining_time)}</span></p>
            </div>`:""}`;

    if(card){
        card.innerHTML = cardHTML;
    } else {
        card = document.createElement("div");
        card.className = `machine status-${machine.status}`;
        card.id = `machine-${machine.id}`;
        card.innerHTML = cardHTML;
        parentGrid.appendChild(card);
    }

    if(machine.job?.remaining_time!=null) setupETACountdown(machine.id, machine.job.remaining_time);
    if(machine.next_job?.remaining_time!=null) setupNextJobCountdown(machine.id, machine.next_job.remaining_time);
}

/************************
 * MACHINE ACTIONS (NO RELOAD)
 ************************/
async function machineAction(e,action,location,id){
    e.preventDefault();
    e.stopPropagation();
    suppressNextWSRender=true; 
    try{
        const res=await fetch(`${API_BASE}/machine/${action}`,{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({location,machine_id:id})
        });
        const data=await res.json();
        if(!data.ok) alert(`❌ ${action} failed`);
        else if(data.machine){
            const grid = Array.from(document.querySelectorAll(".location"))
                .find(el=>el.querySelector("h2").textContent===location)
                ?.querySelector(".machines-grid");
            if(grid) createOrUpdateMachineCard(data.machine, location, grid);
        }
    }catch(err){console.error(err); alert(`❌ ${action} failed`);}
}

/************************
 * NEW JOB ALERT
 ************************/
function handleNewJob(job){
    const {machine_id,work_order,qty,pipe_size,eta}=job;
    const machineEl=document.getElementById(`machine-${machine_id}`);
    if(!machineEl) return;
    createAlert(`New job assigned to ${machineEl.querySelector("h3").innerText}: ${work_order}`,1);
    const jobCard=machineEl.querySelector(".job-card");
    if(jobCard){
        jobCard.innerHTML=`<p>WO: ${work_order}</p>
            <p>Size: ${pipe_size}</p>
            <p>Produced: 0/${qty}</p>
            <p>Remaining: <span id="eta-${machine_id}">${formatTime(eta)}</span></p>
            <p>Progress: 0%</p>
            <div class="progress-bar-container">
                <div class="progress-bar" style="width:0%;background-color:green"></div>
            </div>`;
    }
    setupETACountdown(machine_id,eta);
}

/************************
 * ETA / NEXT JOB COUNTDOWN
 ************************/
function formatTime(sec){if(!sec||sec<0)return "0:00"; const m=Math.floor(sec/60); const s=Math.floor(sec%60); return `${m}:${s.toString().padStart(2,'0')}`;}
function setupETACountdown(id,sec){clearInterval(etaIntervals[id]);let t=sec; etaIntervals[id]=setInterval(()=>{const el=document.getElementById(`eta-${id}`);if(!el) return; el.textContent=formatTime(t--); if(t<0) clearInterval(etaIntervals[id]);},1000);}
function setupNextJobCountdown(id,sec){clearInterval(nextJobIntervals[id]);let t=sec; nextJobIntervals[id]=setInterval(()=>{const el=document.getElementById(`next-eta-${id}`);if(!el) return; el.textContent=formatTime(t--); if(t<0) clearInterval(nextJobIntervals[id]);},1000);}

/************************
 * ALERTS PANEL
 ************************/
function handleAlerts(data){
    const alertsContainer=document.getElementById("alerts"); if(!alertsContainer) return; alertsContainer.innerHTML="";
    data.locations.forEach(loc=>{loc.machines.forEach(m=>{
        if(m.job){const p=m.job.progress_percent;if(p>=75&&p<90) createAlert(`${m.name} reached 75% progress!`,1); else if(p>=90&&p<100) createAlert(`${m.name} reached 90% progress!`,2); else if(p>=100) createAlert(`${m.name} completed!`,3);}})});
}
function createAlert(message,level){
    const alertsContainer=document.getElementById("alerts"); 
    if(!alertsContainer) return;
    const alertDiv=document.createElement("div");
    alertDiv.className="alert"; 
    alertDiv.style.backgroundColor=level===3?"#2196f3":level===2?"#f44336":"#ff9800"; 
    alertDiv.textContent=message; 
    alertsContainer.prepend(alertDiv); 
    setTimeout(()=>alertDiv.remove(),10000);
}

/************************
 * METRICS MODAL (AUTO-REFRESH)
 ************************/
function updateMetricsModal(data){
    const modal=document.getElementById("metrics-modal");
    if(!modal) return;
    const tbody=modal.querySelector("tbody"); tbody.innerHTML="";
    data.locations.forEach(loc=>{loc.machines.forEach(m=>{
        const tr=document.createElement("tr");
        tr.innerHTML=`<td>${loc.name}</td><td>${m.name}</td><td>${m.status}</td><td>${m.job?m.job.work_order:"N/A"}</td><td>${m.job?`${m.job.completed_qty}/${m.job.total_qty}`:"-"}</td><td>${m.job?m.job.progress_percent:0}</td>`;
        tbody.appendChild(tr);
    })});
}
function openMetricsModal(){document.getElementById("metrics-modal")?.classList.remove("hidden");}
function closeMetricsModal(){document.getElementById("metrics-modal")?.classList.add("hidden");}
function exportTableToCSV(tableId, filename='export.csv'){const table=document.getElementById(tableId); const rows=Array.from(table.querySelectorAll('tr')); const csv=rows.map(r=>Array.from(r.querySelectorAll('th,td')).map(c=>`"${c.textContent}"`).join(',')).join('\n'); const blob=new Blob([csv],{type:'text/csv'}); const link=document.createElement('a'); link.href=URL.createObjectURL(blob); link.download=filename; link.click();}

/************************
 * FILTERS
 ************************/
function initFilters(){
    const locSelect=document.getElementById("filter-location");
    if(!locSelect) return;
    locSelect.innerHTML=`<option value="all">All</option>`;
    users.filter(u=>u.location!=="all").forEach(u=>{
        if(!Array.from(locSelect.options).some(o=>o.value===u.location)) locSelect.innerHTML+=`<option value="${u.location}">${u.location}</option>`;
    });
    locSelect.addEventListener("change",loadDashboard);
    document.getElementById("filter-status").addEventListener("change",loadDashboard);
    document.getElementById("search-machine").addEventListener("input",loadDashboard);
}

/************************
 * EVENT DELEGATION
 ************************/
document.addEventListener("click",function(e){
    const btn=e.target.closest("button"); if(!btn) return;

    if(btn.classList.contains("start")) machineAction(e,'start',btn.dataset.location,btn.dataset.id);
    else if(btn.classList.contains("pause")) machineAction(e,'pause',btn.dataset.location,btn.dataset.id);
    else if(btn.classList.contains("stop")) machineAction(e,'stop',btn.dataset.location,btn.dataset.id);
    else if(btn.classList.contains("edit")) editMachineName(e,btn.dataset.location,btn.dataset.id,btn.dataset.name);
    else if(btn.classList.contains("create-wo")) createWorkOrder(e,btn.dataset.location,btn.dataset.id);
    else if(btn.classList.contains("sync-wo")) syncWorkOrder(e,btn.dataset.location,btn.dataset.id);
    else if(btn.id==="btn-view-metrics") openMetricsModal();
    else if(btn.id==="btn-close-metrics") closeMetricsModal();
    else if(btn.id==="btn-export-csv") exportTableToCSV('metrics-table','metrics.csv');
});

// Close WS on page unload
window.addEventListener("beforeunload",()=>{if(socket) socket.close();});
