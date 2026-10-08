"""

Synchronization primitives
reader_mutex  (Lock)         : protects read_count
rw_lock       (Semaphore(1)) : protects the shared resource
service_queue (Semaphore(1)) : turnstile - once a writer holds it, no NEW
                               reader can start, so writers cannot starve
event_lock    (Lock)         : protects ONLY the logging/visualisation state
                               (not part of the synchronization algorithm)
"""

import threading
import time
import random
import json
import webbrowser
from pathlib import Path


# CONFIGURATION

NUM_READERS = 5
NUM_WRITERS = 2
ROUNDS = 3

# SHARED RESOURCE (critical section data)

shared_data = 0
writer_updates = 0


# SYNCHRONIZATION PRIMITIVES

reader_mutex = threading.Lock()          # protects read_count
rw_lock = threading.Semaphore(1)         # protects shared resource
service_queue = threading.Semaphore(1)   # turnstile (fair admission)

read_count = 0


# VISUALISATION STATE

reader_states = {f"R{i}": "Not Started" for i in range(NUM_READERS)}
writer_states = {f"W{i}": "Not Started" for i in range(NUM_WRITERS)}

waiting_readers = []
waiting_writers = []
active_readers = []
active_writer = None

# Statistics used by the real verification checks
reader_entries = 0       # total times any reader entered the critical section
writer_wait_times = []   # seconds each writer waited (request -> entry)
writer_overtakes = []    # readers that entered after a writer held the turnstile

events = []
event_lock = threading.Lock()
start_time = time.time()


def add_event_locked(message, event_type):
    """Store a snapshot of the actual state. event_lock MUST be held."""
    events.append({
        "t": round(time.time() - start_time, 3),
        "message": message,
        "event_type": event_type,
        "read_count": read_count,
        "shared_data": shared_data,
        "active_readers": list(active_readers),
        "active_writer": active_writer,
        "waiting_readers": list(waiting_readers),
        "waiting_writers": list(waiting_writers),
        "reader_states": dict(reader_states),
        "writer_states": dict(writer_states),
    })


def record_event(message, event_type="info"):
    """Record an event (acquires event_lock itself)."""
    with event_lock:
        add_event_locked(message, event_type)

# READER THREAD

def reader(reader_id):
    global read_count, reader_entries

    name = f"R{reader_id}"

    for round_no in range(1, ROUNDS + 1):

        # --- waiting / thinking ---
        with event_lock:
            reader_states[name] = "Waiting"
            if name not in waiting_readers:
                waiting_readers.append(name)
            add_event_locked(f"Reader {name} is waiting (round {round_no}).",
                             "waiting")

        time.sleep(random.uniform(0.05, 0.15))
        record_event(f"Reader {name} requests READ access.", "request")

        # --- entry protocol ---
        service_queue.acquire()
        reader_mutex.acquire()

        with event_lock:
            read_count += 1
            is_first = (read_count == 1)
            add_event_locked(
                f"Reader {name} acquired reader_mutex. "
                f"read_count = {read_count}.", "reader_mutex")

        if is_first:
            record_event(f"Reader {name} is the first reader and requests "
                         f"rw_lock.", "resource_request")
            rw_lock.acquire()   # may block while a writer is writing
            record_event(f"Reader {name} acquired rw_lock for shared reading.",
                         "resource_acquired")

        reader_mutex.release()
        service_queue.release()

        # --- critical section (shared reading) ---
        # Reader leaves the waiting queue only NOW, when it really enters.
        with event_lock:
            if name in waiting_readers:
                waiting_readers.remove(name)
            if name not in active_readers:
                active_readers.append(name)
            reader_states[name] = "Reading"
            reader_entries += 1
            add_event_locked(
                f"Reader {name} ENTERED the critical section and reads "
                f"shared data = {shared_data}.", "reader_enter")

        time.sleep(random.uniform(0.15, 0.30))
        record_event(f"Reader {name} is reading shared data = {shared_data}.",
                     "reading")
        time.sleep(random.uniform(0.05, 0.15))

        # --- exit protocol ---
        reader_mutex.acquire()

        with event_lock:
            if name in active_readers:
                active_readers.remove(name)
            reader_states[name] = "Releasing"
            read_count -= 1
            is_last = (read_count == 0)
            add_event_locked(
                f"Reader {name} EXITED the critical section. "
                f"read_count = {read_count}.", "reader_exit")

        if is_last:
            # Record BEFORE releasing so the log order matches reality.
            record_event(f"Reader {name} is the last reader and releases "
                         f"rw_lock.", "resource_released")
            rw_lock.release()

        reader_mutex.release()

        # --- next round / finished ---
        with event_lock:
            if round_no < ROUNDS:
                reader_states[name] = "Idle"
                add_event_locked(f"Reader {name} finished round {round_no}.",
                                 "round_done")
            else:
                reader_states[name] = "Finished"
                add_event_locked(f"Reader {name} FINISHED all {ROUNDS} rounds.",
                                 "finished")

        time.sleep(random.uniform(0.05, 0.15))


# WRITER THREAD

def writer(writer_id):
    global shared_data, writer_updates, active_writer

    name = f"W{writer_id}"

    for round_no in range(1, ROUNDS + 1):

        # --- waiting / thinking ---
        with event_lock:
            writer_states[name] = "Waiting"
            if name not in waiting_writers:
                waiting_writers.append(name)
            add_event_locked(f"Writer {name} is waiting (round {round_no}).",
                             "waiting")

        time.sleep(random.uniform(0.05, 0.15))
        request_time = time.time()
        record_event(f"Writer {name} requests WRITE access.", "request")

        # --- entry protocol ---
        service_queue.acquire()

        with event_lock:
            entries_at_turnstile = reader_entries
            add_event_locked(f"Writer {name} holds the service_queue "
                             f"(no new readers can start).", "service_queue")

        record_event(f"Writer {name} is waiting for exclusive rw_lock access.",
                     "resource_request")

        # Blocks WITHOUT holding event_lock, so the HTML state stays valid.
        rw_lock.acquire()
        service_queue.release()

        # --- critical section (exclusive writing) ---
        # Writer leaves the waiting queue only NOW, when it really enters.
        with event_lock:
            if name in waiting_writers:
                waiting_writers.remove(name)
            active_writer = name
            writer_states[name] = "Writing"
            writer_wait_times.append(time.time() - request_time)
            writer_overtakes.append(reader_entries - entries_at_turnstile)
            add_event_locked(f"Writer {name} ENTERED the critical section "
                             f"with exclusive access.", "writer_enter")

        time.sleep(random.uniform(0.15, 0.30))

        with event_lock:
            shared_data += 1
            writer_updates += 1
            add_event_locked(f"Writer {name} UPDATED shared data to "
                             f"{shared_data}.", "writing")

        time.sleep(random.uniform(0.05, 0.15))

        # --- exit protocol ---
        with event_lock:
            active_writer = None
            writer_states[name] = "Releasing"
            add_event_locked(f"Writer {name} EXITED the critical section.",
                             "writer_exit")

        # Record BEFORE releasing so the log order matches reality.
        record_event(f"Writer {name} releases rw_lock.", "resource_released")
        rw_lock.release()

        # --- next round / finished ---
        with event_lock:
            if round_no < ROUNDS:
                writer_states[name] = "Idle"
                add_event_locked(f"Writer {name} finished round {round_no}.",
                                 "round_done")
            else:
                writer_states[name] = "Finished"
                add_event_locked(f"Writer {name} FINISHED all {ROUNDS} rounds.",
                                 "finished")

        time.sleep(random.uniform(0.05, 0.15))


# VERIFICATION (computed from the recorded execution, not hard-coded)

def check_invariants():
    """Return a dict of check_name -> (passed, detail) from real event data."""
    results = {}

    # 1. Mutual exclusion: a writer is never active together with a reader
    violations = [i for i, e in enumerate(events)
                  if e["active_writer"] and e["active_readers"]]
    results["Writer exclusive access (no reader/writer overlap)"] = (
        not violations,
        "no violations in any recorded state" if not violations
        else f"violated at steps {violations[:5]}")

    # 2. Readers really ran concurrently
    max_readers = max((len(e["active_readers"]) for e in events), default=0)
    results["Multiple readers allowed concurrently"] = (
        max_readers > 1, f"max simultaneous readers = {max_readers}")

    # 3. read_count never negative / never above number of readers
    bad_count = [e["read_count"] for e in events
                 if not 0 <= e["read_count"] <= NUM_READERS]
    results["read_count protected by reader_mutex"] = (
        not bad_count and read_count == 0,
        f"final read_count = {read_count}")

    # 4. Starvation: every writer finished, and overtaking is bounded
    max_wait = max(writer_wait_times, default=0)
    max_over = max(writer_overtakes, default=0)
    results["Writer starvation prevented"] = (
        max_over <= NUM_READERS and
        all(s == "Finished" for s in writer_states.values()),
        f"max writer wait = {max_wait:.2f}s, max readers that entered after "
        f"a writer held the turnstile = {max_over} (limit {NUM_READERS})")

    return results


def final_verification():
    print("\n" + "=" * 65)
    print("FINAL SYNCHRONIZATION VERIFICATION")
    print("=" * 65)

    print("\nREADERS")
    for n, s in reader_states.items():
        print(f"{n}: {s}")
    print("\nWRITERS")
    for n, s in writer_states.items():
        print(f"{n}: {s}")

    expected = NUM_WRITERS * ROUNDS
    print("\nSHARED RESOURCE")
    print(f"Final shared data value: {shared_data}")
    print(f"Expected writer updates: {expected}")
    print(f"Actual writer updates:   {writer_updates}")

    all_passed = True

    basic = {
        "All readers completed": all(s == "Finished" for s in reader_states.values()),
        "All writers completed": all(s == "Finished" for s in writer_states.values()),
        "Writer update count correct": writer_updates == expected and shared_data == expected,
        "No active/waiting processes left": not (active_readers or active_writer
                                                  or waiting_readers or waiting_writers),
    }

    print("\nCHECKS")
    for name, ok in basic.items():
        print(f"{name}: {'PASSED' if ok else 'FAILED'}")
        all_passed &= ok

    for name, (ok, detail) in check_invariants().items():
        print(f"{name}: {'PASSED' if ok else 'FAILED'}  ({detail})")
        all_passed &= ok

    print("\n" + ("ALL FINAL CHECKS PASSED." if all_passed else "FINAL CHECK FAILED."))
    print("=" * 65)
    return all_passed



# HTML GENERATION

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Readers-Writers Semaphore Simulation</title>
<style>
*{box-sizing:border-box}
body{margin:0;font-family:Arial,Helvetica,sans-serif;background:#f3f1ea;color:#292929}
.header{background:#252525;color:#fff;padding:28px;border-bottom:5px solid #d99b32}
.header-inner,.container{width:94%;max-width:1450px;margin:0 auto}
.header h1{margin:0;font-size:30px}
.header p{margin:8px 0 0;color:#d7d2c8}
.container{padding:25px 0 40px}
.summary{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:20px}
.summary-card{background:#fffdf8;border:1px solid #ded8ca;border-radius:12px;padding:18px;text-align:center}
.summary-label{font-size:12px;color:#777064;text-transform:uppercase;letter-spacing:.7px}
.summary-value{margin-top:7px;font-size:27px;font-weight:bold}
.main-grid{display:grid;grid-template-columns:1fr 1.35fr 1fr;gap:18px;align-items:stretch}
.panel{background:#fffdf8;border:1px solid #ded8ca;border-radius:14px;padding:18px}
.panel-heading{display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #e2ddd2;padding-bottom:12px;margin-bottom:14px}
.panel-heading h2{margin:0;font-size:20px}
.panel-tag{font-size:11px;font-weight:bold;padding:6px 9px;border-radius:6px;background:#eee9dc}
.reader-tag{background:#dcefe5;color:#276247}
.writer-tag{background:#f5ddd7;color:#944331}
.actor{border:1px solid #ddd7cb;border-left:5px solid #d8d0c1;border-radius:9px;padding:11px;margin-bottom:9px;background:#faf8f2;transition:.2s}
.actor.reading{border-left-color:#4b8b68;background:#edf7f0}
.actor.writing{border-left-color:#c65343;background:#fbece8}
.actor.waiting{border-left-color:#d49c2e;background:#fff8e8}
.actor.finished{border-left-color:#48729b;background:#edf4fa}
.actor-top{display:flex;justify-content:space-between;align-items:center}
.actor-name{font-weight:bold;font-size:15px}
.actor-description{font-size:11px;color:#777064;margin-top:5px}
.badge{font-size:10px;font-weight:bold;padding:5px 8px;border-radius:12px;background:#e7e3da}
.badge-reading{color:#276247;background:#dcefe5}
.badge-writing{color:#944331;background:#f5ddd7}
.badge-waiting{color:#9a6a09;background:#fff0c5}
.badge-finished{color:#315c80;background:#dcebf7}
.resource-wrapper{display:flex;flex-direction:column;align-items:center}
.resource{width:100%;min-height:290px;border:3px solid #aaa397;border-radius:14px;background:#292929;color:#fff;display:flex;flex-direction:column;justify-content:center;align-items:center;text-align:center;transition:.25s}
.resource.reading{border-color:#4b8b68}
.resource.writing{border-color:#c65343}
.resource-icon{font-size:42px;margin-bottom:10px}
.resource-title{font-size:21px;font-weight:bold}
.resource-subtitle{margin-top:5px;color:#d4d0c7;font-size:12px}
.resource-value{font-size:38px;font-weight:bold;color:#e5b74e;margin:13px 0}
.resource-status{padding:7px 13px;border-radius:6px;background:#5a554d;font-size:11px;font-weight:bold}
.resource.reading .resource-status{background:#315c43}
.resource.writing .resource-status{background:#7d3e34}
.resource-free-text{margin-top:13px;color:#d4d0c7;font-size:11px}
.sync-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:12px;width:100%}
.sync-box,.info-box,.queue{border:1px solid #d9d3c8;border-radius:8px;padding:10px 6px;text-align:center;background:#faf8f2}
.sync-name{font-family:Consolas,monospace;font-size:11px;font-weight:bold}
.sync-description{font-size:9px;color:#777064;margin-top:4px}
.sync-status{margin-top:5px;font-size:10px;font-weight:bold}
.resource-info{margin-top:12px;display:grid;grid-template-columns:1fr 1fr;gap:8px;width:100%}
.info-box{padding:9px;text-align:left}
.info-label{font-size:9px;text-transform:uppercase;color:#777064}
.info-value{margin-top:4px;font-size:12px;font-weight:bold}
.queue-section{margin-top:14px;display:grid;grid-template-columns:1fr 1fr;gap:10px}
.queue{padding:11px;text-align:left}
.queue-title{font-size:12px;font-weight:bold;margin-bottom:8px}
.queue-items{min-height:27px;display:flex;flex-wrap:wrap;gap:5px}
.queue-item{background:#e9e4d9;border-radius:5px;padding:5px 7px;font-size:10px;font-weight:bold}
.queue-empty{color:#999;font-size:11px}
.event-panel{margin-top:18px}
.event-top{display:flex;justify-content:space-between;align-items:center}
.event-top h2{margin:0}
.event-step{font-size:11px;color:#777064}
.event-card{margin-top:12px;border-left:5px solid #d99b32;background:#f8f3e8;border-radius:8px;padding:17px}
.event-type{font-size:11px;font-weight:bold;color:#9a6a09;text-transform:uppercase}
.event-message{margin-top:7px;font-size:17px;font-weight:bold}
.controls{display:flex;justify-content:center;align-items:center;gap:9px;margin-top:15px;flex-wrap:wrap}
button{border:none;border-radius:7px;padding:10px 15px;background:#353535;color:#fff;font-weight:bold;cursor:pointer}
button:hover{background:#1f1f1f}
.speed{display:flex;align-items:center;gap:7px;font-size:12px}
input[type=range]{width:120px}
.log-panel{margin-top:18px}
.log{height:270px;overflow-y:auto;background:#242424;color:#e7e2d8;border-radius:9px;padding:14px;font-family:Consolas,monospace;font-size:12px}
.log-entry{padding:6px 0;border-bottom:1px solid #3a3a3a}
.log-number{color:#e5b74e;font-weight:bold}
.footer{text-align:center;padding:25px;color:#777064;font-size:12px}
@media(max-width:1050px){.main-grid{grid-template-columns:1fr}.summary{grid-template-columns:repeat(2,1fr)}}
@media(max-width:600px){.summary,.queue-section,.sync-grid{grid-template-columns:1fr}}
</style>
</head>
<body>

<div class="header"><div class="header-inner">
  <h1>Readers–Writers Synchronization</h1>
  <p>Semaphore Solution • Actual Execution Visualization</p>
</div></div>

<div class="container">

<div class="summary">
  <div class="summary-card"><div class="summary-label">Readers</div><div class="summary-value">__NUM_READERS__</div></div>
  <div class="summary-card"><div class="summary-label">Writers</div><div class="summary-value">__NUM_WRITERS__</div></div>
  <div class="summary-card"><div class="summary-label">Current Read Count</div><div class="summary-value" id="readCount">0</div></div>
  <div class="summary-card"><div class="summary-label">Execution Step</div><div class="summary-value" id="eventCounter">0 / __TOTAL__</div></div>
</div>

<div class="main-grid">

  <div class="panel">
    <div class="panel-heading"><h2>Readers</h2><span class="panel-tag reader-tag">SHARED ACCESS</span></div>
    <div id="readerList"></div>
  </div>

  <div class="panel">
    <div class="panel-heading"><h2>Shared Resource</h2><span class="panel-tag">PROTECTED BY rw_lock</span></div>
    <div class="resource-wrapper">
      <div class="resource" id="resource">
        <div class="resource-icon" id="resourceIcon"></div>
        <div class="resource-title">SHARED DATA</div>
        <div class="resource-subtitle">Common resource accessed by readers and writers</div>
        <div class="resource-value" id="sharedData">0</div>
        <div class="resource-status" id="resourceStatus">RESOURCE AVAILABLE</div>
        <div class="resource-free-text" id="resourceActor">No active reader or writer</div>
      </div>

      <div class="sync-grid">
        <div class="sync-box"><div class="sync-name">reader_mutex</div><div class="sync-description">Protects read_count</div><div class="sync-status">MUTEX</div></div>
        <div class="sync-box"><div class="sync-name">rw_lock</div><div class="sync-description">Protects shared resource</div><div class="sync-status" id="rwLockStatus">FREE</div></div>
        <div class="sync-box"><div class="sync-name">service_queue</div><div class="sync-description">Controls admission</div><div class="sync-status">ADMISSION</div></div>
      </div>

      <div class="resource-info">
        <div class="info-box"><div class="info-label">Active Readers</div><div class="info-value" id="activeReaders">None</div></div>
        <div class="info-box"><div class="info-label">Active Writer</div><div class="info-value" id="activeWriter">None</div></div>
        <div class="info-box"><div class="info-label">Read Count</div><div class="info-value" id="resourceReadCount">0</div></div>
        <div class="info-box"><div class="info-label">Shared Data</div><div class="info-value" id="resourceData">0</div></div>
      </div>
    </div>

    <div class="queue-section">
      <div class="queue"><div class="queue-title">Reader Waiting Queue</div><div class="queue-items" id="readerQueue"></div></div>
      <div class="queue"><div class="queue-title">Writer Waiting Queue</div><div class="queue-items" id="writerQueue"></div></div>
    </div>
  </div>

  <div class="panel">
    <div class="panel-heading"><h2>Writers</h2><span class="panel-tag writer-tag">EXCLUSIVE ACCESS</span></div>
    <div id="writerList"></div>
  </div>

</div>

<div class="panel event-panel">
  <div class="event-top"><h2>Execution Step</h2><div class="event-step" id="eventStep">STEP 0</div></div>
  <div class="event-card">
    <div class="event-type" id="eventType">READY</div>
    <div class="event-message" id="eventMessage">Simulation ready.</div>
  </div>
  <div class="controls">
    <button onclick="previousEvent()"> Previous</button>
    <button onclick="togglePlay()" id="playButton">Play</button>
    <button onclick="nextEvent()">Next </button>
    <button onclick="restart()"> Restart</button>
    <div class="speed">
      <span>Slow</span>
      <input type="range" min="100" max="1500" value="1000" id="speed">
      <span>Fast</span>
    </div>
  </div>
</div>

<div class="panel log-panel">
  <div class="panel-heading"><h2>Execution Log</h2></div>
  <div class="log" id="log"></div>
</div>

</div>

<div class="footer">
  Readers–Writers Problem | Semaphore Synchronization |
  __NUM_READERS__ Readers | __NUM_WRITERS__ Writers | __ROUNDS__ Rounds
</div>

<script>
// ===== ACTUAL EXECUTION DATA (recorded by the Python program) =====
const events = __EVENTS__;
const NUM_READERS = __NUM_READERS__;
const NUM_WRITERS = __NUM_WRITERS__;

// Snapshot shown before step 1 / after Restart
const initialState = {
  event_type: "ready", message: "Simulation ready.",
  read_count: 0, shared_data: 0,
  active_readers: [], active_writer: null,
  waiting_readers: [], waiting_writers: [],
  reader_states: {}, writer_states: {}
};
for (let i = 0; i < NUM_READERS; i++) initialState.reader_states["R" + i] = "Not Started";
for (let i = 0; i < NUM_WRITERS; i++) initialState.writer_states["W" + i] = "Not Started";

let currentEvent = -1;
let playing = false;
let timer = null;

const $ = id => document.getElementById(id);

function createActors(containerId, prefix, count, label) {
  const c = $(containerId);
  c.innerHTML = "";
  for (let i = 0; i < count; i++) {
    const id = prefix + i;
    c.innerHTML += `
      <div class="actor" id="actor-${id}">
        <div class="actor-top">
          <div class="actor-name">${label} ${id}</div>
          <div class="badge" id="status-${id}">NOT STARTED</div>
        </div>
        <div class="actor-description" id="description-${id}">Waiting for execution</div>
      </div>`;
  }
}

function cls(state) { return state.toLowerCase().replaceAll(" ", "-"); }

const readerText = {
  "Waiting": "Waiting for permission", "Reading": "Inside shared read section",
  "Releasing": "Leaving shared section", "Idle": "Between rounds",
  "Finished": "All rounds completed"
};
const writerText = {
  "Waiting": "Waiting for exclusive access", "Writing": "Inside exclusive write section",
  "Releasing": "Releasing shared resource", "Idle": "Between rounds",
  "Finished": "All rounds completed"
};

function updateActors(states, texts) {
  for (const [id, state] of Object.entries(states)) {
    $("actor-" + id).className = "actor " + cls(state);
    $("status-" + id).textContent = state.toUpperCase();
    $("status-" + id).className = "badge badge-" + cls(state);
    $("description-" + id).textContent = texts[state] || "Waiting for execution";
  }
}

function updateResource(ev) {
  const res = $("resource");
  res.classList.remove("free", "reading", "writing");
  if (ev.active_writer) {
    res.classList.add("writing");
    $("resourceIcon").textContent = "";
    $("resourceStatus").textContent = "EXCLUSIVE WRITE ACCESS";
    $("resourceActor").textContent = ev.active_writer + " is modifying shared data";
    $("rwLockStatus").textContent = "LOCKED BY WRITER";
  } else if (ev.active_readers.length > 0) {
    res.classList.add("reading");
    $("resourceIcon").textContent = "";
    $("resourceStatus").textContent = "SHARED READ ACCESS";
    $("resourceActor").textContent = ev.active_readers.join(", ") + " reading together";
    $("rwLockStatus").textContent = "LOCKED BY READERS";
  } else {
    res.classList.add("free");
    $("resourceIcon").textContent = "";
    $("resourceStatus").textContent = "RESOURCE AVAILABLE";
    $("resourceActor").textContent = "No active reader or writer";
    $("rwLockStatus").textContent = "FREE";
  }
  $("sharedData").textContent = ev.shared_data;
  $("resourceData").textContent = ev.shared_data;
  $("resourceReadCount").textContent = ev.read_count;
  $("readCount").textContent = ev.read_count;
  $("activeReaders").textContent = ev.active_readers.length ? ev.active_readers.join(", ") : "None";
  $("activeWriter").textContent = ev.active_writer ? ev.active_writer : "None";
}

function fillQueue(id, items) {
  const c = $(id);
  c.innerHTML = items.length
    ? items.map(x => `<span class="queue-item">${x}</span>`).join("")
    : `<span class="queue-empty">Empty</span>`;
}

function updateLog() {
  const log = $("log");
  let html = "";
  for (let i = Math.max(0, currentEvent - 30); i <= currentEvent; i++) {
    if (!events[i]) continue;
    html += `<div class="log-entry"><span class="log-number">STEP ${i + 1}</span>
             (t=${events[i].t}s) — ${events[i].message}</div>`;
  }
  log.innerHTML = html;
  log.scrollTop = log.scrollHeight;
}

// Draw ANY snapshot (initial state or a recorded event)
function render(ev) {
  updateActors(ev.reader_states, readerText);
  updateActors(ev.writer_states, writerText);
  updateResource(ev);
  fillQueue("readerQueue", ev.waiting_readers);
  fillQueue("writerQueue", ev.waiting_writers);
  $("eventType").textContent = ev.event_type.toUpperCase();
  $("eventMessage").textContent = ev.message;
}

function showEvent(index) {
  if (index < 0) {                       // full reset to the initial snapshot
    currentEvent = -1;
    render(initialState);
    $("eventStep").textContent = "STEP 0";
    $("eventCounter").textContent = "0 / " + events.length;
    updateLog();
    return;
  }
  if (index >= events.length) { stopPlaying(); return; }
  currentEvent = index;
  render(events[index]);
  $("eventStep").textContent = "STEP " + (index + 1) + "  (t = " + events[index].t + " s)";
  $("eventCounter").textContent = (index + 1) + " / " + events.length;
  updateLog();
}

function nextEvent() {
  if (currentEvent < events.length - 1) showEvent(currentEvent + 1);
  else stopPlaying();
}

function previousEvent() {
  if (currentEvent >= 0) showEvent(currentEvent - 1);   // can go back to step 0
}

// Slider: right = faster. Interval = 1600 - value (100..1500 -> 1500..100 ms)
function intervalMs() { return 1600 - parseInt($("speed").value); }

function startTimer() {
  clearInterval(timer);
  timer = setInterval(nextEvent, intervalMs());
}

function startPlaying() {
  if (currentEvent >= events.length - 1) showEvent(-1);
  playing = true;
  $("playButton").textContent = " Pause";
  startTimer();
}

function stopPlaying() {
  playing = false;
  clearInterval(timer);
  $("playButton").textContent = " Play";
}

function togglePlay() { playing ? stopPlaying() : startPlaying(); }

function restart() { stopPlaying(); showEvent(-1); }

// Speed change applies immediately while playing
$("speed").addEventListener("input", () => { if (playing) startTimer(); });

createActors("readerList", "R", NUM_READERS, "Reader");
createActors("writerList", "W", NUM_WRITERS, "Writer");
restart();
</script>
</body>
</html>
"""


def get_output_folder():
    folder = Path(__file__).resolve().parent / "generated_html"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def generate_html():
    """Build the HTML page from the events recorded during execution."""
    html_file = get_output_folder() / "readers_writers_semaphore.html"

    html = (HTML_TEMPLATE
            .replace("__EVENTS__", json.dumps(events))
            .replace("__NUM_READERS__", str(NUM_READERS))
            .replace("__NUM_WRITERS__", str(NUM_WRITERS))
            .replace("__ROUNDS__", str(ROUNDS))
            .replace("__TOTAL__", str(len(events))))

    html_file.write_text(html, encoding="utf-8")

    print("\nHTML visualization generated.")
    print(f"File: {html_file}")
    print("\nOpening visualization in browser...")
    try:
        webbrowser.open(html_file.resolve().as_uri())
    except Exception as exc:
        print(f"(Could not open browser automatically: {exc})")



# MAIN

def main():
    global start_time

    print("=" * 65)
    print("READERS-WRITERS - SEMAPHORE SOLUTION")
    print("=" * 65)

    print(f"\nNumber of readers: {NUM_READERS}")
    print(f"Number of writers: {NUM_WRITERS}")
    print(f"Rounds per process: {ROUNDS}")

    print("\nSynchronization:")
    print("reader_mutex  -> protects read_count")
    print("rw_lock       -> protects the shared resource")
    print("service_queue -> turnstile for fair admission (prevents writer starvation)")

    print("\nCritical section:")
    print("Readers: reading the shared resource (many at once)")
    print("Writers: modifying the shared resource (exclusive)")

    print("\nStarting reader and writer threads...\n")

    start_time = time.time()
    threads = []

    for i in range(NUM_READERS):
        threads.append(threading.Thread(target=reader, args=(i,), name=f"Reader-R{i}"))
    for i in range(NUM_WRITERS):
        threads.append(threading.Thread(target=writer, args=(i,), name=f"Writer-W{i}"))

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    success = final_verification()
    generate_html()

    if success:
        print("\nAll readers and writers completed successfully.")
    else:
        print("\nExecution completed with verification errors.")


if __name__ == "__main__":
    main()