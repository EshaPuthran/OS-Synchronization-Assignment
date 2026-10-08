"""

MONITOR DESIGN
--------------
Python has no native `monitor` keyword, so the monitor is built the standard
way: one class that owns
  * ONE mutex (self._lock)  -> only one thread is inside the monitor at a time
  * condition variables     -> can_read / can_write, both tied to that mutex
  * private monitor state   -> readers, writer_active, FIFO queue
All state is touched only inside the monitor's methods while holding the lock.

Public monitor procedures:  start_read, end_read, start_write, end_write.
The actual reading/writing of the shared data happens OUTSIDE the monitor,
between start_x() and end_x() - the monitor guarantees that this is safe.

FAIRNESS: requests are served in FIFO order (a queue of tickets). A reader
may enter only when it is at the head of the queue and no writer is active;
a writer only when it is at the head, no writer is active and no reader is
active. Consecutive readers at the head enter one after another, so readers
still run concurrently. Neither readers nor writers can starve.

event_lock protects ONLY the logging/visualisation state, not the algorithm.
"""

import threading
import time
import random
import json
import webbrowser
from collections import deque
from pathlib import Path


# CONFIGURATION

NUM_READERS = 5
NUM_WRITERS = 2
ROUNDS = 3


# SHARED RESOURCE (critical section data)

shared_data = 0
writer_updates = 0


# VISUALISATION STATE 

read_count = 0
reader_states = {f"R{i}": "Not Started" for i in range(NUM_READERS)}
writer_states = {f"W{i}": "Not Started" for i in range(NUM_WRITERS)}

fifo_queue = []          # names waiting in the monitor, in FIFO order
active_readers = []
active_writer = None

reader_wait_times = []
writer_wait_times = []
monitor_violations = []  # invariant violations detected inside the monitor
fifo_violations = []     # FIFO fairness violations detected inside the monitor

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
        "fifo_queue": list(fifo_queue),
        "waiting_readers": [n for n in fifo_queue if n.startswith("R")],
        "waiting_writers": [n for n in fifo_queue if n.startswith("W")],
        "reader_states": dict(reader_states),
        "writer_states": dict(writer_states),
    })


def record_event(message, event_type="info"):
    with event_lock:
        add_event_locked(message, event_type)


# THE MONITOR

class ReadersWritersMonitor:
    def __init__(self):
        self._lock = threading.Lock()                        # monitor mutex
        self._can_read = threading.Condition(self._lock)     # readers wait here
        self._can_write = threading.Condition(self._lock)    # writers wait here
        self._queue = deque()          # FIFO of (name, "R"/"W") tickets
        self._readers = 0              # number of active readers
        self._writer_active = False
        self._served = 0               # requests admitted so far (fairness check)

    def _wake_head(self):
        """Signal the condition variable of the process at the queue head.
        Predicates are re-checked by the waiters, so extra wake-ups are safe."""
        if not self._queue:
            return
        if self._queue[0][1] == "R":
            self._can_read.notify_all()
        else:
            self._can_write.notify_all()

    # ---------------- reader procedures ----------------
    def start_read(self, name):
        global read_count
        ticket = (name, "R")
        t0 = time.time()
        with self._lock:                                   # enter monitor
            position = len(self._queue)                    # requests ahead of me
            served_at_arrival = self._served
            self._queue.append(ticket)
            with event_lock:
                fifo_queue.append(name)
                add_event_locked(
                    f"Reader {name} entered the monitor and joined the FIFO "
                    f"queue (requests ahead: {position}).", "monitor_enter")

            blocked = False
            # Wait until: I am at the head AND no writer is active.
            while not (self._queue[0] == ticket and not self._writer_active):
                if not blocked:
                    blocked = True
                    record_event(f"Reader {name} must wait: can_read.wait() "
                                 f"(monitor lock released).", "cond_wait")
                self._can_read.wait()

            # --- admitted ---
            self._queue.popleft()
            self._readers += 1
            if self._writer_active:
                monitor_violations.append(f"reader {name} admitted during write")
            if self._served - served_at_arrival != position:
                fifo_violations.append(f"{name} overtaken or overtook others")
            self._served += 1

            with event_lock:
                fifo_queue.remove(name)
                read_count = self._readers
                active_readers.append(name)
                reader_states[name] = "Reading"
                reader_wait_times.append(time.time() - t0)
                add_event_locked(
                    f"Reader {name} ENTERED the critical section and reads "
                    f"shared data = {shared_data}. read_count = {self._readers}.",
                    "reader_enter")
            self._wake_head()      # next queued reader (if any) may join us

    def end_read(self, name):
        global read_count
        with self._lock:
            self._readers -= 1
            with event_lock:
                if name in active_readers:
                    active_readers.remove(name)
                reader_states[name] = "Releasing"
                read_count = self._readers
                add_event_locked(
                    f"Reader {name} EXITED the critical section. "
                    f"read_count = {self._readers}.", "reader_exit")
            if self._readers == 0:
                record_event(f"Reader {name} was the last reader: monitor "
                             f"signals the process at the queue head.", "signal")
            self._wake_head()

    # ---------------- writer procedures ----------------
    def start_write(self, name):
        global active_writer
        ticket = (name, "W")
        t0 = time.time()
        with self._lock:
            position = len(self._queue)
            served_at_arrival = self._served
            self._queue.append(ticket)
            with event_lock:
                fifo_queue.append(name)
                add_event_locked(
                    f"Writer {name} entered the monitor and joined the FIFO "
                    f"queue (requests ahead: {position}).", "monitor_enter")

            blocked = False
            # Wait until: I am at the head, no writer and no reader is active.
            while not (self._queue[0] == ticket
                       and not self._writer_active and self._readers == 0):
                if not blocked:
                    blocked = True
                    record_event(f"Writer {name} must wait: can_write.wait() "
                                 f"(monitor lock released).", "cond_wait")
                self._can_write.wait()

            # --- admitted ---
            self._queue.popleft()
            if self._writer_active or self._readers > 0:
                monitor_violations.append(f"writer {name} admitted while busy")
            self._writer_active = True
            if self._served - served_at_arrival != position:
                fifo_violations.append(f"{name} overtaken or overtook others")
            self._served += 1

            with event_lock:
                fifo_queue.remove(name)
                active_writer = name
                writer_states[name] = "Writing"
                writer_wait_times.append(time.time() - t0)
                add_event_locked(f"Writer {name} ENTERED the critical section "
                                 f"with exclusive access.", "writer_enter")

    def end_write(self, name):
        global active_writer
        with self._lock:
            self._writer_active = False
            with event_lock:
                active_writer = None
                writer_states[name] = "Releasing"
                add_event_locked(f"Writer {name} EXITED the critical section.",
                                 "writer_exit")
            record_event(f"Writer {name} leaves the monitor and signals the "
                         f"process at the queue head.", "signal")
            self._wake_head()


monitor = ReadersWritersMonitor()


# READER THREAD

def reader(reader_id):
    name = f"R{reader_id}"

    for round_no in range(1, ROUNDS + 1):
        with event_lock:
            reader_states[name] = "Waiting"
            add_event_locked(f"Reader {name} is thinking (round {round_no}).",
                             "waiting")

        time.sleep(random.uniform(0.05, 0.15))
        record_event(f"Reader {name} requests READ access.", "request")

        monitor.start_read(name)            # blocks until admitted

        # ---- critical section: shared reading (outside monitor lock) ----
        time.sleep(random.uniform(0.15, 0.30))
        record_event(f"Reader {name} is reading shared data = {shared_data}.",
                     "reading")
        time.sleep(random.uniform(0.05, 0.15))

        monitor.end_read(name)

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
    global shared_data, writer_updates

    name = f"W{writer_id}"

    for round_no in range(1, ROUNDS + 1):
        with event_lock:
            writer_states[name] = "Waiting"
            add_event_locked(f"Writer {name} is thinking (round {round_no}).",
                             "waiting")

        time.sleep(random.uniform(0.05, 0.15))
        record_event(f"Writer {name} requests WRITE access.", "request")

        monitor.start_write(name)           # blocks until admitted

        # ---- critical section: exclusive writing (outside monitor lock) ----
        time.sleep(random.uniform(0.15, 0.30))

        with event_lock:
            shared_data += 1
            writer_updates += 1
            add_event_locked(f"Writer {name} UPDATED shared data to "
                             f"{shared_data}.", "writing")

        time.sleep(random.uniform(0.05, 0.15))

        monitor.end_write(name)

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


# VERIFICATION (computed from the recorded execution)

def check_invariants():
    results = {}

    violations = [i for i, e in enumerate(events)
                  if e["active_writer"] and e["active_readers"]]
    results["Writer exclusive access (no reader/writer overlap)"] = (
        not violations and not monitor_violations,
        "no violations in any recorded state" if not violations
        else f"violated at steps {violations[:5]}")

    max_readers = max((len(e["active_readers"]) for e in events), default=0)
    results["Multiple readers allowed concurrently"] = (
        max_readers > 1, f"max simultaneous readers = {max_readers}")

    bad = [e["read_count"] for e in events
           if not 0 <= e["read_count"] <= NUM_READERS]
    results["read_count kept consistent by the monitor"] = (
        not bad and read_count == 0, f"final read_count = {read_count}")

    max_w = max(writer_wait_times, default=0)
    max_r = max(reader_wait_times, default=0)
    results["FIFO fairness / no starvation"] = (
        not fifo_violations
        and all(s == "Finished" for s in writer_states.values())
        and all(s == "Finished" for s in reader_states.values()),
        f"every request served after exactly the requests ahead of it; "
        f"max writer wait = {max_w:.2f}s, max reader wait = {max_r:.2f}s")

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
        "No active/waiting processes left": not (active_readers or active_writer or fifo_queue),
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
<title>Readers-Writers Monitor Simulation</title>
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
.queue-title{font-size:12px;font-weight:bold;margin-bottom:8px;font-family:Consolas,monospace}
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
  <p>Monitor Solution • Actual Execution Visualization</p>
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
    <div class="panel-heading"><h2>Shared Resource</h2><span class="panel-tag">PROTECTED BY MONITOR</span></div>
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
        <div class="sync-box"><div class="sync-name">monitor lock</div><div class="sync-description">One thread inside the monitor</div><div class="sync-status">MUTEX</div></div>
        <div class="sync-box"><div class="sync-name">can_read</div><div class="sync-description">Condition variable (readers)</div><div class="sync-status" id="canReadStatus">0 WAITING</div></div>
        <div class="sync-box"><div class="sync-name">can_write</div><div class="sync-description">Condition variable (writers)</div><div class="sync-status" id="canWriteStatus">0 WAITING</div></div>
      </div>

      <div class="resource-info">
        <div class="info-box"><div class="info-label">Active Readers</div><div class="info-value" id="activeReaders">None</div></div>
        <div class="info-box"><div class="info-label">Active Writer</div><div class="info-value" id="activeWriter">None</div></div>
        <div class="info-box"><div class="info-label">Read Count</div><div class="info-value" id="resourceReadCount">0</div></div>
        <div class="info-box"><div class="info-label">FIFO Queue (head → tail)</div><div class="info-value" id="fifoQueue">Empty</div></div>
      </div>
    </div>

    <div class="queue-section">
      <div class="queue"><div class="queue-title">Waiting on can_read</div><div class="queue-items" id="readerQueue"></div></div>
      <div class="queue"><div class="queue-title">Waiting on can_write</div><div class="queue-items" id="writerQueue"></div></div>
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
    <button onclick="restart()">Restart</button>
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
  Readers–Writers Problem | Monitor Synchronization |
  __NUM_READERS__ Readers | __NUM_WRITERS__ Writers | __ROUNDS__ Rounds
</div>

<script>
// ===== ACTUAL EXECUTION DATA (recorded by the Python program) =====
const events = __EVENTS__;
const NUM_READERS = __NUM_READERS__;
const NUM_WRITERS = __NUM_WRITERS__;

const initialState = {
  event_type: "ready", message: "Simulation ready.",
  read_count: 0, shared_data: 0,
  active_readers: [], active_writer: null,
  fifo_queue: [], waiting_readers: [], waiting_writers: [],
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
  "Waiting": "Thinking / requesting access", "Reading": "Inside shared read section",
  "Releasing": "Leaving shared section", "Idle": "Between rounds",
  "Finished": "All rounds completed"
};
const writerText = {
  "Waiting": "Thinking / requesting access", "Writing": "Inside exclusive write section",
  "Releasing": "Leaving shared section", "Idle": "Between rounds",
  "Finished": "All rounds completed"
};

function updateActors(states, texts, queued) {
  for (const [id, state] of Object.entries(states)) {
    let shown = state, desc = texts[state] || "Waiting for execution";
    // a process that is inside the monitor queue is blocked on a condition variable
    if (state === "Waiting" && queued.includes(id)) {
      desc = "In monitor queue, waiting for its turn";
    }
    $("actor-" + id).className = "actor " + cls(state);
    $("status-" + id).textContent = shown.toUpperCase();
    $("status-" + id).className = "badge badge-" + cls(state);
    $("description-" + id).textContent = desc;
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
  } else if (ev.active_readers.length > 0) {
    res.classList.add("reading");
    $("resourceIcon").textContent = "";
    $("resourceStatus").textContent = "SHARED READ ACCESS";
    $("resourceActor").textContent = ev.active_readers.join(", ") + " reading together";
  } else {
    res.classList.add("free");
    $("resourceIcon").textContent = "";
    $("resourceStatus").textContent = "RESOURCE AVAILABLE";
    $("resourceActor").textContent = "No active reader or writer";
  }
  $("sharedData").textContent = ev.shared_data;
  $("resourceReadCount").textContent = ev.read_count;
  $("readCount").textContent = ev.read_count;
  $("activeReaders").textContent = ev.active_readers.length ? ev.active_readers.join(", ") : "None";
  $("activeWriter").textContent = ev.active_writer ? ev.active_writer : "None";
  $("fifoQueue").textContent = ev.fifo_queue.length ? ev.fifo_queue.join(" → ") : "Empty";
  $("canReadStatus").textContent = ev.waiting_readers.length + " QUEUED";
  $("canWriteStatus").textContent = ev.waiting_writers.length + " QUEUED";
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

function render(ev) {
  updateActors(ev.reader_states, readerText, ev.waiting_readers);
  updateActors(ev.writer_states, writerText, ev.waiting_writers);
  updateResource(ev);
  fillQueue("readerQueue", ev.waiting_readers);
  fillQueue("writerQueue", ev.waiting_writers);
  $("eventType").textContent = ev.event_type.toUpperCase();
  $("eventMessage").textContent = ev.message;
}

function showEvent(index) {
  if (index < 0) {
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
  if (currentEvent >= 0) showEvent(currentEvent - 1);
}

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
    html_file = get_output_folder() / "readers_writers_monitor.html"

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
    print("READERS-WRITERS - MONITOR SOLUTION")
    print("=" * 65)

    print(f"\nNumber of readers: {NUM_READERS}")
    print(f"Number of writers: {NUM_WRITERS}")
    print(f"Rounds per process: {ROUNDS}")

    print("\nMonitor (lock + condition variables):")
    print("monitor lock -> only one thread executes monitor code at a time")
    print("can_read     -> condition variable where readers wait")
    print("can_write    -> condition variable where writers wait")
    print("FIFO queue   -> requests are served in arrival order (no starvation)")

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
