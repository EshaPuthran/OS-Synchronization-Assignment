import threading
import time
import random
import json
import webbrowser
from pathlib import Path


# ============================================================
# 1. CONFIGURATION
# ============================================================

N = 4                       # Exactly four philosophers
ROUNDS = 3                  # Meals per philosopher

# Shared resources: four forks (F0, F1, F2, F3).
# Fork F_i lies between P_i and P_(i+1):
#   P0 uses F3 and F0
#   P1 uses F0 and F1
#   P2 uses F1 and F2
#   P3 uses F2 and F3
#
# Critical section: the EATING step, while a philosopher owns both forks.
#
# Every shared variable below is read or written ONLY while the
# monitor lock is held (inside DiningMonitor methods or inside
# "with monitor.lock").

philosopher_states = ["Thinking"] * N
fork_owners = [None] * N
held_forks = [[] for _ in range(N)]
waiting_for = [None] * N

rounds_completed = [0] * N

events = []

# Statistics
total_wait_time = [0.0] * N
maximum_wait_time = [0.0] * N
fork_acquisition_count = [0] * N

# Exceptions raised by philosopher worker threads
worker_errors = []


# ============================================================
# 2. THE MONITOR
# ============================================================

class DiningMonitor:
    """
    Monitor abstraction built from one mutex and condition variables.

    * lock     : the single monitor lock (mutual exclusion).
    * can_eat  : one condition variable per philosopher. A philosopher
                 waits on its own condition variable until both of its
                 forks are free AND no older hungry neighbour is ahead.

    Deadlock prevention:
        Both forks are taken atomically inside the monitor (all or
        nothing). A philosopher never holds one fork while waiting
        for the other, so a circular wait cannot form.

    Starvation prevention:
        Every hungry philosopher gets an increasing ticket number.
        A philosopher may not eat while an adjacent hungry philosopher
        holds an older ticket. The oldest hungry philosopher is
        therefore never overtaken by a neighbour.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.can_eat = [threading.Condition(self.lock) for _ in range(N)]

        self.hungry_ticket = [None] * N   # None = not hungry
        self.next_ticket = 0

        self.eating_now = 0
        self.max_eating = 0

    # ---------- helpers (call only while holding self.lock) ----------

    @staticmethod
    def _forks_of(i):
        return (i - 1) % N, i              # (left fork, right fork)

    @staticmethod
    def _neighbours(i):
        return (i - 1) % N, (i + 1) % N

    def _may_eat(self, i):
        """Condition predicate: both forks free and no older hungry neighbour."""
        left, right = self._forks_of(i)

        if fork_owners[left] is not None or fork_owners[right] is not None:
            return False

        for neighbour in self._neighbours(i):
            ticket = self.hungry_ticket[neighbour]
            if ticket is not None and ticket < self.hungry_ticket[i]:
                return False               # older hungry neighbour goes first

        return True

    def _blocking_fork(self, i):
        """The fork that currently stops philosopher i (for the display)."""
        left, right = self._forks_of(i)
        if fork_owners[left] is not None:
            return left
        if fork_owners[right] is not None:
            return right
        return left                        # blocked only by ticket order

    # ---------- monitor operations ----------

    def pickup_forks(self, i):
        """Block until philosopher i may take BOTH forks, then take them."""
        left, right = self._forks_of(i)
        start_wait = time.monotonic()

        with self.lock:

            ticket = self.next_ticket
            self.next_ticket += 1
            self.hungry_ticket[i] = ticket

            add_event_locked(
                i,
                "Hungry",
                f"P{i} is HUNGRY and requests F{left} and F{right}. "
                f"Monitor ticket {ticket}."
            )

            announced = False

            # "while", not "if": the condition must be re-checked
            # every time the thread wakes up.
            while not self._may_eat(i):

                if not announced:
                    waiting_for[i] = self._blocking_fork(i)
                    add_event_locked(
                        i,
                        "Waiting",
                        f"P{i} WAITS on its condition variable "
                        f"(blocked by F{waiting_for[i]} or an older "
                        f"hungry neighbour)."
                    )
                    announced = True

                self.can_eat[i].wait()     # releases the lock while waiting

            waiting_for[i] = None

            # Explicit mutual-exclusion verification
            if fork_owners[left] is not None or fork_owners[right] is not None:
                raise RuntimeError(
                    f"MUTUAL EXCLUSION VIOLATION: P{i} tried to take "
                    f"F{left} and F{right}, owners are "
                    f"{fork_owners[left]} and {fork_owners[right]}"
                )

            # Both forks are taken atomically.
            for fork_number in (left, right):
                fork_owners[fork_number] = i
                held_forks[i].append(fork_number)
                fork_acquisition_count[fork_number] += 1

            self.hungry_ticket[i] = None

            self.eating_now += 1
            self.max_eating = max(self.max_eating, self.eating_now)

            elapsed = time.monotonic() - start_wait
            total_wait_time[i] += elapsed
            maximum_wait_time[i] = max(maximum_wait_time[i], elapsed)

            add_event_locked(
                i,
                "Holding",
                f"P{i} acquired F{left} and F{right} atomically "
                f"(ticket {ticket}). Exclusive ownership verified."
            )

            print(f"[MUTEX CHECK PASSED] F{left} and F{right} belong only to P{i}")

    def putdown_forks(self, i):
        """Release both forks and wake up the waiting philosophers."""
        left, right = self._forks_of(i)

        with self.lock:

            for fork_number in (right, left):

                if fork_owners[fork_number] != i:
                    raise RuntimeError(
                        f"INVALID RELEASE: P{i} attempted to release "
                        f"F{fork_number}, owned by P{fork_owners[fork_number]}"
                    )

                fork_owners[fork_number] = None
                held_forks[i].remove(fork_number)

            self.eating_now -= 1

            add_event_locked(
                i,
                "Releasing",
                f"P{i} released F{left} and F{right}. Both forks are FREE."
            )

            print(f"[RELEASE VERIFIED] F{left} and F{right} are now FREE")

            # Signal: every waiting philosopher re-checks its predicate.
            for condition in self.can_eat:
                condition.notify()


monitor = DiningMonitor()


# ============================================================
# 3. STATE VERIFICATION AND EVENT RECORDING
# ============================================================

def verify_shared_state():
    """
    Verify that:
    1. Every fork has at most one owner.
    2. A fork is held only by one of its two neighbours.
    3. Fork ownership agrees with the held_forks records.
    """

    for fork_number, owner in enumerate(fork_owners):

        if owner is not None:

            allowed_owners = (fork_number, (fork_number + 1) % N)

            if owner not in allowed_owners:
                raise RuntimeError(
                    f"INVALID OWNERSHIP: P{owner} cannot own F{fork_number}"
                )

            if held_forks[owner].count(fork_number) != 1:
                raise RuntimeError(
                    f"OWNERSHIP RECORD ERROR: F{fork_number} and P{owner}"
                )

    for philosopher_number in range(N):

        for fork_number in held_forks[philosopher_number]:

            if fork_owners[fork_number] != philosopher_number:
                raise RuntimeError(
                    f"INCONSISTENT OWNERSHIP: "
                    f"P{philosopher_number}, F{fork_number}"
                )


def add_event_locked(number, state, message):
    """
    Record a complete snapshot of the current execution.

    Must be called while monitor.lock is held.
    """

    philosopher_states[number] = state

    if state != "Waiting":
        waiting_for[number] = None

    verify_shared_state()

    event = {
        "step": len(events) + 1,
        "philosopher": number,
        "state": state,
        "message": message,
        "philosophers": philosopher_states.copy(),
        "forks": fork_owners.copy(),
        "held_forks": [item.copy() for item in held_forks],
        "waiting_for": waiting_for.copy(),
        "rounds": rounds_completed.copy()
    }

    events.append(event)

    print(message)


def record_event(number, state, message):
    """Record an event from outside the monitor methods."""

    with monitor.lock:
        add_event_locked(number, state, message)


# ============================================================
# 4. PHILOSOPHER THREAD
# ============================================================

def philosopher(number):
    """
    Each philosopher repeatedly:
    THINK -> HUNGRY -> (WAIT) -> ACQUIRE BOTH FORKS ->
    EAT -> RELEASE FORKS.
    """

    for round_number in range(ROUNDS):

        # ---------------- THINKING ----------------

        record_event(number, "Thinking", f"P{number} is THINKING")

        time.sleep(random.uniform(0.8, 1.5))

        # ---------------- HUNGRY / WAITING / ACQUIRING ----------------
        # pickup_forks() blocks inside the monitor until both forks
        # can be taken together.

        monitor.pickup_forks(number)

        try:

            # ---------------- EATING (critical section) ----------------

            left_fork = (number - 1) % N
            right_fork = number

            record_event(
                number,
                "Eating",
                f"P{number} is EATING with F{left_fork} and F{right_fork} "
                f"(round {round_number + 1})."
            )

            time.sleep(random.uniform(0.8, 1.5))

        finally:

            # ---------------- RELEASING ----------------
            # Always release the forks, even if an error occurs.

            monitor.putdown_forks(number)

        # ---------------- THINKING AGAIN ----------------

        with monitor.lock:

            rounds_completed[number] = round_number + 1

            add_event_locked(
                number,
                "Thinking",
                f"P{number} completed round {round_number + 1}"
            )

    # ---------------- FINISHED ----------------

    record_event(number, "Finished", f"P{number} has FINISHED all meals")


# ============================================================
# 5. FINAL VERIFICATION
# ============================================================

def final_verification():

    print("\n")
    print("=" * 60)
    print("FINAL SYNCHRONIZATION VERIFICATION (MONITOR)")
    print("=" * 60)

    with monitor.lock:

        verify_shared_state()

        print("\nPHILOSOPHER MEAL COMPLETION")

        for i in range(N):

            print(f"P{i}: Completed {rounds_completed[i]} meals")

            if rounds_completed[i] != ROUNDS:
                raise RuntimeError(
                    f"P{i} did not complete all {ROUNDS} meals."
                )

        print("\nFORK OWNERSHIP AND ACQUISITION COUNTS")

        for i in range(N):

            owner_text = (
                "FREE"
                if fork_owners[i] is None
                else f"Owned by P{fork_owners[i]}"
            )

            print(
                f"F{i}: {owner_text}; "
                f"acquired {fork_acquisition_count[i]} times"
            )

            if fork_owners[i] is not None:
                raise RuntimeError(f"F{i} was not released.")

            if fork_acquisition_count[i] != 2 * ROUNDS:
                raise RuntimeError(
                    f"F{i} should have {2 * ROUNDS} acquisitions."
                )

        print("\nCONCURRENT EATING VERIFICATION")
        print(f"Maximum philosophers eating at the same time: {monitor.max_eating}")
        print(f"Allowed maximum (ring of {N}, neighbours exclusive): {N // 2}")

        if monitor.eating_now != 0:
            raise RuntimeError("Some philosopher is still marked as eating.")

        if monitor.max_eating > N // 2:
            raise RuntimeError("Too many philosophers ate at the same time.")

        if any(ticket is not None for ticket in monitor.hungry_ticket):
            raise RuntimeError("A philosopher is still marked as hungry.")

        if any(state != "Finished" for state in philosopher_states):
            raise RuntimeError("Not all philosophers finished.")

        print("\nWAITING TIME STATISTICS")

        for i in range(N):

            print(
                f"P{i}: Total waiting time = {total_wait_time[i]:.2f}s, "
                f"Maximum waiting time = {maximum_wait_time[i]:.2f}s"
            )

        print("\nALL FINAL CHECKS PASSED.")
        print("Mutual exclusion: PASSED")
        print("All philosophers completed the required meals: PASSED")
        print("Every fork was acquired exactly 2 * ROUNDS times: PASSED")
        print("All forks are free: PASSED")
        print("Adjacent philosophers never ate together: PASSED")
        print("Deadlock prevention: both forks taken atomically (all or nothing).")
        print(
            "Starvation mitigation: ticket order - a philosopher never "
            "eats ahead of an older hungry neighbour."
        )
        print(
            "Fairness assumption: threads continue to be scheduled and "
            "each meal eventually finishes."
        )
        print("=" * 60)


# ============================================================
# 6. GENERATE HTML VISUALIZATION
# ============================================================

def get_output_folder():
    """
    Generate the HTML inside the monitor folder.
    """

    script_path = Path(__file__).resolve()

    return script_path.parent / "generated_html"


def generate_html():

    output_folder = get_output_folder()

    output_folder.mkdir(parents=True, exist_ok=True)

    html = r"""
<!DOCTYPE html>

<html lang="en">

<head>

<meta charset="UTF-8">

<meta name="viewport"
content="width=device-width, initial-scale=1.0">

<title>Dining Philosophers - Monitor Simulation</title>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    padding: 20px;
    font-family: Arial, sans-serif;
    background: #101827;
    color: white;
    text-align: center;
}

h1 {
    color: #8ec5ff;
}

.subtitle {
    color: #aab8ca;
}

.dashboard {
    max-width: 1200px;
    margin: 20px auto;

    display: grid;
    grid-template-columns: minmax(480px, 1fr) 330px;
    gap: 20px;
}

.simulation,
.side-panel {
    background: #1b293c;
    padding: 20px;
    border-radius: 18px;
}

.table {
    position: relative;
    width: 500px;
    height: 460px;
    max-width: 100%;
    margin: auto;
}

.circle {
    position: absolute;
    width: 250px;
    height: 250px;

    border-radius: 50%;
    background: #30435c;
    border: 8px solid #536d8e;

    left: 50%;
    top: 50%;

    transform: translate(-50%, -50%);

    display: flex;
    align-items: center;
    justify-content: center;

    font-size: 20px;
    font-weight: bold;
}

.philosopher {
    position: absolute;

    width: 115px;
    min-height: 80px;

    padding: 10px 5px;

    border-radius: 15px;
    border: 2px solid #64748b;

    z-index: 3;

    font-size: 13px;
    font-weight: bold;

    transition: transform 0.4s ease,
                background 0.3s ease,
                box-shadow 0.3s ease;
}

#p0 {
    top: 0;
    left: 50%;
    transform: translateX(-50%);
}

#p1 {
    right: 0;
    top: 50%;
    transform: translateY(-50%);
}

#p2 {
    bottom: 0;
    left: 50%;
    transform: translateX(-50%);
}

#p3 {
    left: 0;
    top: 50%;
    transform: translateY(-50%);
}

/* Neighbour movement */

#p0.leaning {
    transform: translate(-50%, 18px);
}

#p1.leaning {
    transform: translate(-18px, -50%);
}

#p2.leaning {
    transform: translate(-50%, -18px);
}

#p3.leaning {
    transform: translate(18px, -50%);
}

/* Philosopher states */

.thinking {
    background: #286b83;
}

.hungry {
    background: #a87521;
}

.waiting {
    background: #a34b35;
    box-shadow: 0 0 14px #a34b35;
}

.holding {
    background: #53658d;
}

.eating {
    background: #23875b;
    box-shadow: 0 0 18px #23875b;
    animation: pulse 0.8s infinite alternate;
}

.releasing {
    background: #6956a5;
}

.finished {
    background: #475569;
}

@keyframes pulse {
    from { box-shadow: 0 0 4px #23875b; }
    to { box-shadow: 0 0 22px #42df91; }
}

/* Forks */

.fork {
    position: absolute;

    width: 85px;
    padding: 12px 4px;

    border-radius: 10px;
    border: 2px solid #a9b8ca;

    background: #64748b;

    color: white;
    font-size: 12px;
    font-weight: bold;

    z-index: 4;

    transition: transform 0.4s ease,
                background 0.3s ease;
}

.fork.held {
    background: #e4a83c;
    border-color: #ffe1a0;
    color: #18202c;
}

#f0 {
    top: 100px;
    right: 85px;
}

#f1 {
    bottom: 100px;
    right: 85px;
}

#f2 {
    bottom: 100px;
    left: 85px;
}

#f3 {
    top: 100px;
    left: 85px;
}

.controls {
    margin: 12px;
}

button {
    padding: 11px 16px;
    margin: 5px;

    border: none;
    border-radius: 8px;

    background: #4285d4;
    color: white;

    font-weight: bold;
    cursor: pointer;
}

button:hover {
    background: #65a4ed;
}

select {
    padding: 8px;
    border-radius: 7px;
}

#counter {
    color: #8ec5ff;
    font-size: 18px;
    font-weight: bold;
}

.status {
    background: #29394e;
    padding: 11px;
    margin: 8px 0;
    border-radius: 9px;
    text-align: left;
    font-size: 13px;
}

.wait-label {
    color: #ffc66d;
}

.hold-label {
    color: #8ec5ff;
}

#log {
    background: #111c2c;
    border-radius: 10px;
    padding: 12px;

    height: 230px;
    overflow-y: auto;

    text-align: left;
    font-size: 12px;
}

#log p {
    padding-bottom: 6px;
    border-bottom: 1px solid #29394e;
}

@media(max-width: 900px) {

    .dashboard {
        grid-template-columns: 1fr;
    }

}

</style>

</head>

<body>

<h1>Dining Philosophers Problem</h1>

<p class="subtitle">
Monitor-Based Synchronization (lock + condition variables) |
4 Philosophers | 4 Forks
</p>

<div class="dashboard">

<div class="simulation">

<h2>Dining Table</h2>

<div class="table">

<div class="circle">
DINING TABLE
</div>

<div class="philosopher thinking" id="p0">
P0<br>Thinking
</div>

<div class="philosopher thinking" id="p1">
P1<br>Thinking
</div>

<div class="philosopher thinking" id="p2">
P2<br>Thinking
</div>

<div class="philosopher thinking" id="p3">
P3<br>Thinking
</div>

<div class="fork" id="f0">F0<br>Free</div>
<div class="fork" id="f1">F1<br>Free</div>
<div class="fork" id="f2">F2<br>Free</div>
<div class="fork" id="f3">F3<br>Free</div>

</div>

<div class="controls">

<button onclick="play()">▶ Play</button>

<button onclick="pause()">Ⅱ Pause</button>

<button onclick="next()">Next Event</button>

<button onclick="restart()">↻ Restart</button>

</div>

<label for="speed">Playback speed:</label>

<select id="speed" onchange="changeSpeed()">
    <option value="1600">Slow</option>
    <option value="850" selected>Normal</option>
    <option value="400">Fast</option>
    <option value="150">Very Fast</option>
</select>

<p id="counter">Event 0</p>

</div>

<div class="side-panel">

<h2>Philosopher Status</h2>

<div id="statuses"></div>

<h3>Fork Ownership</h3>

<div id="forkInfo"></div>

<h3>Execution Log</h3>

<div id="log"></div>

</div>

</div>


<script>

const events = __EVENT_DATA__;

let index = 0;
let timer = null;
let speed = 850;


// --------------------------------------------------
// Display labels for philosopher states
// --------------------------------------------------

const displayState = {
    "Waiting": "Waiting (condition variable)",
    "Holding": "Acquiring Forks",
    "Releasing": "Releasing Forks"
};


// --------------------------------------------------
// Fork movement towards its owner
// --------------------------------------------------

const forkOffsets = {

    0: {
        0: [-12, -12],
        1: [12, 12]
    },

    1: {
        1: [12, -12],
        2: [-12, 12]
    },

    2: {
        2: [12, 12],
        3: [-12, -12]
    },

    3: {
        3: [-12, 12],
        0: [12, -12]
    }

};


// --------------------------------------------------
// Display an event
// --------------------------------------------------

function showEvent() {

    if (index >= events.length) {

        pause();
        return;

    }

    const event = events[index];


    // Remove old neighbour animations

    for (let i = 0; i < 4; i++) {

        document.getElementById("p" + i)
            .classList.remove("leaning");

    }


    // Update philosopher positions and states

    for (let i = 0; i < 4; i++) {

        const p = document.getElementById("p" + i);

        const state = event.philosophers[i];

        const held = event.held_forks[i];

        const requested = event.waiting_for[i];

        // Keep the original internal state for CSS styling.
        p.className =
            "philosopher " + state.toLowerCase();

        // Use readable labels in the visualization.
        let label = "P" + i + "<br>" +
            (displayState[state] || state);

        if (requested !== null) {

            label +=
                `<br><small class="wait-label">
                Blocked by F${requested}
                </small>`;

        }

        if (held.length > 0) {

            label +=
                `<br><small class="hold-label">
                Holding ${held.map(f => "F" + f).join(", ")}
                </small>`;

        }

        p.innerHTML = label;

    }


    // When a philosopher waits, their two neighbours
    // visually lean towards them.

    for (let i = 0; i < 4; i++) {

        if (event.philosophers[i] === "Waiting") {

            const left = (i + 3) % 4;
            const right = (i + 1) % 4;

            document.getElementById("p" + left)
                .classList.add("leaning");

            document.getElementById("p" + right)
                .classList.add("leaning");

        }

    }


    // Update fork ownership

    for (let i = 0; i < 4; i++) {

        const f = document.getElementById("f" + i);

        const owner = event.forks[i];

        if (owner === null) {

            f.innerHTML = "F" + i + "<br>Free";

            f.classList.remove("held");

            f.style.transform = "";

        } else {

            f.innerHTML =
                "F" + i + "<br>Held by P" + owner;

            f.classList.add("held");

            const offset = forkOffsets[i][owner];

            f.style.transform =
                `translate(${offset[0]}px, ${offset[1]}px) scale(1.1)`;

        }

    }


    // Update status panel

    document.getElementById("statuses").innerHTML =
        event.philosophers.map((state, i) => {

            const held = event.held_forks[i];

            const requested = event.waiting_for[i];

            let details = "";

            if (requested !== null) {

                details +=
                    `<br><span class="wait-label">
                    Blocked by F${requested}
                    </span>`;

            }

            if (held.length > 0) {

                details +=
                    `<br><span class="hold-label">
                    Holding: ${held.map(f => "F" + f).join(", ")}
                    </span>`;

            }

            return `
                <div class="status">
                    <strong>P${i}</strong> :
                    ${displayState[state] || state}
                    ${details}
                </div>
            `;

        }).join("");


    // Update fork panel

    document.getElementById("forkInfo").innerHTML =
        event.forks.map((owner, i) => {

            return `
                <div class="status">
                    <strong>F${i}</strong> :
                    ${
                        owner === null
                        ? "Free"
                        : "Held by P" + owner
                    }
                </div>
            `;

        }).join("");


    // Append event to log

    const log = document.getElementById("log");

    const entry = document.createElement("p");

    entry.textContent =
        "Event " + event.step + ": " + event.message;

    log.appendChild(entry);

    log.scrollTop = log.scrollHeight;


    // Event counter

    index++;

    document.getElementById("counter").innerText =
        "Event " + index + " of " + events.length;

}


// --------------------------------------------------
// Play
// --------------------------------------------------

function play() {

    if (timer !== null) return;

    if (index >= events.length) return;

    timer = setInterval(() => {

        if (index >= events.length) {

            pause();
            return;

        }

        showEvent();

    }, speed);

}


// --------------------------------------------------
// Pause
// --------------------------------------------------

function pause() {

    if (timer !== null) {

        clearInterval(timer);

        timer = null;

    }

}


// --------------------------------------------------
// Next event
// --------------------------------------------------

function next() {

    pause();

    showEvent();

}


// --------------------------------------------------
// Change speed (keeps playing if it was playing)
// --------------------------------------------------

function changeSpeed() {

    const wasPlaying = (timer !== null);

    pause();

    speed = Number(
        document.getElementById("speed").value
    );

    if (wasPlaying) {

        play();

    }

}


// --------------------------------------------------
// Reset screen
// --------------------------------------------------

function renderInitial() {

    for (let i = 0; i < 4; i++) {

        const p = document.getElementById("p" + i);

        p.className = "philosopher thinking";

        p.innerHTML =
            "P" + i + "<br>Thinking";

        const f = document.getElementById("f" + i);

        f.innerHTML =
            "F" + i + "<br>Free";

        f.classList.remove("held");

        f.style.transform = "";

    }

    document.getElementById("statuses").innerHTML = "";

    document.getElementById("forkInfo").innerHTML = "";

    document.getElementById("log").innerHTML = "";

    document.getElementById("counter").innerText =
        "Event 0 of " + events.length;

}


// --------------------------------------------------
// Restart
// --------------------------------------------------

function restart() {

    pause();

    index = 0;

    renderInitial();

}


// Initial display

renderInitial();

</script>

</body>

</html>
"""

    # Insert the actual recorded execution events
    html = html.replace(
        "__EVENT_DATA__",
        json.dumps(events)
    )

    file_path = output_folder / "dining_philosophers_monitor.html"

    # Overwrite the same HTML file every run
    file_path.write_text(html, encoding="utf-8")

    print("\nHTML visualization generated.")
    print("File:", file_path.resolve())

    return file_path


# ============================================================
# 7. AUTOMATIC BROWSER OPENING
# ============================================================

def open_visualization(file_path):

    # The execution events are embedded in the HTML page.
    file_url = file_path.resolve().as_uri()

    print("\nOpening visualization in browser...")
    webbrowser.open(file_url)
    print("Visualization file:", file_path.resolve())


def run_philosopher(number):

    try:
        philosopher(number)

    except Exception as exc:

        with monitor.lock:
            worker_errors.append((number, exc))

        print(
            f"WORKER ERROR: Philosopher P{number} raised "
            f"{type(exc).__name__}: {exc}"
        )


# ============================================================
# 8. MAIN PROGRAM
# ============================================================

if __name__ == "__main__":

    print("\n")
    print("=" * 60)
    print("DINING PHILOSOPHERS - MONITOR SOLUTION")
    print("=" * 60)

    print("\nNumber of philosophers:", N)
    print("Number of forks:", N)
    print("Meals per philosopher:", ROUNDS)

    print("\nDeadlock prevention:")
    print("Both forks are taken atomically inside the monitor.")

    print("\nStarvation mitigation:")
    print("Ticket order: no philosopher eats ahead of an older hungry neighbour.")

    print("\nStarting philosopher threads...\n")

    threads = []

    for i in range(N):

        thread = threading.Thread(
            target=run_philosopher,
            args=(i,),
            name=f"Philosopher-{i}",
            daemon=True
        )

        threads.append(thread)
        thread.start()

    # One overall deadline, not 60 seconds per thread.
    deadline = time.monotonic() + 60

    for thread in threads:

        remaining = max(0, deadline - time.monotonic())

        thread.join(timeout=remaining)

    # Report worker exceptions before reporting a possible hang.
    if worker_errors:

        details = "; ".join(
            f"P{number}: {type(exc).__name__}: {exc}"
            for number, exc in worker_errors
        )

        raise RuntimeError(
            "One or more philosopher threads failed: " + details
        )

    still_running = [
        thread.name
        for thread in threads
        if thread.is_alive()
    ]

    if still_running:

        raise TimeoutError(
            "DEADLOCK OR HANG SUSPECTED: these threads did not finish "
            "within 60 seconds: " + ", ".join(still_running)
        )

    final_verification()

    html_file = generate_html()

    open_visualization(html_file)

    print("\nAll philosophers completed their meals successfully.")
