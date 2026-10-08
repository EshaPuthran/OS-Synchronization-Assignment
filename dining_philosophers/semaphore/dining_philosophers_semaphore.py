
import threading
import time
import random
import json
import webbrowser
from pathlib import Path


# ============================================================
# FAIR FIFO SEMAPHORE
# ============================================================

class FairSemaphore:
    """A semaphore that grants permits in FIFO ticket order."""

    def __init__(self, permits=1):
        if permits < 1:
            raise ValueError("A semaphore must have at least one permit.")

        self.permits = permits
        self.max_permits = permits
        self.condition = threading.Condition()
        self.next_ticket = 0
        self.serving_ticket = 0

    def acquire(self):
        """Wait for this request's FIFO turn and return its ticket."""
        with self.condition:
            ticket = self.next_ticket
            self.next_ticket += 1

            while ticket != self.serving_ticket or self.permits == 0:
                self.condition.wait()

            self.permits -= 1
            self.serving_ticket += 1
            self.condition.notify_all()
            return ticket

    def release(self):
        """Return one permit and wake waiting requesters."""
        with self.condition:
            if self.permits >= self.max_permits:
                raise RuntimeError("FairSemaphore released too many times.")

            self.permits += 1
            self.condition.notify_all()


# ============================================================
# 1. CONFIGURATION
# ============================================================

N = 4                       # Exactly four philosophers
ROUNDS = 3                  # Meals per philosopher


# Shared resources: Four forks (F0, F1, F2, F3).
# Each fork is protected by a binary FIFO semaphore.
forks = [FairSemaphore(1) for _ in range(N)]

# Deadlock prevention:
# At most N-1 philosophers may compete for the forks.
room = FairSemaphore(N - 1)


# A philosopher enters the eating section only after
# successfully acquiring both adjacent forks.

event_lock = threading.Lock()

philosopher_states = ["Thinking"] * N
fork_owners = [None] * N
held_forks = [[] for _ in range(N)]
waiting_for = [None] * N

rounds_completed = [0] * N

events = []

# Waiting-time statistics
total_wait_time = [0.0] * N
maximum_wait_time = [0.0] * N

# Number of successful acquisitions of each fork
fork_acquisition_count = [0] * N

# Room occupancy verification
in_room = 0
max_in_room = 0

# Exceptions raised by philosopher worker threads
worker_errors = []


# ============================================================
# 2. MUTUAL EXCLUSION AND STATE VERIFICATION
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

            # F0 is shared by P0 and P1.
            # F1 is shared by P1 and P2.
            # F2 is shared by P2 and P3.
            # F3 is shared by P3 and P0.

            allowed_owners = (
                fork_number,
                (fork_number + 1) % N
            )

            if owner not in allowed_owners:
                raise RuntimeError(
                    f"INVALID OWNERSHIP: "
                    f"P{owner} cannot own F{fork_number}"
                )

            if held_forks[owner].count(fork_number) != 1:
                raise RuntimeError(
                    f"OWNERSHIP RECORD ERROR: "
                    f"F{fork_number} and P{owner}"
                )

    for philosopher in range(N):

        for fork_number in held_forks[philosopher]:

            if fork_owners[fork_number] != philosopher:
                raise RuntimeError(
                    f"INCONSISTENT OWNERSHIP: "
                    f"P{philosopher}, F{fork_number}"
                )


# ============================================================
# 3. EVENT RECORDING
# ============================================================

def add_event_locked(number, state, message):
    """
    Record a complete snapshot of the current execution.

    This function must be called while event_lock is held.
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
        "held_forks": [
            item.copy() for item in held_forks
        ],
        "waiting_for": waiting_for.copy(),
        "rounds": rounds_completed.copy()
    }

    events.append(event)

    print(message)


def record_event(number, state, message):

    with event_lock:
        add_event_locked(number, state, message)


# ============================================================
# 4. REQUEST A FORK
# ============================================================

def request_fork(number, fork_number):

    with event_lock:

        philosopher_states[number] = "Waiting"
        waiting_for[number] = fork_number

        verify_shared_state()

        event = {
            "step": len(events) + 1,
            "philosopher": number,
            "state": "Waiting",
            "message": (
                f"P{number} is WAITING for F{fork_number} "
                f"while holding {held_forks[number]}"
            ),
            "philosophers": philosopher_states.copy(),
            "forks": fork_owners.copy(),
            "held_forks": [
                item.copy() for item in held_forks
            ],
            "waiting_for": waiting_for.copy(),
            "rounds": rounds_completed.copy()
        }

        events.append(event)

        print(event["message"])


# ============================================================
# 5. ACQUIRE A FORK
# ============================================================

def acquire_fork(number, fork_number):
    """Acquire a fork in FIFO order and record exclusive ownership."""

    ticket = forks[fork_number].acquire()

    with event_lock:

        # Explicit mutual-exclusion verification
        if fork_owners[fork_number] is not None:

            # Return the permit before raising an error.
            forks[fork_number].release()

            raise RuntimeError(
                f"MUTUAL EXCLUSION VIOLATION: "
                f"F{fork_number} is already owned "
                f"by P{fork_owners[fork_number]}"
            )

        # Assign exclusive ownership
        fork_owners[fork_number] = number
        held_forks[number].append(fork_number)
        fork_acquisition_count[fork_number] += 1
        waiting_for[number] = None

        add_event_locked(
            number,
            "Holding",
            f"P{number} successfully acquired F{fork_number}. "
            f"FIFO ticket {ticket}. Exclusive ownership verified."
        )

        print(
            f"[MUTEX CHECK PASSED] F{fork_number} belongs only to P{number}"
        )


# ============================================================
# 6. RELEASE A FORK
# ============================================================

def release_fork(number, fork_number):

    with event_lock:

        # Only the current owner may release the fork
        if fork_owners[fork_number] != number:

            raise RuntimeError(
                f"INVALID RELEASE: P{number} attempted "
                f"to release F{fork_number}, "
                f"owned by P{fork_owners[fork_number]}"
            )

        # Release the binary semaphore
        forks[fork_number].release()

        # Clear ownership
        fork_owners[fork_number] = None

        if fork_number in held_forks[number]:
            held_forks[number].remove(fork_number)

        add_event_locked(
            number,
            "Releasing",
            f"P{number} released F{fork_number}. "
            f"Fork is now FREE."
        )

        print(
            f"[RELEASE VERIFIED] F{fork_number} is now FREE"
        )


# ============================================================
# 7. PHILOSOPHER THREAD
# ============================================================

def philosopher(number):
    """
    Each philosopher repeatedly:
    THINK -> HUNGRY -> ACQUIRE TWO FORKS ->
    EAT -> RELEASE FORKS.

    Fork allocation:

    P0 uses F3 and F0
    P1 uses F0 and F1
    P2 uses F1 and F2
    P3 uses F2 and F3
    """

    global in_room, max_in_room

    left_fork = (number - 1) % N
    right_fork = number

    for round_number in range(ROUNDS):

        # ------------------------------------------------
        # THINKING
        # ------------------------------------------------

        record_event(
            number,
            "Thinking",
            f"P{number} is THINKING"
        )

        time.sleep(random.uniform(0.8, 1.5))

        # ------------------------------------------------
        # HUNGRY
        # ------------------------------------------------

        record_event(
            number,
            "Hungry",
            f"P{number} is HUNGRY "
            f"(Round {round_number + 1})"
        )

        start_wait = time.monotonic()

        # ------------------------------------------------
        # DEADLOCK PREVENTION
        # ------------------------------------------------

        # At most three philosophers may compete for forks.
        # This prevents all four philosophers from holding
        # one fork and waiting for another simultaneously.

        record_event(
            number,
            "Hungry",
            f"P{number} is REQUESTING room access (FIFO queue)."
        )

        room_ticket = room.acquire()

        acquired = []
        entered_room = False

        try:

            # Measure room occupancy before acquiring either fork.
            with event_lock:

                in_room += 1
                entered_room = True
                max_in_room = max(max_in_room, in_room)

                if in_room > N - 1:
                    raise RuntimeError(
                        f"ROOM CAPACITY VIOLATION: {in_room} philosophers "
                        f"entered; maximum allowed is {N - 1}."
                    )

                add_event_locked(
                    number,
                    "Hungry",
                    f"P{number} ENTERED the room. "
                    f"Room FIFO ticket: {room_ticket}. "
                    f"Current room occupancy: {in_room}/{N - 1}."
                )

            # --------------------------------------------
            # ACQUIRE FIRST FORK
            # --------------------------------------------

            request_fork(number, left_fork)

            acquire_fork(number, left_fork)

            acquired.append(left_fork)

            # --------------------------------------------
            # ACQUIRE SECOND FORK
            # --------------------------------------------

            request_fork(number, right_fork)

            acquire_fork(number, right_fork)

            acquired.append(right_fork)

            # Both forks have now been acquired.
            # Record the waiting time.

            elapsed = time.monotonic() - start_wait

            with event_lock:

                total_wait_time[number] += elapsed

                maximum_wait_time[number] = max(
                    maximum_wait_time[number],
                    elapsed
                )

            # --------------------------------------------
            # EATING
            # --------------------------------------------

            record_event(
                number,
                "Eating",
                f"P{number} acquired F{left_fork} "
                f"and F{right_fork}. "
                f"P{number} is EATING."
            )

            time.sleep(random.uniform(0.8, 1.5))

            # --------------------------------------------
            # RELEASING
            # --------------------------------------------

            record_event(
                number,
                "Releasing",
                f"P{number} is RELEASING both forks"
            )

        finally:

            # Always release acquired forks,
            # even if an exception occurs.

            for fork_number in reversed(acquired):
                release_fork(number, fork_number)

            # Decrement occupancy before releasing the room permit.
            if entered_room:

                with event_lock:

                    in_room -= 1

                    if in_room < 0:
                        raise RuntimeError(
                            "ROOM OCCUPANCY ERROR: occupancy became negative."
                        )

                    add_event_locked(
                        number,
                        "Thinking",
                        f"P{number} LEFT the room after releasing both forks. "
                        f"Current room occupancy: {in_room}/{N - 1}."
                    )

            room.release()

        # ------------------------------------------------
        # THINKING AGAIN
        # ------------------------------------------------

        with event_lock:

            rounds_completed[number] = round_number + 1

            add_event_locked(
                number,
                "Thinking",
                f"P{number} completed round "
                f"{round_number + 1}"
            )

    # ----------------------------------------------------
    # FINISHED
    # ----------------------------------------------------

    record_event(
        number,
        "Finished",
        f"P{number} has FINISHED all meals"
    )


# ============================================================
# 8. FINAL VERIFICATION
# ============================================================

def final_verification():

    print("\n")
    print("=" * 60)
    print("FINAL SYNCHRONIZATION VERIFICATION")
    print("=" * 60)

    with event_lock:

        verify_shared_state()

        all_forks_free = all(
            owner is None for owner in fork_owners
        )

        all_philosophers_finished = all(
            state == "Finished" for state in philosopher_states
        )

        print("\nPHILOSOPHER MEAL COMPLETION")

        for i in range(N):

            print(
                f"P{i}: Completed {rounds_completed[i]} meals"
            )

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

        print("\nROOM CAPACITY VERIFICATION")

        print(
            f"Maximum room occupancy observed: {max_in_room}"
        )

        print(
            f"Allowed maximum room occupancy: {N - 1}"
        )

        if in_room != 0:
            raise RuntimeError(
                "Room occupancy is not zero at program end."
            )

        if max_in_room > N - 1:
            raise RuntimeError(
                "Room capacity was exceeded."
            )

        print("\nWAITING TIME STATISTICS")

        for i in range(N):

            print(
                f"P{i}: Total waiting time = {total_wait_time[i]:.2f}s, "
                f"Maximum waiting time = {maximum_wait_time[i]:.2f}s"
            )

        if not all_forks_free:
            raise RuntimeError(
                "At least one fork is still held."
            )

        if not all_philosophers_finished:
            raise RuntimeError(
                "Not all philosophers finished."
            )

        print("\nALL FINAL CHECKS PASSED.")
        print("Mutual exclusion: PASSED")
        print("All philosophers completed the required meals: PASSED")
        print("Every fork was acquired exactly 2 * ROUNDS times: PASSED")
        print("All forks are free: PASSED")
        print("Room occupancy returned to zero: PASSED")
        print("Maximum room occupancy did not exceed N-1: PASSED")
        print("Deadlock prevention: N-1 admission strategy used.")

        print(
            "Starvation mitigation: FIFO room and fork semaphores "
            "serve requests in ticket order."
        )

        print(
            "Fairness assumption: threads continue to be scheduled and "
            "each meal/critical section eventually finishes."
        )

        print("=" * 60)


# ============================================================
# 9. GENERATE HTML VISUALIZATION
# ============================================================

def generate_html():

    output_folder = (
        Path(__file__).resolve().parent
        / "generated_html"
    )

    output_folder.mkdir(
        parents=True,
        exist_ok=True
    )

    html = r"""
<!DOCTYPE html>

<html lang="en">

<head>

<meta charset="UTF-8">

<meta name="viewport"
content="width=device-width, initial-scale=1.0">

<title>Dining Philosophers - Semaphore Simulation</title>

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
Semaphore-Based Synchronization |
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
                Waiting for F${requested}
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
                    Waiting for F${requested}
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
// Change speed
// --------------------------------------------------

function changeSpeed() {

    pause();

    speed = Number(
        document.getElementById("speed").value
    );

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

    file_path = (
        output_folder /
        "dining_philosophers_semaphore.html"
    )

    # Overwrite the same HTML file every run
    file_path.write_text(
        html,
        encoding="utf-8"
    )

    print("\nHTML visualization generated.")
    print("File:", file_path.resolve())

    return file_path


# ============================================================
# 10. AUTOMATIC BROWSER OPENING
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

        with event_lock:
            worker_errors.append((number, exc))

        print(
            f"WORKER ERROR: Philosopher P{number} raised "
            f"{type(exc).__name__}: {exc}"
        )


# ============================================================
# 11. MAIN PROGRAM
# ============================================================

if __name__ == "__main__":

    print("\n")
    print("=" * 60)
    print("DINING PHILOSOPHERS - SEMAPHORE SOLUTION")
    print("=" * 60)

    print("\nNumber of philosophers:", N)
    print("Number of forks:", N)
    print("Meals per philosopher:", ROUNDS)

    print("\nDeadlock prevention:")
    print("Maximum competing philosophers:", N - 1)

    print("\nStarvation mitigation:")
    print("Fair FIFO semaphores enabled for room and every fork.")

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

        remaining = max(
            0,
            deadline - time.monotonic()
        )

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
