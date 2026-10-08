# OS-Synchronization-Assignment


## Overview

This project implements and demonstrates important operating system synchronization problems using Python threads and synchronization mechanisms.

The assignment focuses on two classical synchronization problems:

1. **Readers-Writers Problem**
2. **Dining Philosophers Problem**

Each problem is implemented using two different synchronization approaches:

* **Semaphores**
* **Monitors**

The project also includes HTML-based visualizations to demonstrate the synchronization behaviour during execution.

---

## Objectives

The main objectives of this assignment are:

* To understand the need for process and thread synchronization.
* To understand critical sections and mutual exclusion.
* To study the use of semaphores for synchronization.
* To understand monitor-based synchronization using locks and condition variables.
* To implement the Readers-Writers synchronization problem.
* To implement the Dining Philosophers synchronization problem.
* To identify and address deadlock and starvation.
* To demonstrate synchronization behaviour through HTML visualizations.
* To test and verify the correctness of the implementations.

---

# Problems Implemented

## 1. Readers-Writers Problem

The Readers-Writers problem involves multiple threads accessing a shared resource.

### Synchronization Requirements

* Multiple readers should be able to read concurrently.
* A writer should have exclusive access to the shared resource.
* A reader should not access the resource while a writer is writing.
* Synchronization should prevent race conditions.
* Fairness should be considered to reduce the possibility of starvation.

### Implementations

The Readers-Writers problem is implemented using:

* Semaphore-based synchronization
* Monitor-based synchronization

### Readers-Writers Using Semaphores

The semaphore implementation uses synchronization primitives to control access to the shared resource and coordinate readers and writers.

The implementation also considers fair access so that waiting threads are not continuously bypassed.

### Readers-Writers Using Monitors

The monitor implementation uses a monitor lock and condition variables to control access.

A waiting mechanism is used to maintain an ordered access policy for readers and writers.

The monitor ensures that:

* Multiple readers can be active when no writer is active.
* Only one writer can be active at a time.
* Readers and writers are coordinated through condition variables.
* Waiting threads are handled according to the synchronization policy.

---

# 2. Dining Philosophers Problem

The Dining Philosophers problem demonstrates synchronization when multiple processes or threads compete for limited shared resources.

In this implementation:

* There are **four philosophers**.
* There are **four forks**.
* Each philosopher requires two neighbouring forks to eat.
* Philosophers alternate between thinking, waiting and eating.

The implementation addresses synchronization, mutual exclusion, deadlock prevention and starvation prevention.

### Implementations

The Dining Philosophers problem is implemented using:

* Semaphore-based synchronization
* Monitor-based synchronization

### Dining Philosophers Using Semaphores

The semaphore implementation controls access to forks and coordinates philosophers competing for the shared resources.

The solution is designed to prevent the situation where all philosophers hold one fork and wait indefinitely for another fork.

Fairness mechanisms are also used to reduce starvation.

### Dining Philosophers Using Monitors

The monitor implementation uses a monitor lock and condition variables to coordinate access to the forks.

The monitor controls the conditions under which a philosopher can acquire both required forks and enter the eating state.

This provides synchronized resource allocation while addressing deadlock and starvation concerns.

---

# Synchronization Concepts Demonstrated

The project demonstrates the following operating system concepts:

* Thread synchronization
* Mutual exclusion
* Critical sections
* Race conditions
* Semaphores
* Monitors
* Locks
* Condition variables
* Resource allocation
* Deadlock prevention
* Starvation prevention
* Fairness
* Concurrent execution

---

# HTML Visualization

Each implementation generates an HTML visualization of the synchronization process.

The visualizations provide a graphical representation of the execution and help demonstrate how threads interact with shared resources.

The generated HTML files are stored in the `generated_html` folder.

### Visualizations Included

* Readers-Writers using Semaphores
* Readers-Writers using Monitors
* Dining Philosophers using Semaphores
* Dining Philosophers using Monitors

---

# Repository Structure

```text
OS-Synchronization-Assignment/
│
├── README.md
│
├── readers_writers/
│   ├── semaphore/
│   │   └── readers_writers_semaphore.py
│   │
│   └── monitor/
│       └── readers_writers_monitor.py
│
├── dining_philosophers/
│   ├── semaphore/
│   │   └── dining_philosophers_semaphore.py
│   │
│   └── monitor/
│       └── dining_philosophers_monitor.py
│
├── generated_html/
│   ├── readers_writers_semaphore.html
│   ├── readers_writers_monitor.html
│   ├── dining_philosophers_semaphore.html
│   └── dining_philosophers_monitor.html
│
├── screenshots/
│   ├── readers_writers_semaphore_terminal.png
│   ├── readers_writers_semaphore_visualization.png
│   ├── readers_writers_monitor_terminal.png
│   ├── readers_writers_monitor_visualization.png
│   ├── dining_philosophers_semaphore_terminal.png
│   ├── dining_philosophers_semaphore_visualization.png
│   ├── dining_philosophers_monitor_terminal.png
│   └── dining_philosophers_monitor_visualization.png
│
├── ai_prompts/
│   └── prompts.md
│
├── references/
│   └── references.md
│
├── report/
│   └── Assignment_Report.pdf
│
```

---

# Technologies Used

* Python
* Python `threading` module
* Semaphores
* Locks
* Condition variables
* HTML
* CSS
* JavaScript

---

# Execution

## Readers-Writers — Semaphore

Navigate to:

```text
readers_writers/semaphore/
```

Run:

```bash
python readers_writers_semaphore.py
```

---

## Readers-Writers — Monitor

Navigate to:

```text
readers_writers/monitor/
```

Run:

```bash
python readers_writers_monitor.py
```

---

## Dining Philosophers — Semaphore

Navigate to:

```text
dining_philosophers/semaphore/
```

Run:

```bash
python dining_philosophers_semaphore.py
```

---

## Dining Philosophers — Monitor

Navigate to:

```text
dining_philosophers/monitor/
```

Run:

```bash
python dining_philosophers_monitor.py
```

---

# Testing and Verification

Each implementation was executed and tested to verify the synchronization behaviour.

The testing focused on:

* Correct thread execution
* Mutual exclusion
* Correct resource acquisition and release
* Reader concurrency
* Writer exclusivity
* Correct fork allocation
* Deadlock prevention
* Starvation prevention
* Correct completion of all threads
* Correct generation of HTML visualizations

Execution screenshots and visualization screenshots are provided in the `screenshots` folder.

---

# AI Usage

AI was used as a learning and implementation support tool during the development of this assignment.

AI assistance was used for:

* Understanding synchronization concepts
* Exploring possible implementation approaches
* Comparing semaphores and monitors
* Planning synchronization logic
* Debugging implementation issues
* Understanding and improving HTML visualizations
* Reviewing synchronization behaviour
* Preparing for viva questions

The student reviewed, modified, tested and validated the final implementations.

Detailed prompts used during the development process are provided in:

```text
ai_prompts/prompts.md
```

---

# References

The main references used for understanding the concepts and implementation are provided in:

```text
references/references.md
```

---

# Conclusion

This project demonstrates the implementation of classical operating system synchronization problems using different synchronization mechanisms.

The Readers-Writers and Dining Philosophers problems were implemented using both semaphores and monitors. The implementations demonstrate how synchronization mechanisms can be used to control concurrent access to shared resources while addressing mutual exclusion, deadlock and starvation.

HTML visualizations are included to provide a clearer representation of the synchronization behaviour during execution.
