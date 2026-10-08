# AI Prompts Used During the Assignment

## Purpose of AI Usage

AI was used as a learning and implementation support tool during this
assignment. The prompts were primarily used to understand operating
system synchronization concepts, compare possible implementation
approaches, plan the solutions, debug implementation issues, and
understand the generated visualizations.

The final programs were reviewed, tested, modified and validated by
the student.

---

# 1. Readers-Writers Problem

## Understanding the Problem

### Prompt 1
Explain the Readers-Writers synchronization problem in simple terms.
What is the actual problem that needs to be solved, what are the
possible conflicts between readers and writers, and why is
synchronization required?

### Prompt 2
Explain the Readers-Writers problem using a real-world example.
Clearly explain why multiple readers can sometimes access a shared
resource simultaneously, while a writer generally requires exclusive
access.

### Prompt 3
What are the main synchronization requirements that a correct
Readers-Writers solution should satisfy? Explain mutual exclusion,
reader concurrency, writer exclusivity, starvation and fairness.

---

## Exploring Possible Implementations

### Prompt 4
What are the different ways in which the Readers-Writers problem can
be implemented using synchronization mechanisms? Compare semaphores,
mutexes and monitors conceptually and explain the advantages and
limitations of each approach.

### Prompt 5
For a Python implementation of the Readers-Writers problem, how can
semaphores and condition variables be used? Explain the role of each
synchronization object before discussing any code.

### Prompt 6
How can a monitor be designed for the Readers-Writers problem?
Explain what state variables, locks and condition variables would be
required and how readers and writers would be allowed to enter and
leave the critical section.

---

## Planning the Solution

### Prompt 7
Help me design the logic for a Readers-Writers solution using
semaphores. Do not directly provide the complete code. First explain
the sequence of operations for a reader and a writer, which shared
variables are required, and where synchronization is needed.

### Prompt 8
Help me design the logic for a Readers-Writers solution using a
monitor. Explain the monitor state, waiting conditions and how the
monitor decides whether a reader or writer can proceed.

### Prompt 9
How can starvation be prevented in a Readers-Writers implementation?
Explain how FIFO ordering or fair admission can be incorporated into
the synchronization design.

---

# 2. Dining Philosophers Problem

## Understanding the Problem

### Prompt 10
Explain the Dining Philosophers problem in simple terms. What are the
processes, what are the resources, and why can deadlock occur?

### Prompt 11
Explain the four necessary conditions for deadlock and show how they
can appear in the Dining Philosophers problem.

### Prompt 12
What is starvation in Dining Philosophers? Explain how starvation can
occur even when deadlock is prevented.

---

## Exploring Possible Implementations

### Prompt 13
What are the possible ways to solve the Dining Philosophers problem
using semaphores? Explain approaches for preventing deadlock and
reducing starvation, without directly writing the complete program.

### Prompt 14
How can a monitor and condition variables be used to implement Dining
Philosophers? Explain how the monitor can control access to the two
forks required by each philosopher.

### Prompt 15
Compare a semaphore-based solution and a monitor-based solution for
Dining Philosophers. Explain how each mechanism handles resource
allocation, mutual exclusion, deadlock and starvation.

---

## Planning the Solution

### Prompt 16
Help me design a Dining Philosophers solution for exactly four
philosophers and four forks. Explain the data structures and
synchronization logic that should be used before implementation.

### Prompt 17
How can a solution prevent the situation where every philosopher
holds one fork and waits forever for the other fork?

### Prompt 18
How can fairness or FIFO ordering be incorporated into Dining
Philosophers so that a philosopher that has been waiting for a long
time is not continuously bypassed?

---

# 3. Understanding the Python Synchronization Mechanisms

### Prompt 19
Explain the difference between a Python Semaphore, Lock and
Condition. When would each one be appropriate in an operating system
synchronization problem?

### Prompt 20
Explain how Python threads interact with synchronization primitives.
What happens when a thread calls acquire(), waits on a condition, and
later continues execution?

### Prompt 21
Explain why condition.wait() releases the associated lock while the
thread is waiting and reacquires it before continuing.

### Prompt 22
Explain the difference between protecting a shared variable and
controlling access to a critical section. Give examples using the
Readers-Writers and Dining Philosophers problems.

---

# 4. Implementation Guidance

### Prompt 23
Based on the synchronization design, explain how I should convert the
logic into a Python threaded implementation. Focus on the order of
operations, shared state and synchronization points rather than
giving the complete code immediately.

### Prompt 24
What are the common implementation mistakes when converting a
Readers-Writers or Dining Philosophers algorithm into Python threads?

### Prompt 25
What should I verify after implementing a synchronization algorithm?
Give me a checklist covering race conditions, deadlock, starvation,
mutual exclusion, concurrency and correct resource release.

---

# 5. Debugging and Verification

### Prompt 26
My synchronization program is running, but I want to verify whether
the synchronization logic is actually correct. What evidence should I
look for in the execution output?

### Prompt 27
How can I verify that multiple readers are allowed to execute
concurrently while a writer has exclusive access?

### Prompt 28
How can I verify that two neighbouring philosophers never use the
same fork simultaneously?

### Prompt 29
What types of output messages or runtime checks can be added to a
synchronization program to verify that resources are acquired and
released correctly?

### Prompt 30
Review the synchronization logic I implemented and identify possible
race conditions, deadlocks or starvation issues. Explain the reason
for each issue and suggest how the design can be improved.

---

# 6. HTML Visualization

### Prompt 31
I want to visualize the execution of a synchronization problem in
HTML. What information should the visualization display so that it
clearly demonstrates the synchronization concepts rather than just
showing an animation?

### Prompt 32
For Readers-Writers, what states should be shown in an HTML
visualization to demonstrate reader concurrency, writer exclusivity,
waiting threads, queue order and resource access?

### Prompt 33
For Dining Philosophers, what information should an HTML visualization
show to clearly demonstrate philosopher states, fork ownership,
waiting, eating, thinking and synchronization?

### Prompt 34
How can I connect the actual events from the Python program to an HTML
visualization so that the visualization represents the real execution
rather than a separately simulated animation?

### Prompt 35
Review my visualization design and suggest improvements that would
make the synchronization behaviour easier to understand during a
demonstration or viva.

---

# 7. Understanding and Improving the Implementation

### Prompt 36
Explain the implementation step-by-step in terms of the operating
system synchronization concepts. For each synchronization object,
explain why it is required and what could happen if it were removed.

### Prompt 37
Explain the complete execution flow of the Readers-Writers program
from the point when a thread requests access until it leaves the
critical section.

### Prompt 38
Explain the complete execution flow of the Dining Philosophers
program from requesting forks to eating and releasing the forks.

### Prompt 39
Help me identify whether the implementation actually demonstrates
the concepts required by the assignment, including synchronization,
mutual exclusion, deadlock prevention and starvation prevention.

---

# 8. Viva Preparation

### Prompt 40
Prepare conceptual viva questions that could be asked about the
Readers-Writers problem using semaphores and monitors. Include the
reasoning behind each answer.

### Prompt 41
Prepare conceptual viva questions about Dining Philosophers using
semaphores and monitors. Focus on deadlock, starvation, mutual
exclusion and resource allocation.

### Prompt 42
Explain why a monitor-based solution is different from a
semaphore-based solution even when both solve the same synchronization
problem.

### Prompt 43
Give me a simple explanation of the synchronization mechanisms used in
my implementations that I can use during a viva without memorizing
code.

---

# 9. Final Review

### Prompt 44
Review the overall design of the four synchronization
implementations:
1. Readers-Writers using Semaphores
2. Readers-Writers using Monitors
3. Dining Philosophers using Semaphores
4. Dining Philosophers using Monitors

For each one, explain the synchronization mechanism, critical
section, possible deadlock situation, starvation concern and the
method used to address it.

### Prompt 45
Give me a final checklist to verify before submitting an operating
systems synchronization assignment. Include code correctness,
execution testing, visualization, screenshots, documentation,
references, GitHub organization and AI usage disclosure.
