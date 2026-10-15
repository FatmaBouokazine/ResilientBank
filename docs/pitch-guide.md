# ResilientBank — Pitch Guide

Read this before you demo. Nothing here needs a protocol name to make sense.

---

## The one sentence

**When you send money, the system writes it down and tells everyone who needs to know —
and if the messenger dies halfway, your money is still safe and the message gets
delivered when the messenger comes back. I can prove it by killing the messenger live.**

---

## The three things the teacher should walk away believing

### 1. "Their money can't disappear."

Every payment is recorded in a database before anything else happens. If you try to send
the same payment twice — double-click, retry, network glitch — you get the SAME payment
back, not two charges. The system recognizes "I've seen this before" and returns the
original. That is the difference between a bank and a spreadsheet.

**Say:** *"Try to pay twice with the same reference. You get one payment, not two. The
system remembers."*

### 2. "The message always gets delivered, eventually."

Recording the payment and telling the rest of the system are two separate acts, and the
danger is doing one without the other. We write both down together — the payment and a
note saying "tell everyone about this" — in a single indivisible step. A separate worker
reads those notes and delivers them. If the delivery worker dies, the notes pile up and
wait. Nothing is lost; it's just late.

**Say:** *"The payment and the notification are written as one action. You cannot have a
payment nobody knows about, or a notification about a payment that doesn't exist."*

### 3. "They proved it by breaking it."

Anyone can claim resilience. We kill the message broker in the middle of real payments and
show that every payment still goes through, every event still arrives, and the system
reports exactly what happened. The test suite that proves this runs automatically on every
code change.

**Say:** *"Watch: I stop the broker now. Payments still succeed. I restart it. Every
pending message is delivered. Nothing lost, nothing duplicated."*

---

## What NOT to say

| Don't say | Say instead |
|---|---|
| "Kafka", "outbox", "idempotency key" | "the messenger", "the to-do list", "the payment reference" |
| "gRPC", "protobuf" | "the payment service" |
| "SKIP LOCKED", "two-phase", "acks=all" | "claims work without stepping on each other", "written together", "confirmed by every copy" |
| "ArgoCD", "GitOps", "Terraform" | "the cluster updates itself from our code", "infrastructure as versioned code" |
| "chaos engineering" | "we break it on purpose to prove it survives" |

The teacher grades understanding, not vocabulary. Every technical term you use is a chance
to lose them; every plain sentence is a chance to convince them.

---

## The 5-minute demo

Do these in order. Each step has something visible to point at.

### Minute 0–1: It works

Create a payment through the service. Show the returned payment ID.

> *"This is a payment. It has an ID. It exists."*

### Minute 1–2: Duplicates are impossible

Send the exact same payment again (same reference). Show the SAME ID comes back.

> *"Same reference, same payment. Try to double-charge me — you can't."*

### Minute 2–4: Kill the messenger

Stop the message broker. Create three more payments — all succeed. Show the pending
count growing: the system is holding the messages, not dropping them.

> *"The broker is dead. Payments still go through. See the queue growing? Those messages
> are waiting, not lost."*

Restart the broker. Watch the queue drain to zero.

> *"Broker's back. Queue draining. Every message delivered."*

### Minute 4–5: The proof is automatic

Show the test suite output: the atomicity tests, the relay tests, the smoke suite. Point
at the one that says a failed outbox insert rolls the payment back.

> *"This runs on every change. If someone breaks the guarantee, the build fails before it
> can reach anyone."*

---

## If they ask hard questions

**"What if two payments arrive at the exact same millisecond?"**
The database serializes them. The second one sees the first and takes the duplicate path.
There is a test with 8 concurrent requests proving exactly one payment and one event.

**"What if the broker is down for an hour?"**
Payments keep succeeding. The backlog grows. When the broker returns, it drains. The only
cost is delay, never loss.

**"What if the event can never be delivered?"**
After 10 attempts it goes to a separate dead-letter queue with the original message intact
and the reason attached, for a human to triage. It is never silently dropped.

**"What about the database itself going down?"**
Payments fail fast with a clear error — no half-written state, because the payment and
its notification are one atomic action. When the database returns, normal operation
resumes with nothing to reconcile.

**"How do you know the tests aren't just passing vacuously?"**
We mutate the code — break the guarantee on purpose — and confirm the tests go red.
A test that can't fail proves nothing.
