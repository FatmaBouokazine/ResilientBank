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

## Technical details: how the system really works

The rest of this document explains the project without using technical words. This
section explains what the project actually *builds*, in slightly more detail — the
words you need are listed at the end, and every sentence is written so that anybody
can follow it.

### The payment service

This is the part of the system where a customer sends a payment. It is a program
written in the programming language **Go**.

When a customer sends a payment, the service does two things, in this order:

1. **Look quickly for a duplicate.** It keeps a very small, very fast table (called
   Redis) that remembers the last requests it has handled. If the exact same request
   arrives again — the same reference, the same amount — the service already knows
   this payment. It then answers: "This payment was already done", and it does not
   charge anybody twice.
2. **Write the payment into the ledger.** If the request is new, the payment is written
   into the ledger (see below). Only after it is safely written does the customer get an
   answer: "Payment accepted."

If Redis itself is broken, step 1 cannot run — but that is all right. The system keeps
working: step 2 still protects against duplicates, because the ledger itself refuses
two payments with the same reference. This is deliberate: **Redis is only a helper. The
ledger is the boss.**

### The ledger (PostgreSQL)

The ledger is a database — a very organised notebook that is stored permanently.
The database is called **PostgreSQL**. Every payment ever accepted is written there and
is never deleted, even if the rest of the system is switched off.

Two rules make the ledger trustworthy:

- A payment is written with its payment ID and all details (who, what, how much).
- There is exactly one rule that matters: the system refuses to store two payments
  under the same "reference name". So a duplicate can never be written, not even by
  two computers that happen to send it at the same moment.

### The messenger (Kafka) and the waiting list

After a payment is written into the ledger, something must tell the rest of the bank
about it — for example, to update reports or other computers. This is done with a
**message** (sometimes called an *event*).

- **Kafka** is the messenger. It is a server whose job is carrying messages from the
  bank to other programs. Think of it as a very reliable post office.
- When a payment is written, a message about it is placed in a **waiting list**
  (called the *outbox* — an "outbox" is a box where outgoing letters wait to be sent).
- A separate small worker, called the **relayer**, reads that list and delivers the
  messages. It runs all the time, but completely separately from the payment service.

Why the two-step design? Imagine doing both jobs at once, in this order:

- Write the payment, *then* send the message. — If the computer breaks after writing
  but before sending, the payment exists but nobody is told about it. For a bank, that
  is unrecoverable.
- Send the message, *then* write the payment. — Even worse: the message tells the bank
  about a payment that was never made.

So the project writes the payment and its message **together**, in a single atomic
action. Both happen, or neither happens. Only then does the message leave the
waiting list. That is why the waiting list exists, and why delivery can wait.

If the messenger breaks down, messages simply stay in the waiting list and are
delivered later — payments are never lost, they are only slightly delayed. If
delivery is stopped for a long time, the list grows; the system reports this, and a
human decides what to do with the oldest messages (they are moved aside, never thrown
away).

### How the system is observed

- **Prometheus** is the measuring machine. It collects numbers about the running
  system: how fast payments are handled, whether anything is late, and so on.
- **Grafana** is the chart drawing machine. It turns those numbers into pictures
  (dashboards). Both are part of the project and are switched on by `make up-all`.
- **The dashboard and status page** are the human-readable pages. The status page uses
  plain words and no technical terms, so anybody can read it. It refreshes itself every
  three seconds and shows: "Everything processed. Nothing waiting, nothing lost."
- If a part of the system is not answering, the pages show it with a red mark — so a
  broken part is visible immediately.

### Planned next steps

The project is still being built, in a sequence of work packages called sprints:

- **More regions.** At the moment everything runs on one computer ("Region A"). Connecting several regions is the next big step, so the bank could run in more than one place.
- **Machine learning.** A separate service that looks at the measured numbers and tries to *predict* problems, for example "something unusual is happening". It is being developed separately.
- **Compliance reports.** Automatic reports about how fast the bank reacts to problems, and about following banking rules.
- **Full cluster deployment.** Running the same system on a big server cluster, with automatic updates (the scripts for this already exist).

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

## A scenario from the test suite (you can tell this story)

The document above describes a demo you can do by hand. This section describes a
**test that runs by itself** — the kind that proves the system without anybody
watching. Telling this story is a good way to show the teacher that the project
does not merely claim to be safe: it proves it, automatically, on every change.

The story starts on a normal working day. A developer has just changed one small
part of the system, and the project wants to know: *"did we break anything?"*

**The story:**

1. **Connect and send a payment.** The test opens the running system, just like a
   customer would. It sends a payment for a small amount and receives the payment ID.
   The teacher can picture this as: *"The customer paid. The bank accepted it."*
2. **Send the very same payment again.** The test sends exactly the same payment —
   the same reference name, the same amount — a second time. The system recognises
   the duplicate. It answers: "This payment was already done" and does **not** charge
   the account twice. *"Nobody was charged twice."*
3. **Reuse the same reference with different details.** The test sends the same
   reference name, but with a bigger amount. The system refuses it immediately,
   because it knows the reference belongs to the first payment. *"A payment reference
   is not a toy — it is protected."*
4. **Break the messenger on purpose.** Now the interesting part: the test deliberately
   stops the messenger server (Kafka) in the middle of real payments. Payments keep
   being accepted — because ingestion does not wait for the messenger. But watch the
   *waiting list*: it grows, because the messages cannot be delivered while the
   messenger is down. Nothing is lost, it is only waiting.
5. **Bring the messenger back.** The test switches the messenger on again. The waiting
   list starts to empty, message by message. When it reaches zero, the test reports:
   *"Every message was delivered."*
6. **Check that the system noticed everything.** Finally, the test asks the measuring
   machines: *"how fast were payments handled? was anything late? were any messages
   lost?"* The charts and the plain-language pages report all of this, and the test
   fails if any guarantee is missing. The test therefore ends with a clear answer:
   *"The system is safe."*

That last step matters, because a test that cannot fail proves nothing — the project
deliberately breaks its own code to make sure the tests really catch mistakes. And the
most important part of the story is step 4: the system must survive being broken, and
that is exactly what the project demonstrates live.

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
