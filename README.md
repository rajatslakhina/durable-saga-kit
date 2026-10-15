# DurableSaga

**Your checkout is four network calls. The app can be killed between any two of them.**
Reserve stock, put a hold on the card, place the order, send the receipt. Each call changes something on a server. If iOS kills the app after the hold but before the order, the user has paid for nothing. If it is killed after the order but before the receipt, they have an order and no email. Retrying blindly charges them twice. Undoing blindly can void the payment on an order that already shipped.

DurableSaga is a Swift 6 library that runs multi-step side-effecting workflows on the client as **sagas**. Each one is backed by a write-ahead journal, so it survives process death, retries without double-applying, rolls back what can be rolled back, and pushes forward what cannot.

> **Demo app:** [durable-saga-demo-app](https://github.com/rajatslakhina/durable-saga-demo-app). Kill the app between "card authorized" and "order placed", relaunch, and watch it finish the order without charging twice.

---

## Why this matters

Most iOS code handles a multi-step operation as one `async` function with a `do/catch`. That works until one of three things happens, and all three happen in production every day:

1. **The process dies mid-sequence.** The OS reclaims memory, the user swipes the app away, a crash happens in unrelated code. Whatever was in local variables is gone. The server state is not.
2. **A response is lost.** A timeout does not mean "it didn't happen". It means "you don't know". Retrying without an idempotency key double-applies. Giving up without compensating leaks a hold.
3. **The user cancels.** Swift's structured cancellation stops the next `await`, which is exactly wrong halfway through a payment.

The usual fix is "do it on the server". Often that is right. But many apps cannot move the orchestration server-side: several third-party APIs, offline-first flows, on-device AI pipelines that call tools with real side effects, or a backend team that will not get to it this year. Then the client needs the same discipline a workflow engine has. That discipline is what this package implements, small enough to read in one sitting.

## The model

A saga definition is validated at construction to have the shape **`compensatable* pivot? retriable*`** (Garcia-Molina & Salem, 1987):

| Kind | Example | On failure |
|---|---|---|
| **compensatable** | reserve stock, authorize payment | roll back every attempted step, in reverse |
| **pivot** (point of no return) | place order | permanent error → roll back. Lost response → **park** (it may have committed) |
| **retriable** | send receipt, notify warehouse | retry. Never compensate. Park for an operator when retries run out |

```
          ┌───────────── forward ─────────────┐
  start → reserve(C) → authorize(C) → place(PIVOT) → receipt(R) → completed
              ▲             │  fail/cancel   │ commit
              └── undo ◄────┘ (reverse)      └─ cancel ignored, must finish

  Every arrow is a journaled event:  intent → effect → outcome
```

## Architecture

```
SagaDefinition ──validates──► C* P? R*         (pure, throws DefinitionError)
SagaState.apply(record)  ── the reducer: the only place an event is judged legal
SagaPlanner.next(state)  ── pure: what to do next (execute / compensate / park / finish)
SagaCoordinator (actor)  ── drives sagas; single writer per saga; fencing; retention
SagaJournal (protocol)   ── InMemoryJournal · FileJournal (CRC-checked lines, fsync per append, torn-tail repair)
RetryPolicy · Sleeper · RandomSource ── injected, so every timing path is testable
```

The load-bearing idea is that **live execution and crash recovery are the same code**. The coordinator runs every event through `SagaState.apply` *before* writing it to the journal, and applies it to memory only *after* the write succeeds. Recovery folds the same events through the same reducer. The two cannot disagree about what state a saga is in, because there is only one definition of state.

## Guarantees and how each is enforced

| Guarantee | Mechanism | Pinned by |
|---|---|---|
| **Write-ahead**: an effect never starts before its intent is durable | `stepIntent` is committed before `execute` is called | `testIntentIsDurableBeforeTheSideEffectStarts` |
| **At most once per step** (with a server that dedupes on keys) | idempotency key = `sagaID#step`, the same across retries *and* crashes | `testTransientFailuresRetry…`, `testCrashInTheDoubtWindow…` (both fail if the key includes the attempt number: see *Verification*) |
| **Compensate what you attempted, not what you saw succeed** | rollback covers every step with an intent; compensation undoes *by key* | `testAmbiguousFailureIsUndoneByKey`, `testCrashDuringTheFinalAttempt…` |
| **Never guess about the pivot** | the pivot is *in doubt* from its first attempt until it succeeds or is definitively rejected. That includes the backoff between retries and the time it is parked, not only while a request is open. Nothing rolls back while it is in doubt | `testPivotTransientExhaustionParks…`, `testCancelDuringPivotBackoff…`, `testCancelWhileParkedAtThePivot…`, `testPivotStaysInDoubtAfterATransientFailure` |
| **Must-finish once the pivot may have happened** | caller cancellation becomes a journaled *request*. Steps run in a task the caller does not own. `started` is journaled before the cancellation handler is installed, so a cancel from an already-cancelled caller is never lost | `testCallerCancellationIsShielded…`, `testCancelAfterCommitIsIgnored…`, `testCancelFromAnAlreadyCancelledCaller…` |
| **Single writer per saga** | one drive task per saga plus a FIFO per-saga lock around every append | `testConcurrentStartsWithOneKeyShareOneRun`, `testCancelArrivingDuringAnAppend…` |
| **No zombie writes** | epoch fencing. Each coordinator takes a larger epoch *before it reads anything*, and the journal rejects stale epochs | `testAZombieCoordinatorIsFencedOutBySuccessor`, `testAReadOnlySuccessorStillFences…`, `testSecondInstanceFencesTheFirst…` |
| **Memory never runs ahead of disk** | a journal error while recording an outcome propagates and is never misread as a step failure | `testFailedJournalWriteLeavesMemoryConsistentAndResumable` |
| **Bounded growth** | finished sagas are pruned in batches once the backlog exceeds `retainFinished` (default 100) by `max(1, retainFinished/4)`. A saga that cannot be loaded is skipped, not fatal. Retry budgets bound records per saga between operator revivals | `testRetentionBoundsTheJournal…`, `testAnUnloadableSagaDoesNotDisableRetention` |
| **Torn writes are survivable, corruption is not ignored** | per-line CRC-32. A bad *final* line is dropped (and physically truncated). A bad *middle* line is reported. A failed append truncates its own partial bytes, so it cannot become mid-log corruption | `CodecTests`, `testTornTailIsRepairedOnOpen` (the truncate-on-failed-append path is not unit-tested: a partial write cannot be injected portably) |

Nine of these mechanisms have been **mutation-checked**: break the mechanism and at least one test fails each time (see *Verification*).

## Design decisions and rejected alternatives

**Journal-as-truth, not "state object + save()".** Persisting a mutable `SagaState` snapshot after each step is simpler, but it creates a window where the snapshot and the server disagree, and it leaves no record of *how* the saga got there. An append-only log of intents and outcomes makes the in-doubt window explicit (an intent with no outcome) and makes recovery a fold. *Rejected:* Core Data/SwiftData entity per saga. It is the same snapshot problem, plus a framework dependency in the hottest path.

**Validate before write, apply after write.** The reducer rejects illegal histories, for example success without an intent, compensation after commit, or compensation out of reverse order. Running it before the durable write means the coordinator can never persist something recovery would reject. *Rejected:* trusting the coordinator's control flow. That works until the first refactor.

**Compensate attempted steps, not succeeded steps.** If `authorize` times out three times, you never saw a hold, but one may exist. Rolling back only observed successes leaks it. So every compensation runs by idempotency key and must treat "nothing to undo" as success. This is the most commonly missed rule in hand-rolled client sagas. The mutation test shows the suite catches its absence.

**The pivot parks on ambiguity.** A permanent pivot error ("out of stock") means nothing committed, so roll back. A *lost response* from the pivot could mean the order exists. Voiding the payment on a placed order is worse than asking a human, so the saga parks forward and `revive(_:)` retries the pivot with the same key. "In doubt" is a property of the history, not of an open request: it holds from the pivot's first attempt until it succeeds or is definitively rejected. An earlier version of this library checked only "is a pivot request open?", which let a cancel during the retry backoff void the payment. The independent review caught it. Three tests and a mutation run now pin the wider definition. *Rejected:* treating exhaustion as failure. That is the bug this library exists to prevent.

**Cancellation is a request, not an interrupt.** Swift's structured cancellation would interrupt a step mid-request. That is the wrong default for a payment. The caller's cancellation is translated into a journaled `cancelRequested`. If the pivot has not been attempted, it rolls back at the next decision point. Once the pivot is in doubt or committed, it is recorded and reported as `cancellationIgnored: true`. Steps run inside a drive task the caller does not own, so the in-flight request is never cut off. This back-deploys the intent of Swift's `withTaskCancellationShield` (SE-0504) to today's toolchains, at saga granularity rather than scope granularity.

**One FIFO lock per saga, inside the actor.** Actor isolation is not enough. A `cancel` that arrives while the drive loop is suspended inside `journal.append` would pick the same sequence number. The lock hands ownership directly to the next waiter (no lost wake-ups). The test forces exactly that interleaving with a gated journal. *Rejected:* a global serial queue. It is correct, but one slow saga would block all of them.

**Epoch fencing instead of "only one coordinator exists".** "Only one instance" is a belief, not a mechanism. A zombie task after `retire()`, or a relaunch that races an old instance, breaks it. Every append carries an epoch, and the journal rejects older ones. A coordinator takes its epoch *before* folding any history, so a predecessor cannot append behind what was just read. `FileJournal` re-reads the epoch file on every append, so it fences instances that share no memory. *Limit, stated plainly:* across **processes** (an app and its extensions), that check-then-append is not atomic without a file lock. Keep one writer process, or put `NSFileCoordinator`/`flock` around the journal. The in-process guarantee does not depend on this.

**Retention defines the dedupe window.** Pruning finished sagas bounds disk and memory, at a stated cost. Once a key's saga is pruned, starting the same key again creates a new saga, and only server-side dedupe remains. Payment APIs make the same trade with expiring idempotency keys. It is a parameter, not an accident, and there is a test for both sides of the window. Sagas finished by an *earlier* process are pruned first. The journal stores no clock, so among themselves they are ordered by `SagaID`, not by finish time.

**Full-jitter backoff, injected clock and randomness.** Full jitter (`random(0…min(cap, base·2ⁿ))`) avoids synchronized retry storms when a dependency recovers. `Sleeper` and `RandomSource` are injected, so the tests assert exact delays without waiting.

**Corruption is all-or-nothing, on purpose.** A torn final line is repaired. A bad line in the *middle* of `FileJournal` makes `open()` throw, and no saga resumes until someone looks at it. Skipping the bad line would fold a history with a hole in it, and the reducer would make a confident decision from a lie. *Rejected:* per-saga quarantine, which needs one file per saga. That is a reasonable next step if the journal ever holds many unrelated sagas. `ResumeReport.failures` covers per-saga problems: an unknown definition, or a history the reducer rejects.

**Retention runs in batches.** Pruning on every finish would rewrite the whole `FileJournal` on every finish once it reached steady state. Instead the coordinator lets the backlog grow to `retainFinished + max(1, retainFinished/4)` and then removes the excess in one `remove([SagaID])` call, which is a single rewrite. The saga whose finish triggered the prune is still running at that moment, so it always survives that batch. Even `retainFinished: 0` therefore keeps the newest finished saga until the next batch.

**What it deliberately does not do:** parallel branches inside one saga, distributed transactions across devices, or exactly-once delivery against a server that does not deduplicate. With a non-idempotent API the best any client can offer is at-least-once, and the README says so instead of pretending otherwise.

## Usage

```swift
.package(url: "https://github.com/rajatslakhina/durable-saga-kit.git", from: "1.0.0")
```

```swift
import DurableSaga

let checkout = try SagaDefinition(name: "checkout", steps: [
    .compensatable("reserveStock",
                   execute:    { inv in try await inventory.reserve(sku, key: inv.idempotencyKey) },
                   compensate: { inv in try await inventory.release(key: inv.idempotencyKey) }),
    .compensatable("authorizePayment",
                   execute:    { inv in try await payments.authorize(amount, key: inv.idempotencyKey) },
                   compensate: { inv in try await payments.void(key: inv.idempotencyKey) }),
    .pivot("placeOrder",   execute: { inv in try await orders.place(key: inv.idempotencyKey) }),
    .retriable("receipt",  execute: { inv in try await mail.sendReceipt(key: inv.idempotencyKey) })
])

let journal = FileJournal(directory: appSupportURL)
let coordinator = try SagaCoordinator(journal: journal, definitions: [checkout])

// On launch: finish whatever the previous process left in flight.
let report = try await coordinator.resumeAll()

// On tap. A double tap with the same key attaches to the same saga.
let outcome = try await coordinator.start("checkout", key: cartID)
switch outcome {
case .completed:                          showConfirmation()
case .compensated(let reason):            showFailure(reason)
case .parked(let step, _, let error):     escalate(step, error)   // later: coordinator.revive(id)
}
```

Throw `StepFailure.permanent(_:)` (or any error conforming to `ClassifiedStepError` with `isPermanent == true`) when the server definitively refused the request. Every other error is treated as ambiguous and retried with the same key.

## Run it

```bash
git clone https://github.com/rajatslakhina/durable-saga-kit.git
cd durable-saga-kit
swift build -Xswiftc -warnings-as-errors
swift test
```

Requires Swift 6.0 (Xcode 16+). Platforms: iOS 16+, macOS 13+, and Linux. Foundation is the only dependency. Durability notes: each append is `fsync`ed. Whole-file rewrites (tail repair, retention, the epoch file) use an atomic rename, but the file and directory are not separately `fsync`ed.

## Verification

What was actually run, and what was not:

- **Local, Linux, Swift 6.0.3:** after `rm -rf .build`, `swift build -Xswiftc -warnings-as-errors` is clean, and `swift test` runs **66 tests, 0 failures** (run three times in a row, all green).
- **Mutation checks (local).** Each mechanism below was broken in a scratch copy, and the suite was rerun against it:

  | Mutation | Killed by |
  |---|---|
  | per-saga lock disabled (`acquire` always succeeds) | `testCancelArrivingDuringAnAppendWaitsForTheSagaLock` (`sequenceConflict`) |
  | idempotency key includes the attempt number | `testTransientFailuresRetry…`, `testCrashInTheDoubtWindow…` |
  | rollback covers only observed successes | `testAmbiguousFailureIsUndoneByKey`, `testCrashDuringTheFinalAttempt…`, `testCompensationCovers…` |
  | pivot in doubt only while its request is open (the round-1 bug) | `testCancelDuringPivotBackoff…`, `testCancelWhileParkedAtThePivot…`, `testPivotStaysInDoubt…` |
  | planner ignores pivot doubt on cancel | `testCancelWhilePivotInDoubtResolvesThePivotFirst` |
  | `InMemoryJournal` epoch check removed | `testAZombieCoordinator…`, `testInMemoryJournalFencingAndSequenceChecks` |
  | one unloadable saga aborts retention | `testAnUnloadableSagaDoesNotDisableRetention` |
  | a journaled permanent failure is ignored after a crash (retried instead of acted on) | `testCrashAfterAJournaledDecline…`, `testAJournaledPermanentFailure…` |
  | both de-duplication guards in concurrent pruning removed | `testConcurrentPrunesOverEarlierProcessSagasDoNotDeadlock` *hangs* (self-deadlock). It was killed by a 150 s timeout |

- **Independent review:** an Opus reviewer with no part in writing the code graded it against a written checklist. Round 1 failed it with two blockers (the pivot-doubt window above, reproduced end to end in the demo) and six major issues. Round 2 confirmed those were fixed, then found one new major issue: after a crash between `stepFailed(permanent: true)` and its follow-up record, recovery re-sent a request the server had already declined. It also found documentation gaps. Round 3, the last round this pipeline allows, confirmed every round-2 item fixed except one. It then found a real deadlock: two overlapping prunes could each pick up the same saga left by an earlier process and insert it twice, and the batched remove would then take that saga's lock twice. It also flagged a scheduling-dependent test and some doc nits. All of these were fixed before anything was pushed. There is a regression test and a mutation check (table above). **These final round-3 fixes were not independently re-reviewed.**
- **CI:** _(filled in from the Actions tab once it has run)_
- **Not done:** the companion demo app has **not** been launched on a Simulator by the author (see its README).

## License

MIT
