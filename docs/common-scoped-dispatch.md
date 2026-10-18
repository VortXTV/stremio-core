# Scoped dispatch transport

This is a backend-neutral source milestone, not an enabled persistence backend.
`Runtime::new` continues to use the Legacy Model methods. Recorded dispatch requires
explicit Model capability, a bootstrap origin and an owned decline receiver through
`Runtime::new_recorded`. The supplied transport fixtures are not evidence of real
storage-owner, startup, native-application or device integration.

## Invocation and ownership boundary

Every recorded root, bare internal and framed library invocation pairs its returned
scope and subject before delivering its ordinary changed fields or effects. A
library child must return its original LibraryItem ID and scope. An unrelated
`Other` acceptance cannot consume that input or fabricate its action error.
Malformed output returns the original owned input through the decline receiver.
It cannot roll back an incorrectly implemented Model's prior in-memory mutation.

The four library action constructors are shared by both routes. Their scoped
frames own only Add, Remove, Rewind or notification-toggle dependent tails. A
refused child suppresses its own success tail and not-yet-started dismissal, not
the surrounding parent's independent effects. Notification removal already
performed before a secondary timestamp child remains independent of that child's
refusal. Player and other screen effects are not blanket-dependent tails.

An independently admitted prefix is submitted before ordinary output or nested
planning. That admission is a Model contract, not something the transport proves
from the returned direct outcome. Scheduling must enqueue; it must not immediately
poll a callback that reacquires the held transition. The decline receiver owns its
input and must not reenter, await, replay or log payloads. Scope-token cloning
copies an Arc, not a profile, library or history.

`AcceptedChange` means an admitted changed-input path. It is not a durable-storage
ACK or proof that legacy timestamp ordering selected the requested value.
`AcceptedNoChange` requires real admission before the equality decision. Domain
errors, current admission refusal, obsolete scope and protocol rejection remain
separate. Admission errors are additive; existing Other codes 1–8 are unchanged.

## Required integration and verification

Recorded completion delivery now asks the Model for an original-authority lease
after acquiring the Runtime transition, for both initial and later resolved
completions. The default Model response refuses; a synthetic validator is not real
storage/auth-attempt authority. A future-side check before waiting is insufficient.
Real owner admission/reservation, recorded reducers, eager persistence suppression,
facade routing, uncertain-publication ownership and their composition fixtures
remain integration requirements. The transport does not provide those by itself.

The current thirteen `common_transport_t01`–`t13` saved tests use actual Runtime,
Ctx and shared constructors with synthetic Env/admission inputs. All thirteen
passed in separately compiled default-local and `env-future-send` source units.
A predecessor Runtime compiled successfully but failed the intended ordinary-field
assertion in the same T10 fixture, establishing the focused negative control.
The original Runtime test suffix remains present. These focused units excluded
23 unrelated old test-module entrypoints only in disposable staged sources; no
original production test body was deleted. They are not a full upstream test-target,
clippy/MSRV, native-application or whole-program compatibility claim. Real Owner,
completion-delivery and storage integration remain open. No SDK, release,
live-data or performance acceptance follows from these synthetic checks.

## Recorded completion queue milestone

The explicitly recorded route uses one elastic queue for ordinary and completion
events. Close and commit use the same queue mutex. A completion's prepared nodes
and rejected payloads outlive the authority, Model and transition guards. Taken
wakers and deferred drops run after those guards release, including on unwind.
An already committed event is not retried if its wake later panics. The Legacy
constructor retains its existing bounded channel and ordinary dispatch behavior.

A pending completion carries one original, non-cloneable publisher/receiver pair.
Cancellation or observer loss is not a durable rollback, ACK or replay permit.
The Runtime holds weak delivery targets so a pending observer does not retain the
whole Model. Original storage/auth and add-on authority must remain valid at each
actual queue commit; cancellation stops observation, not an admitted remote write.

The exact placed source passed all 41 named source-unit cases in separately fresh
default and `env-future-send` compilations: 13 scoped transport, 19 completion
delivery and nine queue cases, with zero ignored or filtered cases in each run.
The original old-test suffix remains present; the same 23 unrelated test-module
entries were excluded only in disposable staging. A test-only Legacy-adapter
dead-code warning remains visible in these narrowed builds, not suppressed.

This is an unwired source milestone, not native activation. The consuming Env
handoff can still reject, panic or drop a future while the transition is held;
recoverable post-guard handoff remains required. Real Owner/borrowed-Ctx issuance,
Unknown publisher binding, ordinary payload destruction, shutdown/quiescence,
queue/resource budgeting and whole-program/device performance remain open.
No full upstream suite, SDK, app build, live-account operation or release result
is claimed by these 41-case checks.
