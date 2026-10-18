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

Completion delivery still needs original storage/auth-attempt and add-on authority
validated after acquiring the Runtime transition, for both initial and later
resolved completions. A future-side check before waiting is insufficient. Real
owner admission/reservation, recorded reducers, eager persistence suppression,
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
