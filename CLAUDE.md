# Shiny.Actors — Working Notes

Guidance for maintaining this repo. Code lives in `src/`, tests in `tests/`, samples in `samples/`,
design docs in `docs/`, the published Claude Code skill in `skills/`. The public documentation site
lives in a **separate** repo at `~/Desktop/dev/documentation` (rendered to https://shinylib.net/actors).

This is an Orleans-style virtual actor framework that runs in one process anywhere .NET runs: .NET
MAUI, Blazor WebAssembly, console and server. It is AOT/trim-clean. Other processes and devices call
actors over HTTP. The core is `Shiny.Actors` (runtime + remote client) plus its source generator
(packed into the core package's `analyzers/`). The add-ons are separate packages under `src/`:

- DocumentDb (storage)
- HttpServer (serving actors)
- Jobs (background reminders)
- Notifications (reminder notifications)
- Discovery (mDNS)
- Testing (`ActorTestHost`)

## After every new feature or fix

A change is not "done" until the four artifacts below are in sync. Do all of them in the same change
unless there's a reason not to.

1. **Code + tests** (`src/`, `tests/`)
   - The trim/AOT analyzers are on for every shipping project, and `TreatWarningsAsErrors` is on
     everywhere. A change that introduces reflection, or an unannotated dynamic dependency, is a
     regression, not a warning to suppress. `samples/Sample.Console` publishes NativeAOT in CI and must
     stay at zero warnings.
   - Run the suite several times (`for i in 1 2 3 4 5; do dotnet test ...; done`). Timing-sensitive
     failures have been real bugs here more often than flaky tests: a racy count in the stateless-worker
     pool, and a provider throwing synchronously past a guard. Find the cause before touching the test.
   - When a test *is* wrong, fix what it asserts, not its timeouts. Assert the property that matters
     (e.g. concurrency), not wall-clock time under full-suite load. `ActivityListener`/`MeterListener`
     are process-wide, so scope telemetry assertions by trace id or by an actor type only that test uses.

2. **Documentation site** (`~/Desktop/dev/documentation/src/content/docs/actors/`)
   - **The folder doesn't exist yet.** The first docs change creates it: an `index.mdx` (overview + package
     table), feature pages, and `release-notes.mdx`. It also needs a sidebar node in `src/sidebar-topics.mjs`
     in that repo. Crib the structure from the `httpserver/` folder next to it.
   - Update the relevant feature page, and add a **release note** (rules below).
   - Pages are `.mdx`; release notes use the `<RN>` component
     (`import RN from '/src/components/ReleaseNote.astro'`), with `type="feature|enhancement|fix|breaking"`.

3. **Skill** (`skills/shiny-actors/SKILL.md`)
   - This is the source of the published `shiny-actors` Claude Code skill: the agent-facing "how to
     generate correct code" doc. `sync-skills.yml` opens a PR against `shinyorg/skills` when `skills/**`
     changes on a `v*` branch.
   - Keep it aligned with the code. Update the `triggers:` list when a new public type, attribute,
     builder method or package appears. If the recommended pattern changes, the skill's guidance changes
     too. Don't let the skill document APIs that were removed.

4. **README.md** (repo root)
   - Packed into every NuGet package as `readme.md`. Update the feature table, the relevant section, and
     the layout list when behavior or packages change.

## Release notes

Release notes go in `~/Desktop/dev/documentation/src/content/docs/actors/release-notes.mdx`.

**Which version?** The `version` field in `version.json` (Nerdbank.GitVersioning), raw version only:
strip any prerelease suffix, so `1.0.0-beta.{height}` becomes `1.0.0`.

**Unreleased work** goes under a `## <version> TBD` heading at the top. Create it if it doesn't
exist. If the feature you're changing hasn't shipped yet (it already has an entry under `TBD`), edit
that entry instead of adding another. **When cutting a release**, promote the heading to a dated one
(`## 1.0 - June 13, 2026` for a minor, `## 1.0.2 - ...` for a patch).

Each note is one `<RN>` line. Use `type="breaking"` for breaking changes. Changes confined to
`samples/` get no note.

## Rules this codebase depends on

- **Registration goes through `ShinyActorBuilder`.**
  - Everything is configured via `services.AddShinyActors(x => ...)`.
  - New packages add `Use*`/`Add*` extension methods on `ShinyActorBuilder`, in the
    `Microsoft.Extensions.DependencyInjection` namespace — never `IServiceCollection` or
    `ActorSystemOptions` extensions.
  - `ActorSystemOptions` is the raw settings object behind `x.Configure(...)` and
    `new ActorSystem(options)`.
  - Repeated `AddShinyActors` calls must keep accumulating into one configuration.
- **The generator is the only discovery mechanism.** Proxies, the wire contract (`ActorContract` / `ActorMethod`
  descriptors, emitted as static fields on the local proxy), registrations, and JSON-context registration all
  come from `src/Shiny.Actors.SourceGenerators`. Never add runtime reflection to compensate for something the
  generator should emit.
- **The dispatcher is the concurrency model.**
  - Every turn runs on the activation's `ConcurrentExclusiveSchedulerPair` exclusive scheduler. The dispatcher
    in `ActorActivation` only decides when a queued item may *start*: exclusive, read-only or interleave.
  - Counts used for scheduling (`Load`, quiet detection) must never see an item in neither place.
    `SingleReader` channels can't count themselves, so we keep our own counter.
- **State writes are ETag-conditional on every provider.**
  - A provider must pass `StateProviderTests` and `EventStoreTests` (`Stores.Kinds`), the conformance
    suites run against memory, file and DocumentDb/SQLite.
  - DocumentDb code must work on IndexedDB too: no `Upsert(patchIfUpdate: false)`, which IndexedDB doesn't
    support; a merge upsert keeps fields the patch leaves null.
- **Breaking changes.** Nothing has shipped yet (1.0.0-beta). Change APIs when it makes the design right, and
  record it as `type="breaking"` once there are release notes to put it in.

## Verifying beyond the test suite

- **NativeAOT:** `dotnet publish samples/Sample.Console -c Release -o <scratch>` must print no warnings, and the
  binary must run.
- **Blazor (WASM):** `dotnet run --project samples/Sample.Blazor --urls http://localhost:5199`, then drive it in
  Chrome. `InvariantGlobalization` is on because the browser could not fetch the ICU data file in this setup.
- **MAUI (`samples/Sample.Maui`):** Debug builds include the `maui devflow` agent.
  - iOS simulator: `dotnet build -t:Run -f net10.0-ios -p:_DeviceName=:v2:udid=<udid>`. This stays attached, so
    run it in the background.
  - Drive the app with `maui devflow batch -ap <port>`. Batch the actions: one-off CLI calls trip "another
    DevFlow session is driving this app". Synthetic taps don't select `CollectionView` rows, so use
    `ui navigate <route>`.
  - Android emulator: other DevFlow-enabled debug apps on the same emulator can hold port 9223, and then ours
    logs "Address already in use". Force-stop them, then `adb forward tcp:<local> tcp:9223`. Use
    `adb exec-out screencap` for screenshots.
  - Background jobs on Android: kill the app with `am kill`, not `force-stop` (which cancels its scheduled
    work). WorkManager refuses a forced `cmd jobscheduler run` for periodic work, so wait for its interval.
    Verify by pulling `files/chat.db` with `run-as` and querying it.
- **mDNS:** `DiscoveryTests` needs a real LAN interface (`Category=Network`; excluded in CI). The server must bind
  `IPAddress.Any`.

## Parked work

Multi-device routing ("the mesh") is designed and planned in `docs/design/` but not started. Before
building it, get answers to the open decisions D1–D5 in `multi-device-routing-plan.md`.
