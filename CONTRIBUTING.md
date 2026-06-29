# Contributing to kubemq-gcp-pub-sub

Thanks for helping improve **kubemq-gcp-pub-sub** — the documentation, examples, and burn-in
harness for the KubeMQ **Google Cloud Pub/Sub** connector. This repository teaches developers to
drive the connector from the standard, off-the-shelf, first-party Google Cloud Pub/Sub client
libraries via the emulator-protocol drop-in (`PUBSUB_EMULATOR_HOST` — zero code changes). It ships
**no installable package, no proto/gRPC bindings, and no published client library** — so there is
nothing to release or version-bump beyond the docs and examples themselves.

Contributions usually fall into three buckets: fixing or extending the docs, adding or improving
examples, and tuning the burn-in harness.

## Prerequisites

- A running **KubeMQ server with the Google Cloud Pub/Sub connector enabled** and reachable (see
  the [README](README.md) for the `PUBSUB_EMULATOR_HOST` drop-in and the default port **8085**).
- The toolchains for whichever example languages you touch (the examples span Go, Python, Java,
  JS/TS, C#, Ruby). Each language's exact version, install, and run steps live in
  [`examples/SHARED-CONVENTIONS.md`](examples/SHARED-CONVENTIONS.md).
- **Git** with commit sign-off configured (see [Sign-off](#sign-off)).

## Repository layout

| Path | What it is |
|------|------------|
| [`docs/`](docs/) | Connector reference: architecture, getting-started, configuration, per-concept docs, task guides, and reference tables. Every behavioral claim should trace back to the connector reference or a named test. |
| [`examples/`](examples/) | Runnable, per-pattern examples across Go, Python, Java, JS/TS, C#, Ruby (up to 90 examples), using the standard first-party Google Cloud Pub/Sub client libraries only. [`examples/SHARED-CONVENTIONS.md`](examples/SHARED-CONVENTIONS.md) is the single source of truth for the per-language loop. |
| [`burnin/`](burnin/) | Standalone Go soak-test harness that exercises the connector under sustained multi-pattern load (one worker per Pub/Sub pattern), on fixed port **8899**. |

## Building & running examples

The per-language **build / lint / run** loop — toolchain versions, dependency install, the
emulator environment variables, and how to run each pattern — is documented once in
[`examples/SHARED-CONVENTIONS.md`](examples/SHARED-CONVENTIONS.md). Follow it rather than inventing
per-example steps; keep new examples consistent with the conventions already there.

> **Emulator drop-in.** Every example connects by exporting `PUBSUB_EMULATOR_HOST=localhost:8085`
> and `PUBSUB_PROJECT_ID=my-project` — the official Google client honours these and uses insecure
> gRPC with no credentials. **No code changes** are required to retarget a Pub/Sub app at KubeMQ.
> Only the `interop/native-events-store` variant additionally opens the native KubeMQ gRPC broker
> (`localhost:50000`).

Two project skills wrap the loop across all languages at once:

- **`/examples`** — runs the examples against a live KubeMQ broker.
- **`/lint`** — auto-formats first, then reports remaining lint issues.

Before opening a PR, run `/lint` and (against a live connector) `/examples` for the languages you
changed, and confirm the docs still match the connector's actual behavior. Keep example resource
names uuid-suffixed and best-effort-deleted so concurrent runs stay parallel-safe (see
[`examples/SHARED-CONVENTIONS.md`](examples/SHARED-CONVENTIONS.md) §1, channel isolation).

## Submitting changes

1. **Fork** the repository and create a topic branch off `main`
   (`git checkout -b fix/my-change`).
2. Make your change. Keep it focused — avoid unrelated refactors in the same PR, and update the
   docs and [`CHANGELOG.md`](CHANGELOG.md) when behavior or coverage changes.
3. Run `/lint` (and `/examples` where relevant) and make sure they pass.
4. Commit with a sign-off (`git commit -s`; see below) and push your branch.
5. Open a **pull request against `main`** with a clear description and a linked issue if one
   exists.

## Sign-off

This project requires the [Developer Certificate of Origin](https://developercertificate.org/)
(DCO) on **every commit**. The DCO is a lightweight statement that you wrote the contribution
or otherwise have the right to submit it under the project's license.

To certify it, add a `Signed-off-by` trailer by committing with the `-s` flag:

```bash
git commit -s -m "docs: clarify exactly-once node-local caveat"
```

This appends a line matching your Git identity:

```
Signed-off-by: Your Name <you@example.com>
```

Make sure your `user.name` and `user.email` are set (`git config user.name` / `user.email`)
so the trailer is valid. Commits without a `Signed-off-by` line cannot be merged. To fix an
existing commit, amend it with `git commit -s --amend`; to fix several, use
`git rebase --signoff`.

## License

By contributing, you agree that your contributions are licensed under the repository's
**Apache-2.0** license (see [`LICENSE`](LICENSE)).
