# EC528 Project — Highly Available Object Storage Development

**Team:** Abdoul Abdou, Bowen Tan, Gyoungmin Chae, Yihan Wang, Jiabao Chen   
**Mentor:** Andressa Cabistani (Red Hat)  
**Project:** OpenStack Swift — contributing a tested bug fix to a real distributed object store, to learn how it handles replication and durability.

## What this is

Swift is a highly available, distributed, eventually consistent object store that keeps data
durable across node failures by replicating objects and rebalancing them when the cluster changes.
We contribute to that durability path directly: pick a live bug from Swift's tracker, reproduce it
on a dev cluster, fix it with a failing-then-passing test, and push it through Gerrit review and CI.


## Quick start

Both parts of the project have detailed setup and experiment commands for
local Linux and the team's prepared LightSail instances:

- **Swift Light — `SwiftLight` instance:** Follow the
  [Swift Light design document](docs/design-document-swift-light.md)
  to start the cluster and run the upload and download experiments.

- **RingBuilder bug fix — `SwiftBugFix` instance:** Follow the
  [bug-fix design document](docs/design-document-bug-fix.md)
  to prepare the OpenStack Swift checkout and run experiments 0–4.

The bug-fix experiments run against the separate OpenStack Swift checkout;
they do not require the Swift Light cluster.

## Repository layout

| Path | Contents |
| --- | --- |
| `docs/` | Design proposal and design document |
| `slides/` | Demo slides (`demo-1.pdf`, `demo-2.pdf`, ...) |
| `src/` | Source code |
| `experiments/` | Scripts that reproduce every result you claim |

## Reproducing our results

See [`docs/design-document-swift-light.md`](docs/design-document-swift-light.md). Every claim we make in a
demo or in the final presentation has a corresponding script in `experiments/`.

## Submission checklist

Deliverables are collected from a **branch named for the demo**, at **12:00 noon**
on the day of that demo. See the
[submission instructions](https://ec528.github.io/ec528/fall26/setup/).

| Deliverable | Branch | Must contain |
| --- | --- | --- |
| Demo 1 | `demo-1` | slides, code, `docs/design-proposal.md` |
| Demo 2 | `demo-2` | slides, code, `docs/design-document-swift-light.md`|
| Demo 3 | `demo-3` | slides, code, updated `docs/design-document-swift-light.md`, demo video |
| Final | `final-demo` | slides, code, artifact documentation, recorded video presentation |
