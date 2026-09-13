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

```bash
# clone, build, and run the smallest thing that shows the system working
```

## Repository layout

| Path | Contents |
| --- | --- |
| `docs/` | Design proposal and design document |
| `slides/` | Demo slides (`demo-1.pdf`, `demo-2.pdf`, ...) |
| `src/` | Source code |
| `experiments/` | Scripts that reproduce every result you claim |

## Reproducing our results

See [`docs/design-document.md`](docs/design-document.md). Every claim we make in a
demo or in the final presentation has a corresponding script in `experiments/`.

## Submission checklist

Deliverables are collected from a **branch named for the demo**, at **12:00 noon**
on the day of that demo. See the
[submission instructions](https://ec528.github.io/ec528/fall26/setup/).

| Deliverable | Branch | Must contain |
| --- | --- | --- |
| Demo 1 | `demo-1` | slides, code, `docs/design-proposal.md` |
| Demo 2 | `demo-2` | slides, code, `docs/design-document.md`, demo video |
| Demo 3 | `demo-3` | slides, code, updated `docs/design-document.md`, demo video |
| Final | `final-demo` | slides, code, artifact documentation, recorded video presentation |
