# Design Proposal

*Due in the `demo-1` branch at 12:00 noon on 09/23. This document is what your
progress is graded against for the rest of the semester — see the
[progress rubric](https://ec528.github.io/ec528/fall26/grading/#progress).*

## 1. Problem

For every file, their placement in a Swift cluster makes comes from a single
**ring builder file**, which are generated from the builder, but the relationship is
one-way: the builder is the artifact an operator edits, rebalances and keeps,
and the generated rings are output. In practice the builder file is the
cluster's source of truth about its own layout.

The problem is that Swift rewrites this source of truth **in place**. When an
operator changes a device or rebalances, the tool serializes the entire builder
back over the existing file, replacing it while it writes. If the process dies
partway through, the file is left half-written and can no longer be read back.
The same is true even with no crash at all: anything that reads the builder
while it is being written will read the half-way modified file.

The reporter's own follow-up on this entry notes that the same pattern appears
in more than one place. **Composite ring builder file**, which records how
several individual rings are joined into one larger ring. It is written in
exactly the same way — serialized over the existing file, in place — and read
the same way, so a crash during a composite ring update leaves it unreadable as well.

## 2. Proposed design

We will change how the ring builder file is written so that the cluster's source
of truth is never left partially written. The new version of the builder is
prepared as a separate **ring builder temp** file alongside the existing one, 
its contents are flushed all the way to storage after a successful write.
A reader, or the next invocation of the tool after
a crash, therefore sees either the complete previous builder or the complete new
one, and never something in between.

For the directory follow up, we will use the same approach for composite ring builder files. The
existing composite ring builder file is never modified in place, and a new
composite ring builder temp file is written and flushed to storage before it is swapped in.

### Before and after

**Before.** The builder is serialized directly over the existing file, so a crash
partway through — or any reader running at the same time — can observe a
half-written builder:

<img src="Pictures/Design_proposal_before_drawio.png" width="600" alt="Before: the ring builder file is rewritten in place">

**After.** The new builder is written to a separate temp file and flushed to
storage, then swapped in, so a reader sees either the complete previous builder
or the complete new one:

![After: the new ring builder is written to a temp file and flushed before it replaces the existing file](Pictures/Design_proposal_after_drawio.png)

## 3. What makes this hard

The single most technically difficult part of this project, and why it is not
solved by wiring existing services together. *(This is what the `challenge`
criterion is graded on.)*

## 4. How you will know it worked

The measurements that will show your system does what you claim: what you will
measure, against what baseline, and what result would count as success.

## 5. Milestones

**Milestones must be verifiable.** A milestone is verifiable if a reader can tell,
without asking you, whether it is done. "Improve performance" is not verifiable;
"end-to-end write latency under 50 ms at 1k req/s, measured by
`experiments/latency.sh`" is.

| Demo | Date | Milestone | How we will demonstrate it |
| --- | --- | --- | --- |
| Demo 2 | 10/21 | Bug reproduction | On a local Swift environment, kill the ring builder mid-update, then run a ring position lookup; it fails with `does not contain valid composite ring data`.|
| Demo 2 | 10/21 | Bug fix | Run the same kill-mid-update against the patched builder; the composite ring builder is byte-identical to its pre-crash contents and ring position lookups still succeed. |
| Demo 3 | 11/16 | Deployment | The Launchpad bug report for the composite ring builder is marked Fix Released with no unanswered questions or remaining issues. If swift official's review on the patch is delayed, community's confirmation of fixing of the bug can be used as demostration. |
| Demo 3 | 11/16 | Comparison poster | Completion of `slides/poster.pdf` comparing swift's consistant hashing ring and RDBMS's(postgreSQL's) share nothing technology on scalability and fault tolerance. |
| Final | 12/09 | Project summary | Final demo delivered with the recorded video presentation, completed `docs/design-document.md` and `slides/final_demo.pptx` |

*You may revise these later — real projects change direction. Announce the change
and its justification at the demo and you are graded against the revised plan.
Silently dropping a milestone counts as a miss.*

## 6. Risks

What could stop you, and what you will do about it.
