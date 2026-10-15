# Reflection

**Which fault types were hardest to catch, and why?**

Subtle numeric faults were hardest because their measurements can remain inside
the published three-sigma bounds and overlap normal variation. Lineage also
requires checking actual edge names, not just whether the upstream list is
empty. The defense combines declared inputs, a job reference observed through
clean practice RPCs, and dependencies learned within the current run. Reference
rules identify pipeline assets, never event IDs or seeds. The private report
only exposes aggregate results, so I cannot identify its individual misses.

**What would you change about your cost/coverage tradeoff, if you had another pass?**

The selected private result is 41.68: TPR 0.9815, FPR 0.2466, and no cost-overage
penalty. The lower statistical cutoffs catch almost every fault but also alert
on too many clean events; this does not meet the 45-point target. With another
pass, I would prioritize precision using more representative clean calibration
and additional independent signals, then validate on fresh streams. I would
retain one-tool-per-event coverage and cache repeated requests: skipping events
would lose evidence while providing no private cost-penalty saving in this run.

**Which statistics in `ctx.state` would you improve with more time?**

I would maintain bounded rolling windows per table, job, feature view, and
corpus, using median and median absolute deviation to measure local variation.
After a minimum warm-up, I would compare these estimates with the published
baseline and require persistent evidence for small deviations. Suspected
anomalies would be excluded from reference updates to reduce contamination.
This remains a proposed improvement, not part of the submitted detector.
