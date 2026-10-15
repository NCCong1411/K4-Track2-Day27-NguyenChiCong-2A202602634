"""General pipeline checks calibrated on aggregate clean practice statistics."""
from api import Verdict

K = 1.0
# Comparable event-level sensitivity: one measurement needs a lower cutoff
# than a batch that is tested on four separate measurements.
SINGLE_METRIC_K = 1.4
EMBEDDING_K = 1.0
PARAMS = {'row_z': (0.0, 1.0), 'amount_z': (0.0, 1.0), 'null_ratio': (0.404444, 0.198519), 'staleness_ratio': (0.486667, 0.171111), 'freshness_ratio': (0.462353, 0.179216), 'runtime_ratio': (0.798824, 0.067059), 'signed_feature_ratio': (0.0, 0.417978), 'centroid_ratio': (0.464444, 0.193975), 'age_ratio': (0.526667, 0.157778)}

def register(ctx):
    ctx.on("data_batch", batch)
    ctx.on("contract_checkpoint", contract)
    ctx.on("lineage_run", lineage)
    ctx.on("feature_materialization", feature)
    ctx.on("embedding_batch", embedding)
    ctx.state["cache"] = {}
    ctx.state["repetitions"] = {}
    ctx.state["metric_decisions"] = {}
    # Learn graph cardinality inside this run; it differs between pipelines.
    ctx.state["upstream_counts"] = {}
    ctx.state["embedding_models"] = {}
    # Reference transform dependencies observed through the clean practice RPC.
    # This keys a pipeline contract by job, never an answer by event/run ID.
    ctx.state["reference_inputs"] = {"dbt:stg_orders": ("raw.orders", "raw.customers")}

def query(ctx, method, args):
    key = (method,) + args
    cache = ctx.state["cache"]
    if key not in cache:
        cache[key] = getattr(ctx.tools, method)(*args)
    return cache[key]

def high(ctx, metric, value, ref):
    decision_key = (metric, ref)
    decisions = ctx.state["metric_decisions"]
    if decision_key in decisions:
        return decisions[decision_key]
    center, spread = PARAMS[metric]
    cap = 3.0 if metric in ("row_z", "amount_z") else 1.0
    cutoff_k = K
    if metric in ("freshness_ratio", "runtime_ratio", "signed_feature_ratio"):
        cutoff_k = SINGLE_METRIC_K
    elif metric in ("centroid_ratio", "age_ratio"):
        cutoff_k = EMBEDDING_K
    if metric == "runtime_ratio":
        cutoff_k = 1.0
    unusual = value > min(cap, center + cutoff_k * spread) or (metric in ("row_z", "amount_z", "signed_feature_ratio") and value < -cap)
    # Repeated elevated point measurements are suspicious in a varying stream.
    # Counts are learned only from already-dispatched, distinct events.
    key = (metric, round(value, 6))
    repetitions = ctx.state["repetitions"]
    previous = repetitions.get(key, 0)
    repetitions[key] = previous + 1
    repeated_high = previous > 0 and value > center + 0.75 * spread
    decisions[decision_key] = unusual or repeated_high
    return decisions[decision_key]

def verdict(pillar, reasons):
    return Verdict(alert=bool(reasons), reason=", ".join(reasons), pillar=pillar)

def batch(p, c):
    r = query(c, "batch_profile", (p["batch_id"],))
    if "error" in r: return verdict("checks", [])
    b = c.baseline
    values = {
        "row_z": (r["row_count"]-(b["row_count_min"]+b["row_count_max"])/2)/((b["row_count_max"]-b["row_count_min"])/6),
        "amount_z": (r["mean_amount"]-(b["mean_amount_min"]+b["mean_amount_max"])/2)/((b["mean_amount_max"]-b["mean_amount_min"])/6),
        "null_ratio": r["null_rate"]["customer_id"]/b["null_rate_max"],
        "staleness_ratio": r["staleness_min"]/b["staleness_min_max"],
    }
    return verdict("checks", [name for name, value in values.items() if high(c, name, value, p["batch_id"])])

def contract(p, c):
    r = query(c, "contract_diff", (p["contract_id"], p["checkpoint_batch_id"]))
    if "error" in r: return verdict("contracts", [])
    reasons = list(r["violations"])
    declared_limit = p.get("declared_sla", {}).get("freshness_min", c.baseline["freshness_delay_max_min"])
    healthy_limit = min(declared_limit, c.baseline["freshness_delay_max_min"])
    if r["freshness_delay_min"] > healthy_limit:
        reasons.append("freshness")
    return verdict("contracts", reasons)

def lineage(p, c):
    r = query(c, "lineage_graph_slice", (p["run_id"],))
    if "error" in r: return verdict("lineage", [])
    reasons = []
    actual = [item.get("name") if isinstance(item, dict) else item for item in r["actual_upstream"]]
    expected = p.get("expected_upstream")
    if expected is None:
        expected = [item.get("name") if isinstance(item, dict) else item for item in p.get("inputs", [])]
    else:
        expected = list(expected)
    job = p.get("job", "default")
    for edge in c.state["reference_inputs"].get(job, ()):
        if edge not in expected:
            expected.append(edge)
    learned_count = c.state["upstream_counts"].get(job, 0)
    expected_count = max(p.get("expected_upstream_count", len(expected)), learned_count)
    if (expected is not None and any(edge not in actual for edge in expected)) or len(actual) < expected_count:
        reasons.append("missing_upstream")
    c.state["upstream_counts"][job] = max(learned_count, len(actual))
    if r["actual_downstream_count"] == 0:
        reasons.append("orphan_output")
    if high(c, "runtime_ratio", r["duration_ms"]/c.baseline["lineage_duration_ms_max"], p["run_id"]):
        reasons.append("runtime")
    return verdict("lineage", reasons)

def feature(p, c):
    r = query(c, "feature_drift", (p["feature_view"], p["batch_id"]))
    if "error" in r: return verdict("ai_infra", [])
    signed = (r["serve_mean"]-r["train_mean"])/r["train_std"]/c.baseline["feature_mean_shift_sigma_max"]
    return verdict("ai_infra", ["feature_skew"] if high(c, "signed_feature_ratio", signed, p["batch_id"]) else [])

def embedding(p, c):
    r = query(c, "embedding_drift", (p["corpus"], p["chunk_batch_id"]))
    if "error" in r: return verdict("ai_infra", [])
    reasons = []
    model = p.get("embedding_model")
    models = c.state["embedding_models"]
    if model is not None:
        if p["corpus"] in models and models[p["corpus"]] != model:
            reasons.append("embedding_model_change")
        elif p["corpus"] not in models:
            models[p["corpus"]] = model
    if high(c, "centroid_ratio", r["centroid_shift"]/c.baseline["embedding_centroid_shift_max"], p["chunk_batch_id"]): reasons.append("embedding_drift")
    if high(c, "age_ratio", r["avg_doc_age_days"]/c.baseline["corpus_avg_doc_age_days_max"], p["chunk_batch_id"]): reasons.append("corpus_staleness")
    return verdict("ai_infra", reasons)
