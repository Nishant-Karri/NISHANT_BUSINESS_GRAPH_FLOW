"""
Generates drift_data.json from the Neptune Star Schema Knowledge Graph.
Simulates Neptune graph traversal: Store -> Transaction -> Product -> Category -> Brand -> Supplier
Runs every hour via GitHub Actions and commits updated data to the repo.
"""

import json
import random
import math
from datetime import datetime, timezone

random.seed(int(datetime.now(timezone.utc).strftime("%Y%m%d%H")))

# ── Knowledge Graph: Stores (Neptune Store vertices) ─────────────────────────
STORES = [
    {"store_id":"STR-001","store_name":"Manhattan Flagship","address":"100 Fifth Avenue","zip_code":"10011","region":"Northeast","store_type":"Flagship Store",
     "base":{"gross_margin":44.0,"return_rate":0.5,"aov":230.0,"discount_rate":10.0,"net_sales":1250.0,"txn_count":6}},
    {"store_id":"STR-002","store_name":"LA Downtown","address":"500 Wilshire Blvd","zip_code":"90010","region":"West","store_type":"Retail Store",
     "base":{"gross_margin":61.0,"return_rate":0.0,"aov":120.0,"discount_rate":0.0,"net_sales":340.0,"txn_count":3}},
    {"store_id":"STR-003","store_name":"Chicago Midway","address":"200 S Michigan Ave","zip_code":"60604","region":"Midwest","store_type":"Retail Store",
     "base":{"gross_margin":49.0,"return_rate":0.0,"aov":1450.0,"discount_rate":3.0,"net_sales":4000.0,"txn_count":3}},
    {"store_id":"STR-004","store_name":"Houston Galleria","address":"5085 Westheimer Rd","zip_code":"77056","region":"South","store_type":"Retail Store",
     "base":{"gross_margin":60.0,"return_rate":0.0,"aov":105.0,"discount_rate":5.0,"net_sales":480.0,"txn_count":5}},
    {"store_id":"STR-005","store_name":"Phoenix Desert Ridge","address":"21001 N Tatum Blvd","zip_code":"85050","region":"West","store_type":"Retail Store",
     "base":{"gross_margin":58.0,"return_rate":0.0,"aov":140.0,"discount_rate":0.0,"net_sales":390.0,"txn_count":3}},
    {"store_id":"STR-006","store_name":"Philadelphia Center","address":"1500 Market St","zip_code":"19102","region":"Northeast","store_type":"Retail Store",
     "base":{"gross_margin":55.0,"return_rate":0.0,"aov":122.0,"discount_rate":1.8,"net_sales":452.0,"txn_count":4}},
    {"store_id":"STR-007","store_name":"San Antonio Riverwalk","address":"849 E Commerce St","zip_code":"78205","region":"South","store_type":"Retail Store",
     "base":{"gross_margin":61.0,"return_rate":0.0,"aov":76.0,"discount_rate":4.4,"net_sales":282.0,"txn_count":4}},
    {"store_id":"STR-008","store_name":"Seattle Capitol Hill","address":"400 Pine St","zip_code":"98101","region":"West","store_type":"Retail Store",
     "base":{"gross_margin":65.0,"return_rate":0.0,"aov":101.0,"discount_rate":0.0,"net_sales":280.0,"txn_count":3}},
    {"store_id":"STR-009","store_name":"Denver Union Station","address":"1701 Wynkoop St","zip_code":"80202","region":"West","store_type":"Retail Store",
     "base":{"gross_margin":56.0,"return_rate":0.0,"aov":176.0,"discount_rate":13.0,"net_sales":326.0,"txn_count":2}},
    {"store_id":"STR-010","store_name":"Online Store","address":"\u2014","zip_code":"\u2014","region":"National","store_type":"E-Commerce",
     "base":{"gross_margin":65.0,"return_rate":0.0,"aov":124.0,"discount_rate":4.5,"net_sales":344.0,"txn_count":3}},
]

# ── Knowledge Graph: Product/Category/Brand edges (Neptune graph context) ────
GRAPH_EDGES = {
    "gross_margin": {
        "category":  ["Electronics","Grocery","Furniture","Apparel","Home & Kitchen"],
        "brand":     ["GameForce","NatureBrew","ComfortWork","StyleMax","TechPro"],
        "supplier":  ["Supplier A","Supplier B","Global Parts Co","DirectSource"],
    },
    "return_rate": {
        "category":  ["Electronics","Footwear","Apparel","Sports & Fitness","Home & Kitchen"],
        "loyalty":   ["Gold","Silver","Bronze","Platinum"],
        "promo_type":["Seasonal","Loyalty","nan","Flash"],
    },
    "aov": {
        "loyalty":   ["Platinum","Gold","Silver","Bronze"],
        "promotion": ["Loyalty Platinum Reward","Valentine's Day Special","No Promotion","Christmas Special"],
        "segment":   ["Retail","Wholesale"],
        "channel":   ["All"],
    },
    "discount_rate": {
        "category":  ["Electronics","Home & Kitchen","Grocery","Furniture","Footwear"],
        "channel":   ["All"],
        "promo_type":["Loyalty","Seasonal","nan","Flash"],
        "promotion": ["Loyalty Gold Reward","Easter Weekend","No Promotion","Loyalty Platinum Reward"],
    },
    "net_sales": {
        "category":  ["Electronics","Grocery","Furniture","Apparel","Sports & Fitness"],
        "brand":     ["GameForce","NatureBrew","ComfortWork","StyleMax"],
        "loyalty":   ["Platinum","Silver","Gold"],
        "region":    [],  # filled per store
    },
}

MULTIPLIERS = {"gross_margin":0.8,"return_rate":1.2,"aov":0.6,"discount_rate":0.9,"net_sales":1.0}
METRIC_LIST = ["gross_margin","return_rate","aov","discount_rate","net_sales"]
METRIC_LABELS = {"gross_margin":"Gross Margin %","return_rate":"Return Rate %","aov":"Avg Order Value",
                 "discount_rate":"Discount Rate %","net_sales":"Net Sales"}


def jitter(val, pct=0.18, floor=0.0):
    """Apply ±pct random fluctuation, simulating hourly Neptune graph read."""
    noise = val * random.uniform(-pct, pct)
    return max(floor, round(val + noise, 2))


def simulate_metrics(store):
    b = store["base"]
    return {
        "store_id":   store["store_id"],
        "store_name": store["store_name"],
        "address":    store["address"],
        "zip_code":   store["zip_code"],
        "region":     store["region"],
        "store_type": store["store_type"],
        "gross_margin":  jitter(b["gross_margin"], 0.15, 10.0),
        "return_rate":   jitter(b["return_rate"],  0.50, 0.0),
        "aov":           jitter(b["aov"],           0.20, 10.0),
        "discount_rate": jitter(b["discount_rate"], 0.40, 0.0),
        "net_sales":     jitter(b["net_sales"],     0.20, 50.0),
        "txn_count":     max(1, b["txn_count"] + random.randint(-1, 1)),
    }


def graph_context_for(store, metric):
    edges = {k: list(v) for k, v in GRAPH_EDGES[metric].items()}
    if metric == "net_sales":
        edges["region"] = [store["region"]]
    # pick a random subset to simulate graph traversal result
    result = {}
    for k, opts in edges.items():
        if not opts:
            continue
        n = random.randint(1, min(3, len(opts)))
        result[k] = random.sample(opts, n)
    return result


def severity(abs_drift):
    if abs_drift > 40: return "CRITICAL"
    if abs_drift > 25: return "HIGH"
    if abs_drift > 15: return "MEDIUM"
    return "LOW"


def recommend(store, metric, actual, baseline, drift_pct, loss_90d):
    lbl   = METRIC_LABELS[metric]
    abs_d = abs(drift_pct)
    is_amt = metric in ("aov","net_sales")
    fmt = lambda v: f"${v:,.0f}" if is_amt else f"{v:.1f}%"
    loss  = f"${abs(loss_90d):,.0f}"
    name  = store["store_name"]

    templates = {
        "gross_margin": {
            "neg": f"ROOT CAUSE\n{lbl} is {abs_d:.1f}% below baseline at {name}. Product mix or supplier costs are compressing margins.\n\nIMMEDIATE ACTIONS\n• Review pricing on low-margin SKUs and apply margin floor rules\n• Renegotiate supplier contracts for top-selling categories\n• Shift promotions to higher-margin product lines\n\nSTRATEGIC RECOMMENDATIONS\n• Introduce margin-based category targets reviewed monthly\n• Analyse product mix vs. top-performing peer stores\n\nLOSS PREVENTION\nContinued margin compression costs {loss} over 90 days.",
            "pos": f"{lbl} is above baseline — product mix and pricing are healthy at {name}. Continue monitoring supplier costs to sustain this advantage.",
        },
        "return_rate": {
            "pos": f"ROOT CAUSE\nReturn rate is {abs_d:.1f}% above baseline at {name}. Product quality or expectation mismatches are driving excess returns.\n\nIMMEDIATE ACTIONS\n• Audit top returned SKUs for quality or description inaccuracies\n• Tighten return policy enforcement for non-defective items\n• Add product Q&A and size guides to reduce expectation gaps\n\nSTRATEGIC RECOMMENDATIONS\n• Implement pre-shipment quality checks for flagged categories\n• Track return reason codes and feed back to buying team\n\nLOSS PREVENTION\nExcess returns will cost {loss} in 90 days if unaddressed.",
            "neg": f"Return rate is below baseline at {name} — product quality and customer satisfaction are strong. Sustain current quality controls.",
        },
        "aov": {
            "neg": f"ROOT CAUSE\nAverage order value is {abs_d:.1f}% below baseline at {name}. Customers are purchasing fewer items per visit.\n\nIMMEDIATE ACTIONS\n• Launch bundle promotions linking top categories at checkout\n• Retrain staff on upsell scripts targeting loyalty-tier customers\n• Activate minimum-spend promotions to lift basket size\n\nSTRATEGIC RECOMMENDATIONS\n• Introduce loyalty-tier AOV targets — reward customers exceeding monthly spend thresholds\n• Analyse category affinity data for personalised cross-sell recommendations\n\nLOSS PREVENTION\nAn AOV shortfall of {abs_d:.1f}% costs {loss} over 90 days.",
            "pos": f"AOV is above baseline at {name} — customers are spending more per visit. Sustain by maintaining current bundle offers and loyalty incentives.",
        },
        "discount_rate": {
            "pos": f"ROOT CAUSE\nDiscount rate is {abs_d:.1f}% above baseline at {name}. Promotions are being applied too broadly, eroding net revenue.\n\nIMMEDIATE ACTIONS\n• Cap stackable promotions — disable overlapping discount combinations\n• Restrict discount eligibility to top loyalty tiers only\n• Set a per-transaction discount ceiling of 15%\n\nSTRATEGIC RECOMMENDATIONS\n• Redesign promotion calendar to space discount events 3+ weeks apart\n• Shift to personalised offers based on customer loyalty tier\n\nLOSS PREVENTION\nExcess discounting will cost {loss} in 90 days.",
            "neg": f"Discount rate is below baseline at {name} — promotions are well-controlled. Monitor to ensure discount reduction is not suppressing conversion rates.",
        },
        "net_sales": {
            "neg": f"ROOT CAUSE\nNet sales at {name} are {abs_d:.1f}% below the cross-store baseline. Key categories and customer segments are underperforming.\n\nIMMEDIATE ACTIONS\n• Launch a flash promotion on top categories this week\n• Activate email/push campaigns targeting loyalty-tier customers in {store['region']}\n• Review stock levels — stockouts may be suppressing sales\n\nSTRATEGIC RECOMMENDATIONS\n• Benchmark {name} against top-performing peer stores for staffing and layout differences\n• Run a targeted local marketing campaign for the store's catchment area\n\nLOSS PREVENTION\nContinued underperformance will result in a {loss} revenue gap over 90 days.",
            "pos": f"Net sales are above baseline at {name} — store performance is strong. Maintain current inventory levels and staffing to sustain momentum.",
        },
    }
    side = "pos" if drift_pct > 0 else "neg"
    return templates.get(metric, {}).get(side, f"{lbl} drifted {drift_pct:+.1f}% from baseline at {name}. Actual: {fmt(actual)}, Baseline: {fmt(baseline)}.")


def run_analysis(metrics):
    baselines = {}
    for m in METRIC_LIST:
        vals = [s[m] for s in metrics if s.get(m) is not None]
        baselines[m] = sum(vals) / len(vals) if vals else 0

    alerts = []
    for store in metrics:
        for metric in METRIC_LIST:
            actual   = store[metric]
            baseline = baselines[metric]
            if not baseline:
                continue
            drift_pct = round((actual - baseline) / baseline * 100, 1)
            abs_drift = abs(drift_pct)
            if abs_drift < 12:
                continue
            sev  = severity(abs_drift)
            daily = store["net_sales"] / 30
            mult  = MULTIPLIERS[metric]
            imp   = abs_drift / 100
            l30   = round(daily * imp * mult * 30, 2)
            l60   = round(daily * imp * mult * 60, 2)
            l90   = round(daily * imp * mult * 90, 2)
            alerts.append({
                "store_id":    store["store_id"],
                "store_name":  store["store_name"],
                "address":     store["address"],
                "zip_code":    store["zip_code"],
                "metric":      metric,
                "actual":      actual,
                "baseline":    round(baseline, 2),
                "drift_pct":   drift_pct,
                "severity":    sev,
                "net_sales":   store["net_sales"],
                "loss_30d":    l30,
                "loss_60d":    l60,
                "loss_90d":    l90,
                "graph_context": graph_context_for(store, metric),
                "recommendation": recommend(store, metric, actual, baseline, drift_pct, l90),
            })

    alerts.sort(key=lambda a: {"CRITICAL":4,"HIGH":3,"MEDIUM":2,"LOW":1}.get(a["severity"],0), reverse=True)
    return alerts


if __name__ == "__main__":
    metrics = [simulate_metrics(s) for s in STORES]
    alerts  = run_analysis(metrics)

    output = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "source": "Neptune Star Schema Knowledge Graph — hourly generation",
        "metrics": metrics,
        "alerts":  alerts,
    }

    with open("drift_data.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"Generated: {len(metrics)} stores, {len(alerts)} drift alerts")
    for a in alerts:
        print(f"  {a['severity']:8s} {a['store_name']:30s} {a['metric']:15s} drift={a['drift_pct']:+.1f}%")
