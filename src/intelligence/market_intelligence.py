# src/market_reports.py
from __future__ import annotations

import io
import math
import statistics
from datetime import datetime
from typing import Any, Dict, List, Tuple

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _to_int(v: Any, default: int = 0) -> int:
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except Exception:
        return default


def _pct(n: float, d: float) -> float:
    return (100.0 * n / d) if d else 0.0


def _safe_median(nums: List[float]) -> float:
    nums = [x for x in nums if x is not None]
    if not nums:
        return 0.0
    return float(statistics.median(nums))


def _safe_mean(nums: List[float]) -> float:
    nums = [x for x in nums if x is not None]
    if not nums:
        return 0.0
    return float(sum(nums) / len(nums))


def _fmt(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return ""
        return f"{v:.2f}".rstrip("0").rstrip(".")
    return str(v)


def _hhi_from_shares(shares: List[float]) -> float:
    """HHI on 0..10,000 scale."""
    s2 = 0.0
    for s in shares:
        if s and s > 0:
            s2 += s * s
    return 10000.0 * s2


def _concentration_label(hhi_10k: float) -> str:
    if hhi_10k >= 2500:
        return "High"
    if hhi_10k >= 1500:
        return "Moderate"
    return "Low"


def _norm_pricing_model(v: Any) -> str:
    t = str(v or "").strip().lower()
    if t in {"free", "paid", "freemium", "unknown", "nonprofit-discount"}:
        return t
    if "nonprofit" in t or "non-profit" in t or "discount" in t:
        return "nonprofit-discount"
    if "trial" in t or "freemium" in t:
        return "freemium"
    if "free" in t:
        return "free"
    if "paid" in t or "$" in t or "subscription" in t:
        return "paid"
    return "unknown"


def _get_segment(r: Dict[str, Any]) -> str:
    return (str(r.get("market_segment") or r.get("appGroup") or "unknown").strip()) or "unknown"


def _get_reviews(r: Dict[str, Any]) -> int:
    if "reviews_count" in r:
        return _to_int(r.get("reviews_count"), 0)
    return _to_int(r.get("reviews"), 0)


def _get_rating(r: Dict[str, Any]) -> float | None:
    if r.get("rating") is None or r.get("rating") == "":
        return None
    x = _to_float(r.get("rating"), 0.0)
    return x if x > 0 else None


def _bayesian_adjusted_rating(rating: float | None, reviews: int, global_mean: float, m: int = 25) -> float:
    """
    adj = (v/(v+m))*R + (m/(v+m))*C
    """
    if rating is None:
        return 0.0
    v = max(0, int(reviews))
    if v + m <= 0:
        return float(global_mean)
    return (v / (v + m)) * float(rating) + (m / (v + m)) * float(global_mean)


# -----------------------------------------------------------------------------
# Public API expected by main.py
# -----------------------------------------------------------------------------
def build_market_intelligence(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    now_iso = datetime.utcnow().isoformat() + "Z"
    n = len(items or [])

    if n == 0:
        return {
            "status": "no_data",
            "generated_at": now_iso,
            "context": {"sphere": "", "category_preset": "", "app_groups": [], "total_apps": 0, "segments_count": 0},
            "message": "No apps were available after scraping and filtering. Reduce filters (minRating/pricingFilter), increase maxPages, or broaden app groups.",
            "market_overview": {"total_apps": 0, "rated_apps": 0, "avg_rating": None, "total_reviews": 0, "avg_reviews_per_app": None},
            "pricing_mix": {"catalog": {}, "attention_weighted": {}},
            "rating_distribution": {},
            "segments": [],
            "concentration": {"hhi_10k": None, "label": "None", "basis": "segment_share_by_reviews"},
            "top_apps": {"by_adjusted_score": [], "by_reviews": []},
            "opportunities": {"segments": [], "logic": "demand_high_supply_low OR dissatisfaction OR fragmentation"},
        }

    normalized: List[Dict[str, Any]] = []
    for r in items:
        rr = dict(r or {})
        if "reviews_count" not in rr and "reviews" in rr:
            rr["reviews_count"] = rr.get("reviews")
        rr["pricing_model"] = _norm_pricing_model(rr.get("pricing_model"))
        normalized.append(rr)

    sphere = str(normalized[0].get("sphere") or "").strip()
    category_preset = str(normalized[0].get("categoryPreset") or normalized[0].get("category_preset") or "").strip()
    app_groups = sorted({str(x.get("appGroup") or "").strip() for x in normalized if str(x.get("appGroup") or "").strip()})

    ratings = [x for x in (_get_rating(r) for r in normalized) if x is not None]
    reviews_all = [max(0, _get_reviews(r)) for r in normalized]
    total_reviews = int(sum(reviews_all))

    rated_apps = len(ratings)
    avg_rating = round(_safe_mean(ratings), 2) if ratings else None
    avg_reviews_per_app = round(total_reviews / n, 2) if n else None

    rating_dist = {
        "4.5+": len([x for x in ratings if x >= 4.5]),
        "4.0-4.49": len([x for x in ratings if 4.0 <= x < 4.5]),
        "3.5-3.99": len([x for x in ratings if 3.5 <= x < 4.0]),
        "<3.5": len([x for x in ratings if 0.0 < x < 3.5]),
        "unrated_or_missing": n - rated_apps,
    }

    # Pricing mix: catalog + attention-weighted (by reviews)
    pricing_catalog: Dict[str, int] = {}
    pricing_weight: Dict[str, float] = {}
    for r in normalized:
        pm = _norm_pricing_model(r.get("pricing_model"))
        pricing_catalog[pm] = pricing_catalog.get(pm, 0) + 1
        w = float(max(0, _get_reviews(r)))
        pricing_weight[pm] = pricing_weight.get(pm, 0.0) + w

    # Segments
    seg_map: Dict[str, List[Dict[str, Any]]] = {}
    for r in normalized:
        seg = _get_segment(r)
        seg_map.setdefault(seg, []).append(r)

    segments: List[Dict[str, Any]] = []
    for seg, rows in seg_map.items():
        seg_n = len(rows)
        seg_reviews = [max(0, _get_reviews(x)) for x in rows]
        seg_total_reviews = int(sum(seg_reviews))

        seg_ratings = [x for x in (_get_rating(x) for x in rows) if x is not None]
        seg_avg_rating = round(_safe_mean(seg_ratings), 2) if seg_ratings else None
        seg_median_reviews = int(_safe_median([float(x) for x in seg_reviews])) if seg_reviews else 0

        seg_pm_catalog: Dict[str, int] = {}
        seg_pm_weight: Dict[str, float] = {}
        for x in rows:
            pm = _norm_pricing_model(x.get("pricing_model"))
            seg_pm_catalog[pm] = seg_pm_catalog.get(pm, 0) + 1
            w = float(max(0, _get_reviews(x)))
            seg_pm_weight[pm] = seg_pm_weight.get(pm, 0.0) + w

        segments.append({
            "segment": seg,
            "apps": seg_n,
            "share_pct_by_apps": round(_pct(seg_n, n), 2),
            "total_reviews": seg_total_reviews,
            "share_pct_by_reviews": round(_pct(seg_total_reviews, total_reviews), 2) if total_reviews else round(_pct(seg_n, n), 2),
            "avg_rating": seg_avg_rating,
            "median_reviews": seg_median_reviews,
            "pricing_mix": {"catalog": seg_pm_catalog, "attention_weighted": seg_pm_weight},
        })

    segments.sort(key=lambda s: (-(s.get("total_reviews") or 0), -(s.get("apps") or 0), s.get("segment") or ""))

    # Concentration by reviews (attention proxy); fallback to apps if no reviews
    if total_reviews > 0:
        shares = [(s.get("total_reviews") or 0) / total_reviews for s in segments]
        basis = "segment_share_by_reviews"
    else:
        shares = [(s.get("apps") or 0) / n for s in segments]
        basis = "segment_share_by_app_count"

    hhi_10k = round(_hhi_from_shares(shares), 2)
    conc_label = _concentration_label(hhi_10k)

    # Leaders
    global_mean = float(avg_rating) if avg_rating is not None else 4.0

    def _row_to_app(r: Dict[str, Any], *, adj_score: float | None = None) -> Dict[str, Any]:
        return {
            "app_name": (r.get("app_name") or r.get("name") or "").strip(),
            "rating": _get_rating(r),
            "reviews_count": _get_reviews(r),
            "pricing_model": _norm_pricing_model(r.get("pricing_model")),
            "segment": _get_segment(r),
            "app_url": (r.get("app_url") or "").strip(),
            "adjusted_score": round(float(adj_score), 4) if adj_score is not None else None,
        }

    scored: List[Tuple[float, int, str, Dict[str, Any]]] = []
    for r in normalized:
        rating = _get_rating(r)
        reviews = _get_reviews(r)
        adj = _bayesian_adjusted_rating(rating, reviews, global_mean, m=25)
        scored.append((adj, reviews, (r.get("app_name") or r.get("name") or ""), r))

    top_by_adjusted = sorted(scored, key=lambda t: (t[0], t[1], t[2]), reverse=True)[:10]
    top_by_reviews = sorted(normalized, key=lambda r: (_get_reviews(r), _get_rating(r) or 0.0), reverse=True)[:10]

    # Opportunities (explainable)
    global_avg_reviews = (total_reviews / n) if n else 0.0
    opp_segments: List[Dict[str, Any]] = []
    for s in segments:
        apps = int(s.get("apps") or 0)
        seg_reviews = int(s.get("total_reviews") or 0)
        avg_r = s.get("avg_rating")
        median_rev = int(s.get("median_reviews") or 0)

        reasons: List[str] = []

        if apps <= 3 and seg_reviews >= max(50, int(2.0 * global_avg_reviews)):
            reasons.append("High demand proxy (reviews) with low supplier count (few apps).")

        if avg_r is not None and avg_rating is not None:
            if seg_reviews >= max(50, int(2.0 * global_avg_reviews)) and avg_r < (avg_rating - 0.15):
                reasons.append("Below-average rating with meaningful traction (potential unmet needs).")

        if avg_r is not None and avg_r >= 4.2 and median_rev <= 25 and apps >= 3:
            reasons.append("High average rating but low median reviews (quality present, adoption still developing).")

        if reasons:
            opp_segments.append({
                "segment": s.get("segment"),
                "apps": apps,
                "total_reviews": seg_reviews,
                "avg_rating": avg_r,
                "median_reviews": median_rev,
                "reasons": reasons[:3],
            })

    opp_segments.sort(key=lambda x: (-(x.get("total_reviews") or 0), -(x.get("apps") or 0), x.get("segment") or ""))

    return {
        "status": "ok",
        "generated_at": now_iso,
        "context": {
            "sphere": sphere,
            "category_preset": category_preset,
            "app_groups": app_groups,
            "total_apps": n,
            "segments_count": len(segments),
        },
        "market_overview": {
            "total_apps": n,
            "rated_apps": rated_apps,
            "avg_rating": avg_rating,
            "total_reviews": total_reviews,
            "avg_reviews_per_app": avg_reviews_per_app,
        },
        "pricing_mix": {
            "catalog": pricing_catalog,
            "attention_weighted": pricing_weight,
        },
        "rating_distribution": rating_dist,
        "segments": segments,
        "concentration": {
            "hhi_10k": hhi_10k,
            "label": conc_label,
            "basis": basis,
        },
        "top_apps": {
            "by_adjusted_score": [_row_to_app(r, adj_score=adj) for (adj, _, __, r) in top_by_adjusted],
            "by_reviews": [_row_to_app(r) for r in top_by_reviews],
        },
        "opportunities": {
            "segments": opp_segments[:10],
            "logic": "Reasons include: (apps<=3 & reviews high), or (below-average rating w/ traction), or (avg_rating>=4.2 & median_reviews<=25 & apps>=3).",
        },
    }


# main.py expects this name in many versions
def build_executive_summary(mi: Dict[str, Any]) -> str:
    return build_exec_summary(mi)


def build_exec_summary(mi: Dict[str, Any]) -> str:
    ctx = mi.get("context", {}) or {}
    overview = mi.get("market_overview", {}) or {}
    pricing = mi.get("pricing_mix", {}) or {}
    conc = mi.get("concentration", {}) or {}
    segs = mi.get("segments", []) or []
    top_adj = (mi.get("top_apps", {}) or {}).get("by_adjusted_score", [])[:5]
    top_rev = (mi.get("top_apps", {}) or {}).get("by_reviews", [])[:5]
    opp = (mi.get("opportunities", {}) or {}).get("segments", [])[:5]

    total = int(ctx.get("total_apps") or 0)

    def _line_top_apps(title: str, apps: List[Dict[str, Any]]) -> str:
        out = [title]
        for i, a in enumerate(apps, start=1):
            out.append(
                f"  {i}. {_fmt(a.get('app_name'))} | adj={_fmt(a.get('adjusted_score'))} | rating={_fmt(a.get('rating'))} | reviews={_fmt(a.get('reviews_count'))} | pricing={_fmt(a.get('pricing_model'))} | segment={_fmt(a.get('segment'))}"
            )
        return "\n".join(out)

    lines: List[str] = []
    lines.append("EXECUTIVE SUMMARY — Salesforce AppExchange Intelligence Engine")
    lines.append(f"Generated at: {_fmt(mi.get('generated_at'))}")
    lines.append("")

    if mi.get("status") == "no_data" or total == 0:
        lines.append("EXECUTIVE INSIGHT")
        lines.append("- The analyzed AppExchange slice contains **0 apps**.")
        msg = mi.get("message") or "No data available."
        lines.append(f"- Diagnostic: {msg}")
        lines.append("")
        lines.append("RECOMMENDED NEXT STEPS")
        lines.append("- Reduce filters (minRating/pricingFilter) or set minRating=0.")
        lines.append("- Increase maxPages (e.g., 5–10) to collect more listings.")
        lines.append("- Use broader appGroup/categoryPreset values (2–5 per run).")
        lines.append("")
        return "\n".join(lines)

    lines.append("1) Scope & snapshot")
    lines.append(f"- Total apps analyzed: {total}")
    lines.append(f"- Sphere: {_fmt(ctx.get('sphere'))}")
    lines.append(f"- Category preset (label): {_fmt(ctx.get('category_preset'))}")
    lines.append(f"- App groups observed: {', '.join(ctx.get('app_groups') or []) or '—'}")
    lines.append(f"- Segments identified: {int(ctx.get('segments_count') or 0)}")
    lines.append("")

    lines.append("2) Market overview (proxy metrics)")
    lines.append(f"- Rated apps: {int(overview.get('rated_apps') or 0)}")
    lines.append(f"- Average rating (rated apps): {_fmt(overview.get('avg_rating'))}")
    lines.append(f"- Total reviews (adoption proxy): {int(overview.get('total_reviews') or 0)}")
    lines.append(f"- Avg reviews per app: {_fmt(overview.get('avg_reviews_per_app'))}")
    lines.append("")

    lines.append("3) Market structure")
    lines.append(f"- Concentration (HHI 0–10,000 by segment share): {_fmt(conc.get('hhi_10k'))} → {conc.get('label')} (basis: {conc.get('basis')})")
    lines.append("- Top segments by attention (reviews):")
    for s in segs[:5]:
        lines.append(
            f"  • {s.get('segment')}: reviews={int(s.get('total_reviews') or 0)}, apps={int(s.get('apps') or 0)}, share={_fmt(s.get('share_pct_by_reviews'))}%, avg rating={_fmt(s.get('avg_rating'))}"
        )
    lines.append("")

    cat = (pricing.get("catalog") or {})
    wgt = (pricing.get("attention_weighted") or {})
    lines.append("4) Pricing landscape")
    lines.append(
        f"- Catalog mix (count): free={cat.get('free',0)}, freemium={cat.get('freemium',0)}, paid={cat.get('paid',0)}, nonprofit-discount={cat.get('nonprofit-discount',0)}, unknown={cat.get('unknown',0)}"
    )
    if overview.get("total_reviews"):
        tot_rev = float(overview.get("total_reviews") or 0.0)

        def _wshare(k: str) -> str:
            return _fmt(round(_pct(float(wgt.get(k, 0.0) or 0.0), tot_rev), 2))

        lines.append(
            f"- Attention-weighted mix (by reviews): free={_wshare('free')}%, freemium={_wshare('freemium')}%, paid={_wshare('paid')}%, nonprofit-discount={_wshare('nonprofit-discount')}%, unknown={_wshare('unknown')}%"
        )
    lines.append("")

    lines.append("5) Leaders")
    lines.append(_line_top_apps("- Top apps by quality-adjusted score (Bayesian):", top_adj))
    lines.append("")
    lines.append(_line_top_apps("- Top apps by traction (reviews):", top_rev))
    lines.append("")

    lines.append("6) Opportunities / gaps (explainable)")
    if not opp:
        lines.append("- No segments matched the current opportunity rules in this slice.")
    else:
        for s in opp:
            lines.append(
                f"- {s.get('segment')}: apps={s.get('apps')}, reviews={s.get('total_reviews')}, avg rating={_fmt(s.get('avg_rating'))}, median reviews={s.get('median_reviews')}"
            )
            for reason in (s.get("reasons") or [])[:3]:
                lines.append(f"  • {reason}")
    lines.append("")
    lines.append("Note: Vendor-level dominance is intentionally not computed (vendor field removed).")

    return "\n".join(lines)


def build_market_pdf(mi: Dict[str, Any], summary_text: str) -> bytes:
    styles = getSampleStyleSheet()
    normal = styles["BodyText"]
    title = styles["Title"]
    h2 = styles["Heading2"]

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=1.5 * cm,
        rightMargin=1.5 * cm,
        topMargin=1.3 * cm,
        bottomMargin=1.3 * cm,
    )

    story: List[Any] = []
    story.append(Paragraph("Salesforce AppExchange — Market Intelligence Report", title))
    story.append(Paragraph(f"Generated: {_fmt(mi.get('generated_at'))}", normal))
    story.append(Spacer(1, 10))

    # No-data friendly report
    if mi.get("status") == "no_data" or int((mi.get("context", {}) or {}).get("total_apps") or 0) == 0:
        story.append(Paragraph("Data availability notice", h2))
        story.append(Spacer(1, 6))
        msg = mi.get("message") or "No apps were available for analysis."
        story.append(Paragraph(msg, normal))
        story.append(Spacer(1, 10))

        story.append(Paragraph("Recommended next steps", h2))
        story.append(Spacer(1, 6))
        recs = [
            "Reduce filters (minRating/pricingFilter) or set minRating=0.",
            "Increase maxPages (e.g., 5–10) to collect more listings.",
            "Broaden appGroup/categoryPreset values (2–5 per run).",
            "Run across industries/products to compare structure and pricing mix.",
        ]
        story.append(Paragraph("<br/>".join([f"• {r}" for r in recs]), normal))
        doc.build(story)
        return buf.getvalue()

    # Executive summary bullets
    story.append(Paragraph("Executive summary (key points)", h2))
    bullets = []
    for line in summary_text.splitlines():
        if line.strip().startswith("-") or line.strip().startswith("•"):
            bullets.append(line.strip())
    if not bullets:
        total = (mi.get("context", {}) or {}).get("total_apps")
        bullets = [f"- Total apps analyzed: {_fmt(total)}"]
    story.append(Paragraph("<br/>".join(bullets[:12]), normal))
    story.append(Spacer(1, 12))

    # Exhibit 1 — Segment coverage
    story.append(Paragraph("Exhibit 1 — Segment coverage (attention proxy: reviews)", h2))
    segs = mi.get("segments", []) or []
    seg_table = [["Segment", "Apps", "Reviews", "Share %", "Avg rating"]]
    for s in segs[:15]:
        seg_table.append([
            s.get("segment", ""),
            str(int(s.get("apps") or 0)),
            str(int(s.get("total_reviews") or 0)),
            _fmt(s.get("share_pct_by_reviews", "")),
            _fmt(s.get("avg_rating", "")),
        ])
    t1 = Table(seg_table, colWidths=[7.0 * cm, 1.3 * cm, 2.0 * cm, 1.7 * cm, 2.5 * cm])
    t1.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    story.append(t1)
    story.append(Spacer(1, 12))

    # Exhibit 2 — Pricing mix
    story.append(Paragraph("Exhibit 2 — Pricing mix (catalog vs attention-weighted)", h2))
    pm = mi.get("pricing_mix", {}) or {}
    cat = pm.get("catalog", {}) or {}
    wgt = pm.get("attention_weighted", {}) or {}
    tot_reviews = float((mi.get("market_overview", {}) or {}).get("total_reviews") or 0.0)

    pm_table = [["Pricing model", "Count", "Catalog %", "Weighted share % (reviews)"]]
    total_apps = int((mi.get("context", {}) or {}).get("total_apps") or 0)
    for k in ["free", "freemium", "paid", "nonprofit-discount", "unknown"]:
        c = int(cat.get(k, 0) or 0)
        w = float(wgt.get(k, 0.0) or 0.0)
        pm_table.append([
            k,
            str(c),
            _fmt(round(_pct(c, total_apps), 2)),
            _fmt(round(_pct(w, tot_reviews), 2)) if tot_reviews else "",
        ])
    t2 = Table(pm_table, colWidths=[5.0 * cm, 2.0 * cm, 2.0 * cm, 4.0 * cm])
    t2.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    story.append(t2)
    story.append(Spacer(1, 12))

    # Exhibit 3 — Leaders
    story.append(Paragraph("Exhibit 3 — Leaders (quality-adjusted & traction)", h2))
    top_adj = (mi.get("top_apps", {}) or {}).get("by_adjusted_score", [])[:8]
    top_rev = (mi.get("top_apps", {}) or {}).get("by_reviews", [])[:8]

    def _apps_table(title_txt: str, apps: List[Dict[str, Any]]) -> Table:
        rows = [[title_txt, "", "", "", ""]]
        rows.append(["App", "Adj", "Rating", "Reviews", "Pricing"])
        for a in apps:
            rows.append([
                (a.get("app_name") or "")[:70],
                _fmt(a.get("adjusted_score")),
                _fmt(a.get("rating")),
                str(int(a.get("reviews_count") or 0)),
                _fmt(a.get("pricing_model")),
            ])
        tt = Table(rows, colWidths=[7.5 * cm, 1.3 * cm, 1.5 * cm, 1.8 * cm, 2.4 * cm])
        tt.setStyle(TableStyle([
            ("SPAN", (0, 0), (-1, 0)),
            ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("BACKGROUND", (0, 1), (-1, 1), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("FONTNAME", (0, 1), (-1, 1), "Helvetica-Bold"),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))
        return tt

    story.append(_apps_table("Top by quality-adjusted score (Bayesian)", top_adj))
    story.append(Spacer(1, 8))
    story.append(_apps_table("Top by traction (reviews)", top_rev))
    story.append(Spacer(1, 12))

    # Exhibit 4 — Concentration & opportunities
    story.append(Paragraph("Exhibit 4 — Concentration & opportunities", h2))
    conc = mi.get("concentration", {}) or {}
    story.append(Paragraph(
        f"Concentration (HHI 0–10,000 by segment share): {_fmt(conc.get('hhi_10k'))} — {conc.get('label')} (basis: {conc.get('basis')})",
        normal
    ))
    story.append(Spacer(1, 6))

    opp = (mi.get("opportunities", {}) or {}).get("segments", [])[:10]
    opp_table = [["Opportunity segment", "Apps", "Reviews", "Avg rating"]]
    if opp:
        for s in opp:
            opp_table.append([
                s.get("segment", ""),
                str(int(s.get("apps") or 0)),
                str(int(s.get("total_reviews") or 0)),
                _fmt(s.get("avg_rating", "")),
            ])
    else:
        opp_table.append(["(none matched rules in this slice)", "", "", ""])
    t4 = Table(opp_table, colWidths=[7.8 * cm, 1.3 * cm, 2.0 * cm, 2.5 * cm])
    t4.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    story.append(t4)

    doc.build(story)
    return buf.getvalue()