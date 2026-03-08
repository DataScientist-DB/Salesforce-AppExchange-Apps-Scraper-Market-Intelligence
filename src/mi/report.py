from __future__ import annotations

import io
import math
from collections import Counter, defaultdict
from datetime import datetime
from statistics import median
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from apify import Actor


def _utc_now_z() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default


def _to_int(v: Any, default: int = 0) -> int:
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except Exception:
        return default


def _safe_mean(nums: List[float]) -> float:
    vals = [float(x) for x in nums if x is not None]
    if not vals:
        return 0.0
    return round(sum(vals) / len(vals), 2)


def _safe_median_int(nums: List[int]) -> int:
    vals = [int(x) for x in nums if x is not None]
    if not vals:
        return 0
    return int(median(vals))


def _norm(s: Any) -> str:
    return str(s or "").strip()


def _json_safe(v: Any) -> Any:
    if v is None:
        return None

    if isinstance(v, (str, int, bool)):
        return v

    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v

    if isinstance(v, dict):
        return {str(k): _json_safe(vv) for k, vv in v.items()}

    if isinstance(v, (list, tuple, set)):
        return [_json_safe(x) for x in v]

    return str(v)


def _records_from_df(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    return df.where(pd.notnull(df), None).to_dict(orient="records")


def _pct(x: float) -> str:
    return f"{round((x or 0.0) * 100, 1)}%"


def _hhi_from_shares(shares: List[float]) -> float:
    return sum((s * 100.0) ** 2 for s in shares if s > 0)


def _hhi_label(hhi: float) -> str:
    if hhi >= 2500:
        return "Highly concentrated"
    if hhi >= 1500:
        return "Moderately concentrated"
    return "Unconcentrated"


def _rank_apps(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []
    for r in items:
        rating = _to_float(r.get("rating"), 0.0)
        reviews = _to_int(r.get("reviews"), 0)
        score = (reviews ** 0.5) * (0.6 + rating / 5.0)
        rr = dict(r)
        rr["adoption_score"] = round(score, 4)
        ranked.append(rr)

    ranked.sort(
        key=lambda x: (
            _to_float(x.get("adoption_score"), 0.0),
            _to_int(x.get("reviews"), 0),
            _to_float(x.get("rating"), 0.0),
        ),
        reverse=True,
    )
    return ranked


def classify_app_position(rating: float | None, reviews: int | None) -> str:
    r = rating or 0.0
    v = reviews or 0

    if r >= 4.6 and v >= 200:
        return "mature_leader"
    if r >= 4.6 and v < 200:
        return "under_discovered_challenger"
    if r < 4.2 and v >= 200:
        return "vulnerable_incumbent"
    return "mid_market"


def _compute_opportunity_score(app: Dict[str, Any], stats: Dict[str, Any]) -> float:
    score = 0.0

    rating = _to_float(app.get("rating"), 0.0)
    reviews = _to_int(app.get("reviews"), 0)
    pricing_model = str(app.get("pricing_model") or "").strip().lower()

    median_reviews = _to_int(stats.get("median_reviews"), 0)
    avg_rating = _to_float(stats.get("avg_rating"), 0.0)

    if rating >= 4.5:
        score += 2.0
    elif rating >= 4.2:
        score += 1.0

    if median_reviews > 0:
        if reviews <= median_reviews:
            score += 1.5
        else:
            score -= 0.5

    if pricing_model in {"free", "freemium"}:
        score += 0.5

    if rating > avg_rating and (median_reviews == 0 or reviews < median_reviews):
        score += 1.5

    if pricing_model == "unknown":
        score -= 0.25

    return round(score, 2)


def _build_segment_rows(items: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    counts = Counter([_norm(x.get("market_segment")) or "unknown" for x in items])
    total = sum(counts.values()) or 1

    rows: List[Dict[str, Any]] = []
    for name, c in counts.most_common():
        rows.append(
            {
                "segment": name,
                "apps": c,
                "share": round(c / total, 4),
            }
        )

    shares = [r["share"] for r in rows]
    hhi = round(_hhi_from_shares(shares), 1)
    cr4 = round(sum(r["share"] for r in rows[:4]), 4)

    concentration = {
        "segment_hhi": hhi,
        "segment_hhi_label": _hhi_label(hhi),
        "segment_cr4": cr4,
    }
    return rows, concentration


def _build_gap_rows(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seg_counts = Counter([_norm(x.get("market_segment")) or "unknown" for x in items])
    seg_reviews: Dict[str, List[int]] = defaultdict(list)

    for r in items:
        seg = _norm(r.get("market_segment")) or "unknown"
        seg_reviews[seg].append(_to_int(r.get("reviews"), 0))

    gap_candidates: List[Tuple[str, int, float]] = []
    for seg, count in seg_counts.items():
        rv = seg_reviews.get(seg, [])
        avg_reviews = round(sum(rv) / len(rv), 1) if rv else 0.0
        gap_candidates.append((seg, count, avg_reviews))

    gap_candidates.sort(key=lambda x: (x[1], -x[2], x[0]))

    return [
        {
            "segment": seg,
            "apps": count,
            "avg_reviews": avg_reviews,
        }
        for seg, count, avg_reviews in gap_candidates[:10]
    ]


def build_market_intelligence(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    total_apps = len(records)

    ratings = [_to_float(r.get("rating"), 0.0) for r in records if r.get("rating") not in (None, "")]
    ratings = [x for x in ratings if x > 0]

    reviews = [_to_int(r.get("reviews"), 0) for r in records if r.get("reviews") not in (None, "")]
    reviews = [x for x in reviews if x >= 0]

    avg_rating = _safe_mean(ratings)
    avg_reviews = _safe_mean([float(x) for x in reviews]) if reviews else 0.0
    median_reviews = _safe_median_int(reviews)

    stats = {
        "total_apps": total_apps,
        "avg_rating": avg_rating,
        "avg_reviews": avg_reviews,
        "median_reviews": median_reviews,
    }

    pricing_distribution: Dict[str, int] = {}
    segment_distribution: Dict[str, int] = {}
    rating_bucket_distribution: Dict[str, int] = {}
    review_bucket_distribution: Dict[str, int] = {}
    position_counts: Dict[str, int] = {
        "mature_leader": 0,
        "under_discovered_challenger": 0,
        "vulnerable_incumbent": 0,
        "mid_market": 0,
    }

    enriched: List[Dict[str, Any]] = []

    for r in records:
        rr = dict(r)

        rr["market_position"] = classify_app_position(
            _to_float(rr.get("rating"), 0.0),
            _to_int(rr.get("reviews"), 0),
        )
        rr["opportunity_score"] = _compute_opportunity_score(rr, stats)

        pricing_key = str(rr.get("pricing_model") or "unknown")
        segment_key = str(rr.get("market_segment") or "unknown")
        rating_bucket_key = str(rr.get("rating_bucket") or "unrated")
        review_bucket_key = str(rr.get("reviews_bucket") or "0")

        pricing_distribution[pricing_key] = pricing_distribution.get(pricing_key, 0) + 1
        segment_distribution[segment_key] = segment_distribution.get(segment_key, 0) + 1
        rating_bucket_distribution[rating_bucket_key] = rating_bucket_distribution.get(rating_bucket_key, 0) + 1
        review_bucket_distribution[review_bucket_key] = review_bucket_distribution.get(review_bucket_key, 0) + 1

        pos = rr["market_position"]
        position_counts[pos] = position_counts.get(pos, 0) + 1

        enriched.append(rr)

    segment_rows, concentration = _build_segment_rows(enriched)
    gap_rows = _build_gap_rows(enriched)

    ranked_by_adoption = _rank_apps(enriched)

    high_rating_low_reviews = [
        r for r in enriched
        if _to_float(r.get("rating"), 0.0) >= 4.5 and _to_int(r.get("reviews"), 0) <= median_reviews
    ]

    review_rich_low_rated = [
        r for r in enriched
        if _to_int(r.get("reviews"), 0) >= max(10, median_reviews) and 0 < _to_float(r.get("rating"), 0.0) < 4.0
    ]

    under_discovered_apps = [
        r for r in enriched
        if r.get("market_position") == "under_discovered_challenger"
    ]

    under_discovered_apps = sorted(
        under_discovered_apps,
        key=lambda x: (
            _to_float(x.get("rating"), 0.0),
            -_to_int(x.get("reviews"), 0),
        ),
        reverse=True,
    )

    top_opportunity_apps = sorted(
        enriched,
        key=lambda x: (
            _to_float(x.get("opportunity_score"), 0.0),
            _to_float(x.get("rating"), 0.0),
            -_to_int(x.get("reviews"), 0),
        ),
        reverse=True,
    )[:10]

    top_apps = ranked_by_adoption[:10]

    competitive_insights: List[str] = []
    if total_apps:
        if pricing_distribution.get("paid", 0) > pricing_distribution.get("free", 0) + pricing_distribution.get("freemium", 0):
            competitive_insights.append(
                "Paid apps dominate the sampled category, suggesting room for freemium acquisition strategies."
            )
        if high_rating_low_reviews:
            competitive_insights.append(
                "Several high-rated apps still have relatively low review counts, indicating under-discovered competitors."
            )
        if review_rich_low_rated:
            competitive_insights.append(
                "Some established apps have meaningful review volume but weaker ratings, suggesting replacement opportunities."
            )
        if segment_rows:
            competitive_insights.append(
                f"Top segment is {segment_rows[0]['segment']} ({_pct(segment_rows[0]['share'])}); concentration is {concentration['segment_hhi_label']}."
            )

    customer_signals: List[str] = []
    if avg_rating >= 4.3:
        customer_signals.append("Overall customer satisfaction appears strong in the sampled market.")
    elif 0 < avg_rating < 4.0:
        customer_signals.append("Average ratings suggest visible customer pain points or unmet expectations.")
    if review_rich_low_rated:
        customer_signals.append(
            "Low-rated incumbents with meaningful review history may indicate demand for better UX, support, or pricing clarity."
        )

    market_trends: List[str] = []
    if pricing_distribution.get("freemium", 0) > 0:
        market_trends.append("Freemium/trial patterns are present, indicating acquisition-led go-to-market behavior.")
    if pricing_distribution.get("unknown", 0) > 0:
        market_trends.append("A notable share of apps do not expose clear pricing, which reduces market transparency.")
    if segment_rows:
        market_trends.append(
            f"Most sampled listings fall into the segment: {segment_rows[0]['segment']}."
        )

    opportunity_signals = [
        {
            "type": "high_rating_low_reviews",
            "title": "Undiscovered high-performing apps",
            "count": len(high_rating_low_reviews),
            "examples": [
                {
                    "app_name": r.get("app_name"),
                    "app_url": r.get("app_url"),
                    "rating": r.get("rating"),
                    "reviews": r.get("reviews"),
                    "opportunity_score": r.get("opportunity_score"),
                    "market_position": r.get("market_position"),
                }
                for r in high_rating_low_reviews[:5]
            ],
        },
        {
            "type": "review_rich_low_rated",
            "title": "Established but vulnerable apps",
            "count": len(review_rich_low_rated),
            "examples": [
                {
                    "app_name": r.get("app_name"),
                    "app_url": r.get("app_url"),
                    "rating": r.get("rating"),
                    "reviews": r.get("reviews"),
                    "market_position": r.get("market_position"),
                }
                for r in review_rich_low_rated[:5]
            ],
        },
        {
            "type": "segment_gap_analysis",
            "title": "Segment gaps",
            "count": len(gap_rows),
            "examples": gap_rows[:5],
        },
    ]

    recommendations: List[str] = []
    if len(under_discovered_apps) > 0:
        recommendations.append(
            "Prioritize under-discovered challengers that combine strong ratings with relatively low review visibility."
        )
    if pricing_distribution.get("unknown", 0) > 0:
        recommendations.append(
            "Improve pricing transparency analysis because a meaningful share of listings do not clearly expose pricing."
        )
    if review_rich_low_rated:
        recommendations.append(
            "Review low-rated incumbents with large review footprints for displacement opportunities."
        )
    if gap_rows:
        recommendations.append(
            "Review low-supply segments with higher average reviews to identify expansion or acquisition opportunities."
        )
    if not recommendations:
        recommendations.append(
            "The sampled market appears balanced; continue monitoring category leaders, pricing shifts, and review momentum."
        )

    return _json_safe(
        {
            "report_metadata": {
                "report_type": "salesforce_appexchange_market_intelligence",
                "generated_at": _utc_now_z(),
                "sample_size": total_apps,
                "schema_version": "1.1",
            },
            "market_overview": {
                "total_apps": total_apps,
                "avg_rating": avg_rating,
                "avg_reviews": avg_reviews,
                "median_reviews": median_reviews,
            },
            "competitive_landscape": {
                "position_counts": position_counts,
                "competitive_insights": competitive_insights,
                "concentration": concentration,
            },
            "pricing_analysis": {
                "pricing_distribution": pricing_distribution,
            },
            "category_analysis": {
                "segment_distribution": segment_distribution,
                "segment_rows": segment_rows,
                "rating_bucket_distribution": rating_bucket_distribution,
                "review_bucket_distribution": review_bucket_distribution,
                "gap_rows": gap_rows,
            },
            "customer_signals": {
                "signals": customer_signals,
            },
            "opportunity_analysis": {
                "under_discovered_count": len(under_discovered_apps),
                "review_rich_low_rated_count": len(review_rich_low_rated),
                "opportunity_signals": opportunity_signals,
            },
            "market_trends": {
                "signals": market_trends,
            },
            "top_apps": [
                {
                    "app_name": r.get("app_name"),
                    "app_url": r.get("app_url"),
                    "rating": r.get("rating"),
                    "reviews": r.get("reviews"),
                    "pricing_model": r.get("pricing_model"),
                    "market_segment": r.get("market_segment"),
                    "market_position": r.get("market_position"),
                    "adoption_score": r.get("adoption_score"),
                }
                for r in top_apps
            ],
            "undiscovered_apps": [
                {
                    "app_name": r.get("app_name"),
                    "app_url": r.get("app_url"),
                    "rating": r.get("rating"),
                    "reviews": r.get("reviews"),
                    "pricing_model": r.get("pricing_model"),
                    "market_segment": r.get("market_segment"),
                    "market_position": r.get("market_position"),
                    "opportunity_score": r.get("opportunity_score"),
                }
                for r in under_discovered_apps[:10]
            ],
            "top_opportunity_apps": [
                {
                    "app_name": r.get("app_name"),
                    "app_url": r.get("app_url"),
                    "rating": r.get("rating"),
                    "reviews": r.get("reviews"),
                    "pricing_model": r.get("pricing_model"),
                    "market_segment": r.get("market_segment"),
                    "market_position": r.get("market_position"),
                    "opportunity_score": r.get("opportunity_score"),
                }
                for r in top_opportunity_apps
            ],
            "recommendations": recommendations,
        }
    )


def build_exec_summary(market_intelligence: Dict[str, Any]) -> str:
    overview = market_intelligence.get("market_overview", {}) or {}
    competitive_landscape = market_intelligence.get("competitive_landscape", {}) or {}
    opportunity_analysis = market_intelligence.get("opportunity_analysis", {}) or {}
    category_analysis = market_intelligence.get("category_analysis", {}) or {}
    customer_signals_obj = market_intelligence.get("customer_signals", {}) or {}
    market_trends_obj = market_intelligence.get("market_trends", {}) or {}
    pricing_analysis = market_intelligence.get("pricing_analysis", {}) or {}

    total_apps = overview.get("total_apps", 0)
    avg_rating = overview.get("avg_rating", 0.0)
    avg_reviews = overview.get("avg_reviews", 0.0)

    pc = competitive_landscape.get("position_counts", {}) or {}
    conc = competitive_landscape.get("concentration", {}) or {}
    insights = competitive_landscape.get("competitive_insights", []) or []
    customer_signals = customer_signals_obj.get("signals", []) or []
    market_trends = market_trends_obj.get("signals", []) or []
    pricing_distribution = pricing_analysis.get("pricing_distribution", {}) or {}
    segment_rows = category_analysis.get("segment_rows", []) or []
    gap_rows = category_analysis.get("gap_rows", []) or []
    ud_count = opportunity_analysis.get("under_discovered_count", 0)

    lines: List[str] = []

    lines.append("EXECUTIVE INSIGHT")
    lines.append(f"- The analyzed AppExchange slice contains {total_apps} apps.")
    lines.append(f"- Average rating is {avg_rating}; average reviews per app are {avg_reviews}.")
    lines.append(
        f"- Competitive profile: mature leaders={pc.get('mature_leader', 0)}, "
        f"under-discovered challengers={pc.get('under_discovered_challenger', 0)}, "
        f"vulnerable incumbents={pc.get('vulnerable_incumbent', 0)}, "
        f"mid-market={pc.get('mid_market', 0)}."
    )
    if conc:
        lines.append(
            f"- Segment concentration is {conc.get('segment_hhi_label', 'unknown')} "
            f"(HHI={conc.get('segment_hhi', 0)}, CR4={_pct(conc.get('segment_cr4', 0.0))})."
        )
    lines.append(f"- Key opportunity: under-discovered high-performing apps ({ud_count} identified).")

    lines.append("")
    lines.append("1) MARKET OVERVIEW")
    lines.append(f"- Total apps: {total_apps}")
    lines.append(f"- Average rating: {avg_rating}")
    lines.append(f"- Average reviews: {avg_reviews}")

    lines.append("")
    lines.append("2) SEGMENT STRUCTURE")
    for r in segment_rows[:6]:
        lines.append(f"- {r['segment']}: {r['apps']} apps ({_pct(r['share'])})")

    lines.append("")
    lines.append("3) PRICING LANDSCAPE")
    if pricing_distribution:
        for k, v in sorted(pricing_distribution.items(), key=lambda kv: kv[1], reverse=True):
            lines.append(f"- {k}: {v}")

    lines.append("")
    lines.append("4) COMPETITIVE LANDSCAPE")
    lines.append(f"- Mature leaders: {pc.get('mature_leader', 0)}")
    lines.append(f"- Under-discovered challengers: {pc.get('under_discovered_challenger', 0)}")
    lines.append(f"- Vulnerable incumbents: {pc.get('vulnerable_incumbent', 0)}")
    lines.append(f"- Mid-market apps: {pc.get('mid_market', 0)}")

    lines.append("")
    lines.append("5) OPPORTUNITY / GAPS")
    lines.append(f"- Undiscovered high-performing apps: {ud_count}")
    for g in gap_rows[:5]:
        lines.append(f"- {g['segment']}: {g['apps']} apps, avg reviews={g['avg_reviews']}")

    if insights:
        lines.append("")
        lines.append("6) EVIDENCE-BASED INSIGHTS")
        for x in insights[:4]:
            lines.append(f"- {x}")

    if customer_signals:
        lines.append("")
        lines.append("7) CUSTOMER SIGNALS")
        for x in customer_signals[:3]:
            lines.append(f"- {x}")

    if market_trends:
        lines.append("")
        lines.append("8) MARKET TRENDS")
        for x in market_trends[:3]:
            lines.append(f"- {x}")

    recommendations = market_intelligence.get("recommendations", []) or []
    if recommendations:
        lines.append("")
        lines.append("RECOMMENDED NEXT STEPS")
        for x in recommendations[:4]:
            lines.append(f"- {x}")

    return "\n".join(lines).strip()


def build_llm_market_summary(
    records: List[Dict[str, Any]],
    market_intelligence: Dict[str, Any],
    exec_summary: str,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    top_apps = market_intelligence.get("top_apps", []) or []

    return _json_safe(
        {
            "report_type": "salesforce_appexchange_market_summary",
            "generated_at": _utc_now_z(),
            "input_context": {
                "mode": config.get("mode"),
                "categoryGroup": config.get("categoryGroup"),
                "sphere": config.get("sphere"),
                "categoryPreset": config.get("categoryPreset"),
                "appGroup": config.get("appGroup"),
                "maxPages": config.get("maxPages"),
                "minRating": config.get("minRating"),
                "pricingFilter": config.get("pricingFilter"),
            },
            "market_overview": market_intelligence.get("market_overview", {}),
            "competitive_landscape": market_intelligence.get("competitive_landscape", {}),
            "pricing_analysis": market_intelligence.get("pricing_analysis", {}),
            "category_analysis": market_intelligence.get("category_analysis", {}),
            "customer_signals": market_intelligence.get("customer_signals", {}),
            "opportunity_analysis": market_intelligence.get("opportunity_analysis", {}),
            "market_trends": market_intelligence.get("market_trends", {}),
            "top_apps": top_apps[:10],
            "executive_summary": exec_summary,
            "recommendations": market_intelligence.get("recommendations", []),
        }
    )


def _build_market_pdf_bytes(mi: Dict[str, Any], summary: str) -> bytes:
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import cm
        from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle, PageBreak
    except Exception:
        return b""

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=1.8 * cm,
        rightMargin=1.8 * cm,
        topMargin=1.6 * cm,
        bottomMargin=1.6 * cm,
        title="Salesforce AppExchange Market Report",
        author="Salesforce AppExchange Intelligence Engine",
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("TitleX", parent=styles["Title"], fontSize=18, leading=22, spaceAfter=8)
    h2 = ParagraphStyle("H2X", parent=styles["Heading2"], fontSize=12, leading=15, spaceAfter=6)
    body = ParagraphStyle("BodyX", parent=styles["BodyText"], fontSize=9.2, leading=12, spaceAfter=4)

    def table(data: List[List[str]], col_widths: Optional[List[float]] = None, header: bool = True):
        t = Table(data, colWidths=col_widths)
        style = TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 8.5),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                ("LINEBELOW", (0, 0), (-1, -1), 0.25, colors.grey),
            ]
        )
        if header and len(data) > 0:
            style.add("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937"))
            style.add("TEXTCOLOR", (0, 0), (-1, 0), colors.white)
            style.add("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold")
        t.setStyle(style)
        return t

    story: List[Any] = []

    story.append(Paragraph("Salesforce AppExchange Market Intelligence Report", title_style))
    story.append(Paragraph(f"Generated at: {_norm((mi.get('report_metadata') or {}).get('generated_at'))}", body))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Executive Summary", h2))
    for line in summary.splitlines():
        if line.strip():
            story.append(Paragraph(line, body))
        else:
            story.append(Spacer(1, 5))

    story.append(PageBreak())

    overview = mi.get("market_overview", {}) or {}
    comp = mi.get("competitive_landscape", {}) or {}
    conc = comp.get("concentration", {}) or {}
    cat = mi.get("category_analysis", {}) or {}
    seg_rows = cat.get("segment_rows", []) or []
    gap_rows = cat.get("gap_rows", []) or []
    pricing_distribution = (mi.get("pricing_analysis", {}) or {}).get("pricing_distribution", {}) or {}
    top_apps = mi.get("top_apps", []) or []
    recommendations = mi.get("recommendations", []) or []

    story.append(Paragraph("1. Market Overview", h2))
    overview_table = [
        ["Metric", "Value"],
        ["Total apps", str(overview.get("total_apps", 0))],
        ["Average rating", str(overview.get("avg_rating", 0.0))],
        ["Average reviews", str(overview.get("avg_reviews", 0.0))],
        ["Median reviews", str(overview.get("median_reviews", 0))],
        ["Segment HHI", str(conc.get("segment_hhi", 0))],
        ["Segment concentration", _norm(conc.get("segment_hhi_label"))],
        ["CR4", _pct(conc.get("segment_cr4", 0.0))],
    ]
    story.append(table(overview_table, col_widths=[6.2 * cm, 8.2 * cm]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("2. Segment Structure", h2))
    seg_table = [["Segment", "Apps", "Share"]]
    for r in seg_rows[:12]:
        seg_table.append([_norm(r.get("segment")), str(r.get("apps", 0)), _pct(r.get("share", 0.0))])
    story.append(table(seg_table, col_widths=[9.0 * cm, 2.2 * cm, 2.5 * cm]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("3. Pricing Landscape", h2))
    pr_table = [["Pricing", "Apps"]]
    for k, v in sorted(pricing_distribution.items(), key=lambda kv: kv[1], reverse=True):
        pr_table.append([str(k), str(v)])
    story.append(table(pr_table, col_widths=[8.0 * cm, 3.0 * cm]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("4. Leading Apps (Adoption proxy)", h2))
    top_table = [["App", "Segment", "Rating", "Reviews", "Adoption score"]]
    for r in top_apps[:12]:
        top_table.append(
            [
                _norm(r.get("app_name")),
                _norm(r.get("market_segment")),
                str(r.get("rating")),
                str(r.get("reviews")),
                str(r.get("adoption_score", "")),
            ]
        )
    story.append(table(top_table, col_widths=[5.0 * cm, 5.0 * cm, 1.5 * cm, 1.7 * cm, 2.3 * cm]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("5. Opportunity / Gaps", h2))
    gap_table = [["Segment", "Apps", "Avg reviews"]]
    for g in gap_rows[:10]:
        gap_table.append([_norm(g.get("segment")), str(g.get("apps", 0)), str(g.get("avg_reviews", 0.0))])
    story.append(table(gap_table, col_widths=[9.4 * cm, 2.0 * cm, 2.8 * cm]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("6. Recommendations", h2))
    for rec in recommendations[:6]:
        story.append(Paragraph(f"- {rec}", body))

    doc.build(story)
    return buffer.getvalue()


async def generate_and_save_reports(
    *,
    df: pd.DataFrame,
    config: Dict[str, Any],
    analysis_options: Optional[Dict[str, Any]] = None,
    kv_prefix: str = "",
) -> Dict[str, Any]:
    analysis_options = analysis_options or {}

    enable_exec_summary = bool(analysis_options.get("enableExecutiveSummary", True))
    output_json_report = bool(analysis_options.get("outputJsonReport", True))
    output_pdf_report = bool(analysis_options.get("outputPdfReport", True))

    prefix = f"{kv_prefix}" if kv_prefix else ""

    def _k(name: str) -> str:
        return f"{prefix}{name}"

    Actor.log.info("[MI] Building market intelligence outputs...")

    records = _records_from_df(df)
    market_intelligence = build_market_intelligence(records)
    exec_summary = build_exec_summary(market_intelligence) if enable_exec_summary else ""
    llm_summary = build_llm_market_summary(records, market_intelligence, exec_summary, config)

    report_generated = False
    llm_generated = False
    exec_generated = False

    if output_json_report:
        await Actor.set_value(
            _k("MARKET_INTELLIGENCE.json"),
            _json_safe(market_intelligence),
            content_type="application/json",
        )

        await Actor.set_value(
            _k("LLM_MARKET_SUMMARY.json"),
            _json_safe(llm_summary),
            content_type="application/json",
        )
        llm_generated = True

    if exec_summary:
        await Actor.set_value(
            _k("EXECUTIVE_SUMMARY.txt"),
            exec_summary,
            content_type="text/plain",
        )
        exec_generated = True

    if output_pdf_report:
        pdf_bytes = _build_market_pdf_bytes(market_intelligence, exec_summary)
        if pdf_bytes:
            await Actor.set_value(
                _k("MARKET_REPORT.pdf"),
                pdf_bytes,
                content_type="application/pdf",
            )
            report_generated = True
            Actor.log.info("[MI] ✅ Saved PDF (prefix='%s')", prefix)
        else:
            Actor.log.warning("[MI] PDF generation skipped because reportlab is unavailable.")

    # Charge only when the premium market intelligence/report output was actually generated.
    # Keep monetization simple and controlled: one premium event per successful report package.
    if report_generated:
        await Actor.charge(event_name="market-intelligence-report")
        Actor.log.info("[MI] ✅ Charged event: market-intelligence-report")
    elif llm_generated or exec_generated:
        # Optional fallback:
        # if you want to charge even when JSON/TXT outputs exist but PDF is disabled,
        # uncomment the next two lines and remove this comment block.
        #
        # await Actor.charge(event_name="market-intelligence-report")
        # Actor.log.info("[MI] ✅ Charged event: market-intelligence-report (non-PDF outputs)")
        pass

    Actor.log.info(
        "[MI] ✅ Saved JSON/TXT outputs (prefix='%s') keys=%s",
        prefix,
        [
            _k("MARKET_INTELLIGENCE.json"),
            _k("EXECUTIVE_SUMMARY.txt"),
            _k("LLM_MARKET_SUMMARY.json"),
        ],
    )

    return {
        "market_json_key": _k("MARKET_INTELLIGENCE.json"),
        "exec_summary_key": _k("EXECUTIVE_SUMMARY.txt"),
        "llm_summary_key": _k("LLM_MARKET_SUMMARY.json"),
        "pdf_key": _k("MARKET_REPORT.pdf"),
        "records": len(records),
    }