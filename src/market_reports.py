# src/market_reports.py
from __future__ import annotations

import math
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional

# PDF rendering
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    PageBreak,
)

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _norm(s: Any) -> str:
    return str(s or "").strip()

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
        return int(v)
    except Exception:
        return default

def _safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0

def _pct(a: float) -> str:
    return f"{a*100:.1f}%"

def _topn(items: List[Tuple[Any, Any]], n: int) -> List[Tuple[Any, Any]]:
    return items[: max(0, n)]

def _hhi_from_shares(shares: List[float]) -> float:
    # HHI = sum( (share*100)^2 ) -> 0..10000
    return sum([(s * 100.0) ** 2 for s in shares if s > 0])

def _hhi_label(hhi: float) -> str:
    # Common interpretation (often used in antitrust):
    # < 1500 unconcentrated, 1500-2500 moderately, >2500 highly
    if hhi >= 2500:
        return "Highly concentrated"
    if hhi >= 1500:
        return "Moderately concentrated"
    return "Unconcentrated"

def _infer_segment(rec: Dict[str, Any]) -> str:
    # Prefer your computed field if present
    seg = _norm(rec.get("market_segment"))
    if seg:
        return seg
    # fallback to appGroup/categoryPreset
    cg = _norm(rec.get("categoryGroup") or rec.get("category_group") or rec.get("sphere"))
    ag = _norm(rec.get("appGroup") or rec.get("app_group"))
    cp = _norm(rec.get("categoryPreset") or rec.get("category_preset"))
    return " / ".join([x for x in [cg, ag or cp] if x]) or "unknown"

def _infer_listing_id(rec: Dict[str, Any]) -> Optional[str]:
    lid = _norm(rec.get("listing_id"))
    if lid:
        return lid
    url = _norm(rec.get("app_url") or rec.get("url"))
    if "listingId=" in url:
        return url.split("listingId=", 1)[1].split("&", 1)[0].strip() or None
    return None

def _pricing_bucket(rec: Dict[str, Any]) -> str:
    # Standardize pricing label
    x = _norm(rec.get("pricing_model") or rec.get("pricing") or "")
    x = x.lower()
    if x in {"free", "freemium", "paid", "unknown", "nonprofit-discount"}:
        return x
    # fallback from price_text if you have it
    t = _norm(rec.get("price") or rec.get("price_text")).lower()
    if "trial" in t or "try it free" in t or "free trial" in t or "freemium" in t:
        return "freemium"
    if "free" in t:
        return "free"
    if "$" in t or "/month" in t or "per user" in t or "subscription" in t:
        return "paid"
    if "nonprofit" in t or "non-profit" in t or "discount" in t:
        return "nonprofit-discount"
    return "unknown"

def _rank_apps(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # “Adoption score” proxy: reviews_count weighted + rating
    # NOTE: without time-series, this is not review velocity; it's a static adoption signal.
    ranked = []
    for r in items:
        rating = _to_float(r.get("rating"), 0.0)
        reviews = _to_int(r.get("reviews") or r.get("reviews_count"), 0)
        score = (reviews ** 0.5) * (0.6 + rating / 5.0)  # gentle weight
        rr = dict(r)
        rr["_adoption_score"] = round(score, 4)
        ranked.append(rr)
    ranked.sort(key=lambda x: (x.get("_adoption_score", 0), _to_int(x.get("reviews") or x.get("reviews_count"), 0)), reverse=True)
    return ranked

def _concentration(items: List[Dict[str, Any]], key: str) -> Dict[str, Any]:
    counts = Counter([_norm(x.get(key)) or "unknown" for x in items])
    total = sum(counts.values()) or 1
    shares = [c / total for c in counts.values()]
    hhi = _hhi_from_shares(shares)

    top = counts.most_common(8)
    top_rows = []
    for name, c in top:
        top_rows.append({"name": name, "count": c, "share": c / total})

    # CR4 (top 4 share)
    cr4 = sum([r["share"] for r in top_rows[:4]]) if top_rows else 0.0

    return {
        "total": total,
        "hhi": round(hhi, 1),
        "hhi_label": _hhi_label(hhi),
        "cr4": round(cr4, 4),
        "top": top_rows,
    }

# ---------------------------------------------------------------------
# Core builders
# ---------------------------------------------------------------------
def build_market_intelligence(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Builds an evidence-based market intelligence JSON (non-generic):
    - market overview
    - segment structure + concentration metrics
    - pricing landscape
    - leaders table (top apps by adoption proxy)
    - “gaps” (segments with demand signals but low supply)
    """
    now = datetime.utcnow().isoformat() + "Z"

    cleaned: List[Dict[str, Any]] = []
    for r in items:
        rr = dict(r)

        rr["listing_id"] = _infer_listing_id(rr)
        rr["segment"] = _infer_segment(rr)
        rr["pricing_bucket"] = _pricing_bucket(rr)

        # normalize key fields
        rr["app_name"] = _norm(rr.get("app_name") or rr.get("name"))
        rr["app_url"] = _norm(rr.get("app_url") or rr.get("url"))
        rr["rating"] = _to_float(rr.get("rating"), 0.0) if rr.get("rating") is not None else None
        rr["reviews_count"] = _to_int(rr.get("reviews") or rr.get("reviews_count"), 0)

        cleaned.append(rr)

    total_apps = len([r for r in cleaned if r.get("app_url")])
    if total_apps == 0:
        return {
            "generated_at": now,
            "overview": {"total_apps": 0},
            "note": "No items available.",
        }

    # Segment stats
    seg_counts = Counter([r["segment"] for r in cleaned])
    seg_total = sum(seg_counts.values()) or 1
    seg_rows = []
    for seg, c in seg_counts.most_common():
        seg_rows.append({"segment": seg, "apps": c, "share": c / seg_total})

    seg_hhi = _hhi_from_shares([r["apps"] / seg_total for r in seg_rows])

    # Pricing stats
    pricing_counts = Counter([r["pricing_bucket"] for r in cleaned])
    pricing_total = sum(pricing_counts.values()) or 1
    pricing_rows = []
    for p, c in pricing_counts.most_common():
        pricing_rows.append({"pricing": p, "apps": c, "share": c / pricing_total})

    # Rating distribution
    ratings = [r["rating"] for r in cleaned if isinstance(r.get("rating"), (int, float)) and r.get("rating") is not None]
    avg_rating = round(sum(ratings) / len(ratings), 2) if ratings else None

    # Reviews distribution
    reviews = [r["reviews_count"] for r in cleaned]
    avg_reviews = round(sum(reviews) / len(reviews), 1) if reviews else 0.0

    # Leaders (top apps) by adoption proxy
    ranked = _rank_apps(cleaned)
    leaders = []
    for r in ranked[:15]:
        leaders.append(
            {
                "app_name": r.get("app_name"),
                "segment": r.get("segment"),
                "pricing": r.get("pricing_bucket"),
                "rating": r.get("rating"),
                "reviews_count": r.get("reviews_count"),
                "adoption_score": r.get("_adoption_score"),
                "app_url": r.get("app_url"),
                "listing_id": r.get("listing_id"),
            }
        )

    # Concentration by segment (HHI on segment shares)
    concentration = {
        "segment_hhi": round(seg_hhi, 1),
        "segment_hhi_label": _hhi_label(seg_hhi),
        "segment_cr4": round(sum([r["share"] for r in seg_rows[:4]]), 4) if seg_rows else 0.0,
    }

    # “Gaps” heuristic:
    # - segment has low app count BUT high average reviews (proxy for demand)
    seg_reviews = defaultdict(list)
    for r in cleaned:
        seg_reviews[r["segment"]].append(_to_int(r.get("reviews_count"), 0))

    gap_candidates = []
    for seg, c in seg_counts.items():
        rv = seg_reviews.get(seg, [])
        avg_rv = sum(rv) / len(rv) if rv else 0.0
        gap_candidates.append((seg, c, avg_rv))

    # prioritize segments with (low supply) and (high demand proxy)
    gap_candidates.sort(key=lambda x: (x[1], -x[2]))  # fewer apps first, then higher avg reviews
    gaps = []
    for seg, c, avg_rv in gap_candidates[:10]:
        gaps.append({"segment": seg, "apps": c, "avg_reviews": round(avg_rv, 1)})

    # Actionable implications (data-driven bullets)
    top_seg = seg_rows[0]["segment"] if seg_rows else "unknown"
    top_seg_share = seg_rows[0]["share"] if seg_rows else 0.0
    top_pricing = pricing_rows[0]["pricing"] if pricing_rows else "unknown"
    top_pricing_share = pricing_rows[0]["share"] if pricing_rows else 0.0

    insights = [
        {
            "headline": "Market structure",
            "evidence": f"Top segment: {top_seg} ({_pct(top_seg_share)} of apps). Segment concentration: {concentration['segment_hhi_label']} (HHI={concentration['segment_hhi']}).",
        },
        {
            "headline": "Pricing landscape",
            "evidence": f"Most common pricing bucket: {top_pricing} ({_pct(top_pricing_share)} of apps).",
        },
    ]

    if avg_rating is not None:
        insights.append(
            {
                "headline": "Quality signal",
                "evidence": f"Average rating across rated apps: {avg_rating}. Average reviews per app: {avg_reviews}.",
            }
        )

    mi = {
        "generated_at": now,
        "overview": {
            "total_apps": total_apps,
            "avg_rating": avg_rating,
            "avg_reviews": avg_reviews,
        },
        "segment_structure": {
            "segments": seg_rows[:30],
            "concentration": concentration,
        },
        "pricing_landscape": {
            "pricing_distribution": pricing_rows,
        },
        "leaders": leaders,
        "gaps": gaps,
        "insights": insights,
        "limitations": [
            "Adoption score uses reviews count + rating as a proxy (no review velocity/time-series).",
            "Vendor-level concentration is not included if vendor is removed from scraping output.",
        ],
    }
    return mi


def build_exec_summary(mi: Dict[str, Any]) -> str:
    """
    McKinsey-style: insight-first + MECE + evidence bullets.
    """
    ov = mi.get("overview", {})
    total = ov.get("total_apps", 0)
    avg_rating = ov.get("avg_rating", None)
    avg_reviews = ov.get("avg_reviews", None)

    seg = (mi.get("segment_structure", {}) or {}).get("segments", []) or []
    pricing = (mi.get("pricing_landscape", {}) or {}).get("pricing_distribution", []) or []
    leaders = mi.get("leaders", []) or []
    gaps = mi.get("gaps", []) or []
    conc = (mi.get("segment_structure", {}) or {}).get("concentration", {}) or {}

    top_seg = seg[0] if seg else {}
    top_pr = pricing[0] if pricing else {}

    lines: List[str] = []

    # Executive Insight (Pyramid top)
    lines.append("EXECUTIVE INSIGHT")
    lines.append(
        f"- The analyzed AppExchange slice contains **{total} apps**."
    )
    if top_seg:
        lines.append(
            f"- Market is led by **{top_seg.get('segment')}** with **{top_seg.get('apps')} apps** ({_pct(top_seg.get('share', 0.0))})."
        )
    if conc:
        lines.append(
            f"- Segment concentration: **{conc.get('segment_hhi_label')}** (HHI={conc.get('segment_hhi')}, CR4={_pct(conc.get('segment_cr4', 0.0))})."
        )
    if top_pr:
        lines.append(
            f"- Pricing mix is dominated by **{top_pr.get('pricing')}** ({_pct(top_pr.get('share', 0.0))})."
        )
    if avg_rating is not None:
        lines.append(f"- Average rating (rated apps): **{avg_rating}**; average reviews per app: **{avg_reviews}**.")

    lines.append("")
    lines.append("1) MARKET OVERVIEW")
    lines.append(f"- Total apps: {total}")
    if avg_rating is not None:
        lines.append(f"- Avg rating: {avg_rating}")
    if avg_reviews is not None:
        lines.append(f"- Avg reviews: {avg_reviews}")

    lines.append("")
    lines.append("2) SEGMENT STRUCTURE (MECE)")
    for r in seg[:8]:
        lines.append(f"- {r['segment']}: {r['apps']} apps ({_pct(r['share'])})")

    lines.append("")
    lines.append("3) PRICING LANDSCAPE")
    for r in pricing[:6]:
        lines.append(f"- {r['pricing']}: {r['apps']} apps ({_pct(r['share'])})")

    lines.append("")
    lines.append("4) LEADING APPS (ADOPTION PROXY)")
    for r in leaders[:8]:
        lines.append(
            f"- {r.get('app_name')} | rating={r.get('rating')} | reviews={r.get('reviews_count')} | pricing={r.get('pricing')} | segment={r.get('segment')}"
        )

    lines.append("")
    lines.append("5) OPPORTUNITY / GAPS (LOW SUPPLY + HIGH DEMAND SIGNAL)")
    for g in gaps[:6]:
        lines.append(f"- {g['segment']}: {g['apps']} apps, avg reviews={g['avg_reviews']}")

    lines.append("")
    lines.append("RECOMMENDED NEXT STEPS")
    lines.append("- Run the same analysis across additional app groups (or across industries/products) to compare structure and pricing mix.")
    lines.append("- Export leaders list and gaps list into a tracking spreadsheet for shortlist / competitor monitoring.")
    lines.append("- If you enable reviews in future, add review-velocity (time-based) signals for true growth detection.")

    return "\n".join(lines).strip() + "\n"


def build_market_pdf(mi: Dict[str, Any], summary: str) -> bytes:
    """
    Builds a PDF report with tables and evidence.
    """
    buf = _BytesBuffer()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=2.0 * cm,
        rightMargin=2.0 * cm,
        topMargin=1.8 * cm,
        bottomMargin=1.8 * cm,
        title="Market Report",
        author="AppExchange Intelligence Engine",
    )

    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("h1", parent=styles["Heading1"], fontSize=16, spaceAfter=8)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=12, spaceAfter=6)
    body = ParagraphStyle("body", parent=styles["BodyText"], fontSize=10, leading=13)

    story: List[Any] = []

    # Title
    story.append(Paragraph("Salesforce AppExchange — Market Intelligence Report", h1))
    story.append(Paragraph(f"Generated at: {mi.get('generated_at', '')}", body))
    story.append(Spacer(1, 10))

    # Executive Summary
    story.append(Paragraph("Executive Summary", h2))
    for line in summary.splitlines():
        if not line.strip():
            story.append(Spacer(1, 6))
            continue
        story.append(Paragraph(line.replace("**", ""), body))
    story.append(PageBreak())

    # Market Overview
    ov = mi.get("overview", {})
    story.append(Paragraph("1. Market Overview", h2))
    ov_table = [
        ["Total apps", str(ov.get("total_apps", ""))],
        ["Average rating (rated apps)", str(ov.get("avg_rating", ""))],
        ["Average reviews per app", str(ov.get("avg_reviews", ""))],
    ]
    story.append(_table(ov_table, col_widths=[6.0 * cm, 8.0 * cm]))
    story.append(Spacer(1, 10))

    # Segment Structure
    seg = (mi.get("segment_structure", {}) or {}).get("segments", []) or []
    conc = (mi.get("segment_structure", {}) or {}).get("concentration", {}) or {}

    story.append(Paragraph("2. Segment Structure", h2))
    story.append(Paragraph(
        f"Concentration: {conc.get('segment_hhi_label')} (HHI={conc.get('segment_hhi')}, CR4={_pct(conc.get('segment_cr4', 0.0))})",
        body
    ))
    story.append(Spacer(1, 6))

    seg_table = [["Segment", "Apps", "Share"]]
    for r in seg[:15]:
        seg_table.append([r["segment"], str(r["apps"]), _pct(r["share"])])
    story.append(_table(seg_table, header=True, col_widths=[9.0 * cm, 2.5 * cm, 2.5 * cm]))
    story.append(Spacer(1, 10))

    # Pricing Landscape
    pricing = (mi.get("pricing_landscape", {}) or {}).get("pricing_distribution", []) or []
    story.append(Paragraph("3. Pricing Landscape", h2))
    pr_table = [["Pricing", "Apps", "Share"]]
    for r in pricing[:10]:
        pr_table.append([r["pricing"], str(r["apps"]), _pct(r["share"])])
    story.append(_table(pr_table, header=True, col_widths=[6.0 * cm, 2.5 * cm, 2.5 * cm]))
    story.append(Spacer(1, 10))

    # Leaders
    leaders = mi.get("leaders", []) or []
    story.append(Paragraph("4. Leading Apps (Adoption proxy: reviews + rating)", h2))
    ld_table = [["App", "Segment", "Pricing", "Rating", "Reviews"]]
    for r in leaders[:15]:
        ld_table.append([
            _norm(r.get("app_name")),
            _norm(r.get("segment")),
            _norm(r.get("pricing")),
            str(r.get("rating")),
            str(r.get("reviews_count")),
        ])
    story.append(_table(ld_table, header=True, col_widths=[5.2 * cm, 4.3 * cm, 2.2 * cm, 1.5 * cm, 1.5 * cm]))
    story.append(Spacer(1, 10))

    # Gaps
    gaps = mi.get("gaps", []) or []
    story.append(Paragraph("5. Opportunity / Gaps", h2))
    story.append(Paragraph("Segments with low supply (few apps) but relatively high demand signal (avg reviews).", body))
    gap_table = [["Segment", "Apps", "Avg reviews"]]
    for g in gaps[:12]:
        gap_table.append([g["segment"], str(g["apps"]), str(g["avg_reviews"])])
    story.append(_table(gap_table, header=True, col_widths=[10.0 * cm, 2.5 * cm, 2.5 * cm]))
    story.append(Spacer(1, 10))

    # Insights
    insights = mi.get("insights", []) or []
    story.append(Paragraph("6. Key Insights (Evidence-based)", h2))
    for ins in insights[:8]:
        story.append(Paragraph(f"- {ins.get('headline')}: {ins.get('evidence')}", body))
    story.append(Spacer(1, 10))

    # Appendix note
    story.append(Paragraph("Appendix", h2))
    story.append(Paragraph(
        "For the full app list, download APPS.csv / APPS.xlsx from the Key-Value store or Dataset export.",
        body
    ))

    doc.build(story)
    return buf.getvalue()


# ---------------------------------------------------------------------
# Small PDF utilities
# ---------------------------------------------------------------------
class _BytesBuffer:
    def __init__(self):
        self._chunks: List[bytes] = []

    def write(self, b: bytes) -> int:
        self._chunks.append(b)
        return len(b)

    def getvalue(self) -> bytes:
        return b"".join(self._chunks)

def _table(data: List[List[str]], header: bool = False, col_widths: Optional[List[float]] = None) -> Table:
    t = Table(data, colWidths=col_widths)
    style = TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LINEBELOW", (0, 0), (-1, -1), 0.25, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.lightgrey]),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ])
    if header and len(data) >= 1:
        style.add("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937"))
        style.add("TEXTCOLOR", (0, 0), (-1, 0), colors.white)
        style.add("LINEBELOW", (0, 0), (-1, 0), 0.5, colors.black)
        style.add("FONTSIZE", (0, 0), (-1, 0), 9)
    t.setStyle(style)
    return t