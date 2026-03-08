from __future__ import annotations

from io import BytesIO
from typing import Any, Dict, List

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle


def _safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def _kv(mi: Dict[str, Any], path: List[str], default=None):
    cur: Any = mi
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    return cur if cur is not None else default


def render_market_report_pdf(mi: Dict[str, Any], executive_summary: str) -> bytes:
    """
    Creates a 2–3 page Upwork-friendly PDF report.
    """
    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=16 * mm,
        rightMargin=16 * mm,
        topMargin=14 * mm,
        bottomMargin=14 * mm,
        title="Salesforce AppExchange Market Intelligence Report",
    )

    styles = getSampleStyleSheet()
    h1 = styles["Title"]
    h2 = styles["Heading2"]
    body = styles["BodyText"]
    small = ParagraphStyle("small", parent=body, fontSize=9, leading=11, textColor=colors.HexColor("#555555"))

    story: List[Any] = []

    story.append(Paragraph("Salesforce AppExchange Market Intelligence Report", h1))
    story.append(Spacer(1, 8))

    ms = _kv(mi, ["market_summary"], {}) or {}
    story.append(
        Paragraph(
            f"Dataset summary: {ms.get('total_apps', 0)} apps • {ms.get('unique_categories', 0)} categories • "
            f"Avg rating: {ms.get('avg_rating', 'N/A')}",
            small,
        )
    )
    story.append(Spacer(1, 10))

    # Executive Summary
    story.append(Paragraph("Executive Summary", h2))
    for line in executive_summary.split("\n"):
        if line.strip():
            story.append(Paragraph(line, body))
        else:
            story.append(Spacer(1, 6))
    story.append(Spacer(1, 10))

    # Top categories table
    story.append(Paragraph("Top Categories", h2))
    top_categories = _kv(mi, ["top_categories"], []) or []
    cat_rows = [["Category", "Apps", "Share %"]]
    for c in top_categories[:10]:
        cat_rows.append([_safe_str(c.get("category")), _safe_str(c.get("apps")), _safe_str(c.get("share_pct"))])

    t = Table(cat_rows, colWidths=[90 * mm, 30 * mm, 30 * mm])
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
            ]
        )
    )
    story.append(t)
    story.append(Spacer(1, 10))

    # Rating distribution
    story.append(Paragraph("Customer Satisfaction (Rating Distribution)", h2))
    rd = _kv(mi, ["customer_analysis", "rating_distribution"], {}) or {}
    rd_rows = [["Rating band", "Apps"]]
    for k in ["5.0-4.5", "4.5-4.0", "4.0-3.5", "<3.5"]:
        rd_rows.append([k, _safe_str(rd.get(k, 0))])

    t2 = Table(rd_rows, colWidths=[90 * mm, 60 * mm])
    t2.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
            ]
        )
    )
    story.append(t2)
    story.append(Spacer(1, 10))

    # Market Gap Signals
    story.append(Paragraph("Market Gap Signals (Opportunity Categories)", h2))
    gaps = _kv(mi, ["market_gaps", "opportunity_categories"], []) or []

    if gaps:
        gap_rows = [["Category", "Apps", "Avg rating", "Share %", "Reason"]]
        for g in gaps[:10]:
            gap_rows.append(
                [
                    _safe_str(g.get("category")),
                    _safe_str(g.get("apps")),
                    _safe_str(g.get("avg_rating")),
                    _safe_str(g.get("share_pct")),
                    Paragraph(_safe_str(g.get("reason")), small),
                ]
            )

        tg = Table(gap_rows, colWidths=[35 * mm, 16 * mm, 20 * mm, 18 * mm, 85 * mm])
        tg.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
                ]
            )
        )
        story.append(tg)
    else:
        story.append(Paragraph("No opportunity categories detected for this dataset (gap detection is heuristic).", small))

    story.append(Spacer(1, 10))

    # Competitor Ranking (Top Apps)
    story.append(Paragraph("Top Competitors (Highest Rated Apps)", h2))
    top_apps = _kv(mi, ["competitor_ranking", "top_apps_overall"], []) or []

    if top_apps:
        app_rows = [["App", "Category", "Rating", "App URL"]]
        for a in top_apps[:12]:
            app_rows.append(
                [
                    Paragraph(_safe_str(a.get("name")), body),
                    _safe_str(a.get("category")),
                    _safe_str(a.get("rating")),
                    Paragraph(_safe_str(a.get("app_url")), small),  # ✅ wrapped URL
                ]
            )

        ta = Table(app_rows, colWidths=[60 * mm, 25 * mm, 18 * mm, 70 * mm])
        ta.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
                ]
            )
        )
        story.append(ta)
    else:
        story.append(Paragraph("No rated apps available for competitor ranking in this run.", small))

    story.append(Spacer(1, 10))

    # SWOT
    story.append(Paragraph("Strategic Snapshot (SWOT)", h2))
    sw = _kv(mi, ["swot"], {}) or {}

    def bullets(arr):
        return "<br/>".join([f"• {x}" for x in (arr or [])[:5]]) or "• (none)"

    swot_table = [
        ["Strengths", "Weaknesses"],
        [Paragraph(bullets(sw.get("strengths")), body), Paragraph(bullets(sw.get("weaknesses")), body)],
        ["Opportunities", "Threats"],
        [Paragraph(bullets(sw.get("opportunities")), body), Paragraph(bullets(sw.get("threats")), body)],
    ]

    t3 = Table(swot_table, colWidths=[90 * mm, 90 * mm])
    t3.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("BACKGROUND", (0, 2), (-1, 2), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, 2), (-1, 2), "Helvetica-Bold"),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    story.append(t3)

    doc.build(story)
    return buf.getvalue()