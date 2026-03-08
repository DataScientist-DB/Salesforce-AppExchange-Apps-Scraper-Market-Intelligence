from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from apify import Actor

from .apps_flow import run_apps_flow


def _first_str(v: Any, default: str) -> str:
    if v is None:
        return default
    if isinstance(v, list):
        v = next((x for x in v if str(x).strip()), default)
        return str(v).strip()
    s = str(v).strip()
    return s if s else default


def _as_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    s = str(v).strip()
    return [s] if s else []


def _to_int(v: Any, default: int) -> int:
    try:
        if v is None or v == "":
            return default
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default


def _to_float(v: Any, default: float) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _to_bool(v: Any, default: bool) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in {"true", "1", "yes", "y"}:
        return True
    if s in {"false", "0", "no", "n"}:
        return False
    return default


def normalize_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(cfg or {})

    out["mode"] = _first_str(out.get("mode"), "apps").lower().strip()

    out["categoryGroup"] = _first_str(
        out.get("categoryGroup"),
        "business-needs",
    ).lower().replace("_", "-")

    out["sphere"] = _first_str(
        out.get("sphere"),
        out["categoryGroup"],
    ).lower().replace("_", "-")

    out["categoryPreset"] = _as_list(out.get("categoryPreset"))
    out["businessNeeds"] = _as_list(out.get("businessNeeds"))
    out["pricingFilter"] = _as_list(out.get("pricingFilter"))
    out["resolvedAppTypes"] = _as_list(out.get("resolvedAppTypes"))

    out["maxPages"] = _to_int(out.get("maxPages"), 1)
    out["startIndex"] = _to_int(out.get("startIndex"), 1)
    out["endIndex"] = _to_int(out.get("endIndex"), 0)
    out["maxReviewsPerApp"] = _to_int(out.get("maxReviewsPerApp"), 0)
    out["minResultsToKeepStrict"] = _to_int(out.get("minResultsToKeepStrict"), 10)
    out["minRating"] = _to_float(out.get("minRating"), 0.0)
    out["headless"] = _to_bool(out.get("headless"), True)

    out["reviewsMode"] = _first_str(out.get("reviewsMode"), "all").lower().strip()

    proxy_settings = out.get("proxySettings")
    out["proxySettings"] = proxy_settings if isinstance(proxy_settings, dict) else {}

    analysis = out.get("analysisOptions")
    analysis = dict(analysis) if isinstance(analysis, dict) else {}

    analysis.setdefault("enableCompetitiveAnalysis", True)
    analysis.setdefault("enableCustomerAnalysis", True)
    analysis.setdefault("enableMarketTrends", True)
    analysis.setdefault("enableExecutiveSummary", True)
    analysis.setdefault("enableSWOT", True)
    analysis.setdefault("outputJsonReport", True)
    analysis.setdefault("outputPdfReport", True)
    analysis.setdefault("reportKvKeyPrefix", "")

    out["analysisOptions"] = analysis

    return out


async def main() -> None:
    async with Actor:
        raw_input = await Actor.get_input() or {}
        cfg = normalize_config(raw_input)

        Actor.log.info(">>> Salesforce AppExchange Discovery Engine starting")
        Actor.log.info(f"MODE: {cfg['mode']}")
        Actor.log.info("Normalized config:\n" + json.dumps(cfg, indent=2))

        project_root = str(Path(__file__).resolve().parents[1])

        if cfg["mode"] != "apps":
            raise ValueError(
                f"Unsupported mode: {cfg['mode']}. "
                "This production build currently supports only 'apps' mode."
            )

        await run_apps_flow(cfg, project_root)

        Actor.log.info("Salesforce AppExchange Discovery Engine completed successfully.")