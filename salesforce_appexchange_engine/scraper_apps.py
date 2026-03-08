from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Set
from urllib.parse import parse_qs, urlencode, urlparse

from apify import Actor
from playwright.async_api import Page, Response

APPX_BASE = "https://appexchange.salesforce.com"

# -----------------------------
# Small string helpers
# -----------------------------
def _vendor_is_bad(v: str) -> bool:
    t = _clean(v)
    if not t:
        return True
    low = t.lower()

    bad_phrases = [
        "offer your solution on appexchange",
        "publish on appexchange",
        "list your solution",
    ]
    if any(p in low for p in bad_phrases):
        return True

    if low.startswith("http://") or low.startswith("https://"):
        return True
    if "@" in t and "." in t:
        return True

    if re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", t, re.I):
        return True

    return False


def _clean(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "").strip())


def _norm_ws(s: Any) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip())


def _json_unescape(s: str) -> str:
    try:
        return json.loads(f'"{s}"')
    except Exception:
        return s


def _first_group(pattern: str, text: str) -> Optional[str]:
    m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    return m.group(1) if m else None


def _parse_int(s: str) -> Optional[int]:
    try:
        s2 = _norm_ws(s).replace(",", "")
        m = re.search(r"(\d+)", s2)
        return int(m.group(1)) if m else None
    except Exception:
        return None


def _parse_float(s: str) -> Optional[float]:
    try:
        m = re.search(r"(\d+(?:\.\d+)?)", _norm_ws(s))
        return float(m.group(1)) if m else None
    except Exception:
        return None


def _extract_listing_id(url: str) -> str:
    try:
        q = parse_qs(urlparse(url).query)
        lid = q.get("listingId", [""])[0]
        return lid or url
    except Exception:
        return url


async def _first_text(page: Page, selectors: List[str]) -> str:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                try:
                    tag = (await loc.evaluate("el => el.tagName")).lower()
                    if tag in ("script", "style"):
                        continue
                except Exception:
                    pass

                t = _clean(await loc.inner_text())
                if t:
                    return t
        except Exception:
            pass
    return ""


# -----------------------------
# Cookie + settle + scroll
# -----------------------------
async def _dismiss_cookie_banner(page: Page) -> None:
    for label in ("Accept All Cookies", "Accept all cookies", "Accept", "I Accept", "Agree"):
        try:
            btn = page.get_by_role("button", name=label)
            if await btn.count() > 0:
                await btn.first.click(timeout=1500)
                await page.wait_for_timeout(500)
                return
        except Exception:
            pass


async def _wait_settle(page: Page) -> None:
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=20000)
    except Exception:
        pass
    try:
        await page.wait_for_load_state("networkidle", timeout=20000)
    except Exception:
        pass
    await page.wait_for_timeout(700)


async def _scroll_load(page: Page, steps: int = 8) -> None:
    for _ in range(steps):
        await page.mouse.wheel(0, 2200)
        await page.wait_for_timeout(650)


# -----------------------------
# Products: transient error overlay + filter click
# -----------------------------
async def _click_try_again_if_error(page: Page) -> None:
    try:
        if await page.get_by_text("Hmm, that didn't work.").count() > 0:
            btn = page.get_by_role("button", name=re.compile(r"try again", re.I))
            if await btn.count() > 0:
                await btn.first.click(timeout=2000)
                await page.wait_for_timeout(1200)
    except Exception:
        pass


def _products_label_variants(v: str) -> list[str]:
    s = (v or "").strip()
    if not s:
        return []
    low = s.lower().strip()

    mapping = {
        "b2b-commerce": "B2B Commerce",
        "b2c-commerce": "B2C Commerce",
        "data cloud": "Data Cloud",
        "data-cloud": "Data Cloud",
    }

    out = []
    if low in mapping:
        out.append(mapping[low])

    out.append(s)
    if "-" in s:
        out.append(" ".join([w[:1].upper() + w[1:] for w in s.split("-") if w]))
    return list(dict.fromkeys([x for x in out if x.strip()]))


async def _apply_products_filter_by_click(page: Page, label: str) -> bool:
    target = (label or "").strip()
    if not target:
        return False

    for t in _products_label_variants(target):
        candidates = [
            page.get_by_role("radio", name=re.compile(rf"^{re.escape(t)}$", re.I)),
            page.get_by_role("checkbox", name=re.compile(rf"^{re.escape(t)}$", re.I)),
            page.get_by_text(re.compile(rf"^{re.escape(t)}$", re.I)),
        ]
        for loc in candidates:
            try:
                if await loc.count() > 0:
                    await loc.first.click(timeout=5000)
                    await page.wait_for_timeout(1200)
                    return True
            except Exception:
                continue

    return False


# -----------------------------
# Listing ID detection
# -----------------------------
_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
_SF_ID = r"a0N[A-Za-z0-9]{12,15}"
_RE_ANY_LISTING_ID = re.compile(rf"({_UUID}|{_SF_ID})", re.IGNORECASE)


def _listing_url(listing_id: str) -> str:
    return f"{APPX_BASE}/appxListingDetail?listingId={listing_id}"


def _extract_listing_ids_from_text(text: str) -> Set[str]:
    if not text:
        return set()

    ids: Set[str] = set()

    for m in re.finditer(rf'listingId"\s*:\s*"({_UUID}|{_SF_ID})"', text, flags=re.IGNORECASE):
        ids.add(m.group(1))

    for m in re.finditer(rf'listing[_]?id"\s*:\s*"({_UUID}|{_SF_ID})"', text, flags=re.IGNORECASE):
        ids.add(m.group(1))

    for m in re.finditer(rf"/appxListingDetail\?listingId=({_UUID}|{_SF_ID})", text, flags=re.IGNORECASE):
        ids.add(m.group(1))

    ids.discard("00000000-0000-0000-0000-000000000000")
    return ids


async def _try_response_text(resp: Response) -> str:
    try:
        ct = (resp.headers.get("content-type") or "").lower()
        if "json" in ct or "text" in ct or "javascript" in ct:
            txt = await resp.text()
            if len(txt) > 2_000_000:
                return txt[:2_000_000]
            return txt
    except Exception:
        pass
    return ""


# -----------------------------
# Explore URL builder
# -----------------------------
def build_explore_url(category_group: str, value: str) -> str:
    g = (category_group or "business-needs").strip().lower().replace("_", "-")
    v = (value or "").strip()

    if g == "products":
        return f"{APPX_BASE}/explore/products"

    if g == "industries":
        return f"{APPX_BASE}/explore/industries?{urlencode({'industry': v})}"

    return f"{APPX_BASE}/explore/business-needs?{urlencode({'category': v})}"


# -----------------------------
# DISCOVERY
# -----------------------------
async def discover_app_urls(
    page: Page,
    *,
    category_group: str,
    sphere_or_category: str,
    max_pages: int = 3,
) -> List[str]:
    g = (category_group or "business-needs").strip().lower().replace("_", "-")
    v = (sphere_or_category or "").strip()

    explore_url = build_explore_url(g, v)

    Actor.log.info("[discover_app_urls] group=%s value=%s explore_url=%s", g, v, explore_url)

    listing_ids: Set[str] = set()

    async def on_response(resp: Response) -> None:
        try:
            if "appexchange.salesforce.com" not in resp.url:
                return

            txt = await _try_response_text(resp)
            if not txt:
                return

            ids = _extract_listing_ids_from_text(txt)
            if ids:
                listing_ids.update(ids)
        except Exception:
            return

    page.on("response", on_response)

    await page.goto(explore_url, wait_until="domcontentloaded")
    await _dismiss_cookie_banner(page)
    await _wait_settle(page)
    await _click_try_again_if_error(page)

    if g == "products":
        clicked = await _apply_products_filter_by_click(page, v)
        Actor.log.info("[discover_app_urls] products filter click label=%r clicked=%s", v, clicked)
        await _wait_settle(page)
        await _click_try_again_if_error(page)

    cycles = max(1, int(max_pages))
    prev = 0

    for i in range(cycles):
        await _scroll_load(page, steps=8)
        await _wait_settle(page)
        await _click_try_again_if_error(page)

        try:
            hrefs = await page.eval_on_selector_all(
                "a[href]",
                "els => els.map(e => e.getAttribute('href')).filter(Boolean)",
            )
            listing_ids.update(_extract_listing_ids_from_text("\n".join([str(h) for h in hrefs or []])))
        except Exception:
            pass

        try:
            html = await page.content()
            listing_ids.update(_extract_listing_ids_from_text(html))
        except Exception:
            pass

        cur = len(listing_ids)
        Actor.log.info("[discover_app_urls] cycle=%s new=%s total=%s", i + 1, cur - prev, cur)
        prev = cur

        if cur >= 40:
            break

    if not listing_ids:
        key = f"{g}_{v}".lower().replace(" ", "_")
        Actor.log.warning("[discover_app_urls] ZERO urls discovered; saving debug artifacts key=%s", key)

        try:
            png = await page.screenshot(full_page=True)
            await Actor.set_value(f"{key}.png", png, content_type="image/png")
        except Exception:
            pass
        try:
            await Actor.set_value(f"{key}.html", await page.content(), content_type="text/html")
        except Exception:
            pass

        return []

    urls = sorted({_listing_url(i) for i in listing_ids})
    Actor.log.info("[discover_app_urls] ✅ discovered listingIds=%s urls=%s", len(listing_ids), len(urls))
    return urls


# -----------------------------
# Detail extractor
# -----------------------------
async def extract_app_detail(page: Page, detail_url: str) -> Dict[str, Any]:
    await page.goto(detail_url, wait_until="domcontentloaded", timeout=90000)

    try:
        await page.wait_for_selector("h1", timeout=12000)
    except Exception:
        pass
    await page.wait_for_timeout(500)

    title = ""
    try:
        title = (await page.title()) or ""
    except Exception:
        title = ""

    cur_url = ""
    try:
        cur_url = page.url or ""
    except Exception:
        cur_url = ""

    if "error?code=PAGE_NOT_FOUND" in cur_url or "page not found" in title.lower():
        listing_id = _extract_listing_id(detail_url)
        Actor.log.warning("[DETAIL] PAGE_NOT_FOUND url=%s", detail_url)
        return {
            "listing_id": listing_id,
            "app_name": "",
            "rating": None,
            "reviews_count": None,
            "price_text": "",
            "pricing_model": "unknown",
            "short_description": "",
            "description": "",
            "app_url": detail_url,
            "status": "not_found",
        }

    html = await page.content()

    app_name = ""
    try:
        for sel in ["h1", "header h1"]:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                t = _norm_ws(await loc.inner_text())
                if t and len(t) <= 250:
                    app_name = t
                    break
    except Exception:
        pass

    try:
        jsonld_texts = await page.eval_on_selector_all(
            "script[type='application/ld+json']",
            "els => els.map(e => e.textContent || '').filter(Boolean)",
        )
    except Exception:
        jsonld_texts = []

    jsonld_blob = "\n".join([t for t in (jsonld_texts or []) if t and len(t) < 300000])

    rating = None
    reviews_count = None

    try:
        body_text = _norm_ws(await page.inner_text("body"))
    except Exception:
        body_text = _norm_ws(html)

    m = re.search(r"\b(\d\.\d)\s*\(([\d,]+)\s*(?:ratings|reviews)\)", body_text, re.IGNORECASE)
    if m:
        rating = _parse_float(m.group(1))
        reviews_count = _parse_int(m.group(2))

    if rating is None:
        v2 = _first_group(r'"averageRating"\s*:\s*([0-9.]+)', html)
        if v2:
            rating = _parse_float(v2)

    if reviews_count is None:
        v3 = _first_group(r'"reviewCount"\s*:\s*(\d+)', html) or _first_group(r'"reviewsCount"\s*:\s*(\d+)', html)
        if v3:
            reviews_count = _parse_int(v3)

    short_description = ""
    meta_desc = _first_group(r'<meta\s+name="description"\s+content="([^"]+)"', html)
    if meta_desc:
        short_description = _json_unescape(meta_desc)

    if not short_description:
        v4 = _first_group(r'"shortDescription"\s*:\s*"([^"]+)"', html) or _first_group(
            r'"tagline"\s*:\s*"([^"]+)"', html
        )
        if v4:
            short_description = _json_unescape(v4)

    price_text = await _first_text(
        page,
        [
            "css=[data-test='pricing']",
            "css=[data-test='pricing-text']",
            "xpath=//*[contains(., 'Pricing')]/following::*[self::p or self::div or self::span][1]",
            "css=span:has-text('Free')",
            "css=span:has-text('Trial')",
        ],
    )

    if not price_text:
        price_text = _json_unescape(_first_group(r'"pricingText"\s*:\s*"([^"]+)"', html) or "") or ""

    if not price_text and jsonld_blob:
        p = _json_unescape(_first_group(r'"price"\s*:\s*"([^"]+)"', jsonld_blob) or "") or ""
        c = _json_unescape(_first_group(r'"priceCurrency"\s*:\s*"([^"]+)"', jsonld_blob) or "") or ""
        if p:
            price_text = f"{p} {c}".strip()

    price_text = _clean(price_text)

    pricing_model = "unknown"
    pt = (price_text or "").lower()
    bt = (body_text or "").lower()

    if any(x in pt or x in bt for x in ["nonprofit", "non-profit", "ngo"]):
        pricing_model = "nonprofit-discount"
    elif any(x in pt or x in bt for x in ["add-on required", "addon required", "paid add-on"]):
        pricing_model = "paid-addon-required"
    elif "freemium" in pt or "free trial" in pt or "trial" in pt or re.search(r"\bfree\s+trial\b", bt, re.IGNORECASE):
        pricing_model = "freemium"
    elif re.search(r"\bfree\b", pt, re.IGNORECASE) or re.search(r"\bfree\b", bt, re.IGNORECASE):
        pricing_model = "free"
    elif any(x in pt for x in ["paid", "subscription", "per user", "per month", "usd", "eur", "gbp", "$", "€", "£"]) or re.search(
        r"(\$\s?\d+|\b\d+\s?(usd|eur|gbp)\b)",
        bt,
        re.IGNORECASE,
    ):
        pricing_model = "paid"

    pricing_model = (pricing_model or "unknown").strip().lower()

    listing_id = _extract_listing_id(detail_url)

    if not price_text:
        Actor.log.warning("[DETAIL DEBUG] price missing url=%s", detail_url)

    Actor.log.info(
        "[DETAIL] pricing_model=%s | price_text=%r | url=%s",
        pricing_model,
        price_text,
        detail_url,
    )

    return {
        "listing_id": listing_id,
        "app_name": app_name or "",
        "rating": float(rating) if rating is not None else None,
        "reviews_count": int(reviews_count) if reviews_count is not None else None,
        "price_text": price_text,
        "pricing_model": pricing_model,
        "short_description": short_description,
        "description": "",
        "app_url": detail_url,
        "status": "ok",
    }