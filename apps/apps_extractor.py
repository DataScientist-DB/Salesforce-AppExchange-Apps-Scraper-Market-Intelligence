# apps/apps_extractor.py
import re
from dataclasses import dataclass, asdict
from typing import List, Dict, Set, Optional, Any, Tuple

from playwright.async_api import Page, ElementHandle

from logger import get_logger
from utils.errors import ExtractionError

from urllib.parse import urlsplit, urlunsplit, urlparse, parse_qs

async def _detect_free_trial(page: Page) -> bool:
    """
    Detect whether the AppExchange listing offers a free trial.
    Best-effort, safe, never crashes the run.
    """
    try:
        # 1. Fast HTML scan
        html = await page.content()
        if re.search(r"\bfree\s*trial\b", html, flags=re.IGNORECASE):
            return True

        # 2. Visible text scan
        body_text = await page.inner_text("body")
        if re.search(r"\bfree\s*trial\b", body_text, flags=re.IGNORECASE):
            return True

        # 3. CTA / chip / button fallbacks
        selectors = [
            "text=/free\\s*trial/i",
            "button:has-text('Free trial')",
            "a:has-text('Free trial')",
            "[aria-label*='free trial' i]",
        ]

        for sel in selectors:
            loc = page.locator(sel)
            if await loc.count() > 0 and await loc.first.is_visible():
                return True

        return False

    except Exception:
        # IMPORTANT: never fail extraction because of this field
        return False

async def collect_all_app_urls_with_pagination(
    page: Page,
    start_url: str,
    seen_urls: Set[str],
    max_pages: int = 20,
) -> List[str]:
    """
    Visit the listing page (start_url) and follow pagination / load-more
    to collect app URLs from multiple pages.

    Returns:
        List of unique app detail URLs.
    """
    all_urls: List[str] = []
    current_page = 1

    await page.goto(start_url, wait_until="networkidle", timeout=60000)

    while True:
        log.info("[APPS] Listing page %d: %s", current_page, page.url)

        # Extract URLs from this page
        new_urls = await extract_app_urls_from_listing(page, page.url, seen_urls)
        log.info("[APPS] Page %d yielded %d new URLs", current_page, len(new_urls))
        all_urls.extend(new_urls)

        if current_page >= max_pages:
            log.info("[APPS] Reached max_pages=%d, stopping pagination.", max_pages)
            break

        # ---- Try pagination: "Next" button or equivalent ----
        next_locator = page.locator("button:has-text('Next'), a[aria-label='Next']")
        try:
            if await next_locator.count() == 0:
                log.info("[APPS] No Next button found, stopping.")
                break

            await next_locator.first.click()
            await page.wait_for_timeout(1500)  # wait for cards to load
            current_page += 1

        except Exception as e:
            log.warning("[APPS] Failed to click Next on page %d: %s", current_page, e)
            break

    log.info("[APPS] Total unique app URLs collected: %d", len(all_urls))
    return all_urls

def _canonicalize_appex_url(url: str) -> str:
    """
    Normalize AppExchange URLs so that tracking query params / fragments
    don't create duplicates.
    """
    parsed = urlsplit(url)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))


def _extract_listing_id(url: str) -> str:
    """
    Extract the listingId from a URL like:
      https://appexchange.salesforce.com/appxListingDetail?listingId=a0N3...
    Returns empty string if not found.
    """
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        ids = qs.get("listingId") or qs.get("listingid") or []
        return ids[0] if ids else ""
    except Exception:
        return ""


log = get_logger(__name__)

# ------------------------
# Listing-page card selectors (kept for future use)
# ------------------------
card_selectors = [
    "div[class*='card']",           # discovered working selector
    "div.slds-card",                # SLDS variant
    "div[class*='tile']",           # fallback
    "section div[class*='card']",   # nested fallback
]

# For detail page (app profile)
DETAIL_NAME_SELECTORS = [
    "h1[type-style='display-6']",
    "h1[data-region-name='appx-app-name']",
    "h1",
]

DETAIL_VENDOR_SELECTORS = [
    "div[type-style='display-7']",          # vendor element
    "a[data-region-name*='publisher']",
    "a[title*='View publisher']",
    "a[href*='/partners/']",
    "a[href*='/partner/']",
]

DETAIL_RATING_SELECTORS = [
    "span[aria-label*='out of 5']",
    "span[class*='rating']",
    "span[aria-label*='Rated']",
]

DETAIL_REVIEWS_SELECTORS = [
    "span[aria-label*='review']",
    "button[aria-label*='review']",
    "a[href*='#reviews'] span",
]

CATEGORY_CHIP_SELECTOR = "wds-tag.category"


# ------------------------
# Data model (lean but with rating)
# ------------------------
@dataclass
class AppRecord:
    listing_id: str
    name: str
    vendor: str
    rating: Optional[float]          # 0–5; None if we cannot parse safely
    reviews_count: Optional[int]     # total reviews; None if unknown
    primary_category: str            # first category chip, if any
    categories_raw: str              # all categories, "; "-separated
    short_description: str           # short marketing tagline / summary
    pricing_text: str                # raw pricing string (e.g. "Free", "Contact us")
    has_free_trial: Optional[bool]   # True/False if detectable, else None
    clouds: str                      # e.g. "Sales Cloud; Service Cloud"
    url: str                         # canonical detail URL (appxListingDetail)


# ------------------------
# Generic helpers
# ------------------------
async def _first_aria_label(root: Any, must_contain: List[str]) -> str:
    """
    Scan all elements with aria-label and return the first label
    that contains ANY of the given substrings (case-insensitive).
    """
    try:
        elements = await root.query_selector_all("[aria-label]")
    except Exception:
        elements = []

    must_contain_lower = [m.lower() for m in must_contain]

    for el in elements:
        label = (await el.get_attribute("aria-label")) or ""
        label_stripped = label.strip()
        if not label_stripped:
            continue
        lower = label_stripped.lower()
        if any(token in lower for token in must_contain_lower):
            return label_stripped

    return ""
async def _first_text_for_selector(root: Any, selector: str) -> str:
    """
    Convenience helper: get text_content() for the first matching element.
    """
    try:
        el = await root.query_selector(selector)
    except Exception:
        el = None
    if not el:
        return ""
    txt = (await el.text_content()) or ""
    return txt.strip()

async def _first_text(root: Any, selectors: List[str]) -> str:
    """
    Works for both Page and ElementHandle, anything with query_selector().
    """
    for sel in selectors:
        try:
            el = await root.query_selector(sel)
        except Exception:
            el = None
        if el:
            txt = (await el.text_content()) or ""
            txt = txt.strip()
            if txt:
                return txt
    return ""


async def _first_href(card: ElementHandle, selectors: List[str]) -> Optional[str]:
    for sel in selectors:
        try:
            el = await card.query_selector(sel)
        except Exception:
            el = None
        if el:
            href = await el.get_attribute("href")
            if href:
                return href
    return None

async def _extract_clouds(page: Page) -> str:
    """
    Extract Salesforce Cloud tags from the page.

    Strategy:
      - Look for short texts containing the word 'Cloud'
        on typical tag elements.
    """
    cloud_labels: List[str] = []

    selectors = [
        "wds-tag",
        "wds-tag[slot*='cloud']",
        "[data-region-name*='cloud']",
        "a",
        "span",
    ]

    seen = set()

    for sel in selectors:
        try:
            els = await page.query_selector_all(sel)
        except Exception:
            els = []
        for el in els:
            txt = (await el.text_content()) or ""
            txt = txt.strip()
            if not txt:
                continue

            lower = txt.lower()
            # heuristic: short-ish text that mentions 'cloud'
            if "cloud" in lower and len(txt) <= 40:
                if txt not in seen:
                    seen.add(txt)
                    cloud_labels.append(txt)

    return "; ".join(cloud_labels)


def _parse_rating_and_reviews(
    rating_text: str,
    reviews_raw: str,
) -> Tuple[Optional[float], Optional[int]]:
    """
    Conservative parsing of rating (0–5) and review count.

    If we don't see a clear pattern like "4.9 out of 5" or "4.9 (22 reviews)",
    we prefer to return None rather than a wrong value.
    """
    rating: Optional[float] = None
    reviews_count: Optional[int] = None

    # ---- Rating: prefer "X out of 5" or "Rated X out of 5" ----
    if rating_text:
        m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*(?:out of|/)\s*5", rating_text)
        if m:
            try:
                rating = float(m.group(1))
            except ValueError:
                rating = None

    # Fallback: any standalone number 0–5 in rating_text
    if rating is None and rating_text:
        m = re.search(r"\b([0-5](?:\.[0-9]+)?)\b", rating_text)
        if m:
            try:
                rating = float(m.group(1))
            except ValueError:
                rating = None

    # ---- Reviews count: look first at "reviews" snippet ----
    def _parse_count(src: str) -> Optional[int]:
        if not src:
            return None
        # Match numbers like "22", "1,234"
        m_local = re.search(r"([0-9][0-9,]*)", src.replace("\u00a0", " "))
        if not m_local:
            return None
        try:
            return int(m_local.group(1).replace(",", ""))
        except ValueError:
            return None

    reviews_count = _parse_count(reviews_raw)

    # Sometimes in rating_text as "4.9 (22)"
    if reviews_count is None and rating_text:
        m_paren = re.search(r"\((\d[\d,]*)\)", rating_text)
        if m_paren:
            try:
                reviews_count = int(m_paren.group(1).replace(",", ""))
            except ValueError:
                reviews_count = None

    # Last fallback: "22 reviews" / "22 ratings"
    if reviews_count is None and rating_text:
        m_near = re.search(r"([0-9][0-9,]*)\s+(?:reviews?|ratings?)", rating_text, re.I)
        if m_near:
            try:
                reviews_count = int(m_near.group(1).replace(",", ""))
            except ValueError:
                reviews_count = None

    return rating, reviews_count


async def _extract_categories_from_page(page: Page) -> str:
    """
    Best-effort extraction of category tags.

    Returns:
        "; "-separated string like "Analytics; Sales; Platform".
    """
    categories: List[str] = []

    # 1) Primary selector (what we used before)
    try:
        chips = await page.query_selector_all(CATEGORY_CHIP_SELECTOR)
    except Exception:
        chips = []

    # 2) Fallback selectors if nothing found
    if not chips:
        fallback_selectors = [
            "wds-tag[data-region-name*='category']",
            "wds-tag[slot*='category']",
            "wds-tag",  # generic tags
            "a[data-region-name*='category']",
            "span[data-region-name*='category']",
        ]
        for sel in fallback_selectors:
            try:
                chips = await page.query_selector_all(sel)
            except Exception:
                chips = []
            if chips:
                break

    for chip in chips:
        # Prefer aria-label, fallback to text
        label = (await chip.get_attribute("aria-label")) or ""
        if not label.strip():
            label = (await chip.text_content()) or ""
        label = label.strip()
        if not label:
            continue

        # Clean common wrappers like "View Finance solutions"
        l_lower = label.lower()
        if l_lower.startswith("view "):
            label = label[5:]
        if l_lower.endswith(" solutions"):
            label = label[:-10]

        label = label.strip()
        if label and label not in categories:
            categories.append(label)

    return "; ".join(categories)

# ------------------------
# 1) Listing page: get URLs only
# ------------------------
from typing import List, Dict, Any, Set
from urllib.parse import urljoin
from playwright.async_api import Page
from logger import get_logger
from utils.errors import ExtractionError

log = get_logger(__name__)

APP_DETAIL_HREF_FRAGMENT = "appxListingDetail?listingId="


async def extract_app_urls_from_listing(
    page: Page,
    listing_url: str,
    seen_urls: Set[str],
) -> List[str]:
    """
    Robust extractor for app detail URLs from any AppExchange listing/grid/search page.

    Strategy:
      - Find all <a> elements whose href contains "appxListingDetail?listingId="
      - Normalize to absolute URLs with urljoin
      - Deduplicate using 'seen_urls'
      - If nothing found => save debug HTML and raise ExtractionError
    """
    anchors = await page.query_selector_all(f"a[href*='{APP_DETAIL_HREF_FRAGMENT}']")
    total_anchors = len(anchors)

    new_urls: List[str] = []

    for a in anchors:
        href = await a.get_attribute("href")
        if not href:
            continue

        full_url = urljoin(listing_url, href)

        # Optional: ignore non-HTTPS or any weird URLs
        if not full_url.startswith("http"):
            continue

        if full_url in seen_urls:
            continue

        seen_urls.add(full_url)
        new_urls.append(full_url)

    log.info(
        "[APPS] Anchor scan on %s: found %d candidate anchors, new URLs=%d",
        listing_url,
        total_anchors,
        len(new_urls),
    )

    if not new_urls and total_anchors == 0:
        # No anchors at all – save HTML for inspection and raise
        debug_name = listing_url.replace("://", "_").replace("/", "_").replace("?", "_")
        debug_path = f"appex_apps_debug_{debug_name}.html"
        try:
            html = await page.content()
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(html)
            log.warning(
                "[APPS] No app URLs found via anchor scan. Saved HTML to %s",
                debug_path,
            )
        except Exception as e:
            log.warning("[APPS] Failed to save debug HTML: %s", e)

        raise ExtractionError(
            f"No app URLs found via anchor scan on {listing_url}"
        )

    return new_urls

# ---- Free-trial + clouds helpers ---------------------------------
from playwright.async_api import Page  # already imported at top in your file


async def _detect_free_trial(page: Page) -> bool:
    """
    Best-effort free-trial detection.

    We simply scan the HTML for common phrases like "Free Trial", "Try for free".
    If we don't see any, we return False (rather than crashing).
    """
    try:
        html = (await page.content()).lower()
    except Exception:
        return False

    phrases = [
        "free trial",
        "try for free",
        "start your free trial",
        "start free trial",
        "get a free trial",
    ]
    return any(p in html for p in phrases)

async def _extract_subtitle(page: Page) -> str:
    candidates = [
        "[data-testid*='subtitle' i]",
        "h1 + div",
        "div[class*='subtitle' i]",
        "div[class*='tagline' i]",
    ]

    for sel in candidates:
        loc = page.locator(sel)
        if await loc.count() > 0:
            text = (await loc.first.inner_text()).strip()
            if 3 < len(text) < 200:
                return text
    return ""

async def _extract_category(page: Page) -> str:
    categories = []

    loc = page.locator("a[href*='/explore/' i]")
    count = await loc.count()

    for i in range(min(count, 8)):
        txt = (await loc.nth(i).inner_text()).strip()
        if 2 < len(txt) < 60 and txt not in categories:
            categories.append(txt)

    return " > ".join(categories[:3]) if categories else ""

async def _extract_rating_count(page: Page) -> int:
    try:
        text = await page.inner_text("body")
        m = re.search(
            r"(\d[\d,]*)\s*(ratings|rating|reviews|review)",
            text,
            flags=re.IGNORECASE,
        )
        if m:
            return int(m.group(1).replace(",", ""))
    except Exception:
        pass
    return 0

async def _extract_clouds(page: Page) -> str:
    """
    Very simple heuristic to detect which Salesforce clouds are mentioned
    on the app detail page.

    This is intentionally conservative; if nothing obvious is found we
    return an empty string instead of failing.
    """
    try:
        html = (await page.content()).lower()
    except Exception:
        return ""

    candidates = {
        "sales cloud": "Sales Cloud",
        "service cloud": "Service Cloud",
        "marketing cloud": "Marketing Cloud",
        "commerce cloud": "Commerce Cloud",
        "experience cloud": "Experience Cloud",
        "analytics cloud": "Analytics Cloud",
        "health cloud": "Health Cloud",
        "financial services cloud": "Financial Services Cloud",
        "nonprofit cloud": "Nonprofit Cloud",
        "manufacturing cloud": "Manufacturing Cloud",
    }

    found: list[str] = []
    for key, label in candidates.items():
        if key in html:
            found.append(label)

    # Deduplicate + join
    if not found:
        return ""
    found = sorted(set(found))
    return "; ".join(found)

# ------------------------
# 2) Detail page: resolve full AppRecord
# ------------------------
# ------------------------
# 2) Detail page: resolve full AppRecord
# ------------------------
# ------------------------
# 2) Detail page: resolve full AppRecord
# ------------------------
# ------------------------
# 2) Detail page: resolve full AppRecord
# ------------------------
# ------------------------
# 2) Detail page: resolve full AppRecord
# ------------------------
async def extract_app_detail(
    page: Page,
    app_url: str,
    listing_url: str,  # kept for possible future use, not exported
) -> Dict[str, Any]:
    """
    Open the app's detail page and extract full data.
    """
    log.info("[APPS] Opening detail: %s", app_url)

    await page.goto(app_url, wait_until="networkidle", timeout=60000)
    final_url = page.url

    # --- Heuristic: skip obvious non-app pages ---
    non_app_fragments = ["/learn/", "/resources/", "/videos/"]
    if any(frag in final_url for frag in non_app_fragments):
        raise ExtractionError(f"Non-app page (learn/resources): {final_url}")

    # -----------------------------
    # Basic identity fields
    # -----------------------------
    name = await _first_text(page, DETAIL_NAME_SELECTORS)
    vendor = await _first_text(page, DETAIL_VENDOR_SELECTORS)

    # -----------------------------
    # Rating (already correct via aria-label)
    # -----------------------------
    rating_label = await _first_aria_label(page, ["out of 5", "rated"])
    rating_text = rating_label or ""

    # -----------------------------
    # Reviews: use "11 Reviews" link first
    # -----------------------------
    reviews_raw = await _first_text_for_selector(
        page, "a[href*='scrollTo=reviews']"
    )
    if not reviews_raw:
        reviews_raw = await _first_aria_label(page, ["review"])

    if "appxListingDetail" not in final_url and "listing" not in final_url:
        raise ExtractionError(f"Page does not look like an app listing: {final_url}")

    rating, reviews_count = _parse_rating_and_reviews(rating_text, reviews_raw)
    listing_id = _extract_listing_id(final_url)

    # -----------------------------
    # Categories & clouds
    # -----------------------------
    categories_raw = await _extract_categories_from_page(page) or ""
    primary_category = ""
    clouds = ""

    if categories_raw:
        parts = [p.strip() for p in categories_raw.split(";") if p.strip()]
        if parts:
            primary_category = parts[0]
        # Clouds from categories first
        cloud_parts = [p for p in parts if "cloud" in p.lower()]
        if cloud_parts:
            clouds = "; ".join(cloud_parts)

    # If still no clouds, run dedicated cloud extractor
    if not clouds:
        clouds = await _extract_clouds(page)

    # -----------------------------
    # Short description
    # -----------------------------
    SHORT_DESCRIPTION_SELECTORS = [
        "p[data-region-name='appx-short-description']",
        "div[data-region-name='appx-short-description']",
        "p[data-region-name='appx_short_description']",
        "div[data-region-name='appx_short_description']",
        "p[type-style='body-2']",
    ]
    short_description = await _first_text(page, SHORT_DESCRIPTION_SELECTORS)

    if not short_description:
        # fallback: first reasonable paragraph
        try:
            short_description = (
                await page.locator("p").first.inner_text()
            ).strip()
        except Exception:
            short_description = ""

    # -----------------------------
    # Pricing text (improved, Case A)
    # -----------------------------
    PRICING_SELECTORS = [
        "span[data-region-name*='pricing']",
        "div[data-region-name*='pricing']",
        "p[data-region-name*='pricing']",
        "span[aria-label*='pricing']",
    ]
    pricing_text = await _first_text(page, PRICING_SELECTORS)

    if not pricing_text:
        # Fallback: generic text-based patterns that often hold pricing info
        FALLBACK_PRICING_SELECTORS = [
            "div:has-text('Starts at')",
            "div:has-text('Starting at')",
            "div:has-text('Free')",
            "div:has-text('Contact us')",
            "span:has-text('$')",
            "div:has-text('$')",
        ]
        pricing_text = await _first_text(page, FALLBACK_PRICING_SELECTORS)

    if not pricing_text:
        # Final fallback: whole pricing section region, if present
        pricing_text = await _first_text_for_selector(
            page, "section[data-region-name*='pricing']"
        )

    if pricing_text:
        pricing_text = " ".join(pricing_text.split())


    # -----------------------------
    # Free trial detection (FINAL, safe)
    # -----------------------------
    try:
        has_free_trial = await _detect_free_trial(page)
    except Exception:
        has_free_trial = None

    # -----------------------------
    # Canonical URL from listing_id
    # -----------------------------
    if listing_id:
        canonical_url = (
            f"https://appexchange.salesforce.com/appxListingDetail?listingId={listing_id}"
        )
    else:
        canonical_url = final_url

    record = AppRecord(
        listing_id=listing_id,
        name=name,
        vendor=vendor,
        rating=rating,
        reviews_count=reviews_count,
        primary_category=primary_category,
        categories_raw=categories_raw,
        short_description=short_description,
        pricing_text=pricing_text,
        has_free_trial=has_free_trial,
        clouds=clouds,
        url=canonical_url,
    )

    return asdict(record)
