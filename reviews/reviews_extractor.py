# reviews/reviews_extractor.py
import re
import hashlib
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Set

from apify import Actor
from playwright.async_api import Page, ElementHandle, Locator
from playwright.async_api import BrowserContext
from logger import get_logger
from utils.errors import ExtractionError

log = get_logger(__name__)


@dataclass
class ReviewRecord:
    app_name: str
    app_url: str
    review_text: str
    reviewer_name: str
    rating: Optional[float]
    rating_text: str
    date_text: str




# ------------------------
# Helpers
# ------------------------
from playwright.async_api import Locator


async def _extract_reviews(
    context: BrowserContext,
    page: Page,
    listing_url: str,
    app_name: str,
    max_reviews: int,
    seen_keys: Set[str],
) -> int:
    """
    Extract review cards from the current page and push them to the Apify dataset.

    Returns:
        Number of NEW reviews pushed.
    """

    # Optional: keep your debug HTML dump
    html = await page.content()
    with open("appex_reviews_debug.html", "w", encoding="utf-8") as f:
        f.write(html)
    Actor.log.info("[REVIEWS] Saved current reviews HTML to appex_reviews_debug.html")

    # 1. Find review cards (keep your existing selector if it works well)
    review_selectors = [
        "article",                      # many AppExchange layouts use article per review
        "div[data-review-id]",
        "div[class*='review']",
    ]

    review_cards = None
    for sel in review_selectors:
        locator = page.locator(sel)
        if await locator.count() > 0:
            review_cards = locator
            Actor.log.info(f"[REVIEWS] Using selector '{sel}' with {await locator.count()} cards.")
            break

    if review_cards is None:
        Actor.log.warning("[REVIEWS] No review cards found with any selector.")
        return 0

    pushed = 0
    total_cards = await review_cards.count()
    Actor.log.info(f"[REVIEWS] Found {total_cards} potential review cards.")

    for idx in range(total_cards):
        card = review_cards.nth(idx)

        # --- your existing rating/date extraction should stay here ---
        # Example (replace with your real logic):
        try:
            # Rating
            rating = None
            try:
                # example: stars via aria-label='5 out of 5'
                stars_el = card.locator("[aria-label*='out of 5']")
                if await stars_el.count() > 0:
                    aria = (await stars_el.first.get_attribute("aria-label")) or ""
                    # extract first number
                    import re
                    m = re.search(r"(\d+(?:\.\d+)?)", aria)
                    if m:
                        rating = float(m.group(1))
            except Exception:
                rating = None

            # Review date
            review_date_str = None
            try:
                date_el = card.locator("time, [data-review-date]")
                if await date_el.count() > 0:
                    review_date_str = (await date_el.first.get_attribute("datetime")) or \
                                      (await date_el.first.inner_text())
            except Exception:
                review_date_str = None

            # --- NEW: extract full text & title ---
            review_text, review_title = await _extract_review_text_and_title_from_card(card)

            # Create a key to avoid duplicates
            key = f"{listing_url}|{review_date_str}|{review_text[:50]}"
            if key in seen_keys:
                continue
            seen_keys.add(key)

            item = {
                "listing_id": listing_url.split("listingId=")[-1] if "listingId=" in listing_url else None,
                "app_name": app_name,
                "app_url": listing_url,
                "review_date": review_date_str,
                "rating": rating,
                "review_title": review_title,
                "review_text": review_text,
                # You can also attach reviewer metadata here if you have it:
                # "reviewer_name": ...,
                # "user_vanity": ...,
                # etc.
            }

            # Do NOT push reviews to dataset (Store QA stability)
            # await Actor.push_data(item)

            pushed += 1

            if max_reviews and pushed >= max_reviews:
                Actor.log.info(f"[REVIEWS] Reached max_reviews={max_reviews}, stopping.")
                break

        except Exception as e:
            Actor.log.warning(f"[REVIEWS] Failed to parse review card {idx}: {e}")

    Actor.log.info(f"[REVIEWS] Pushed {pushed} new reviews from page {listing_url}")
    return pushed
def _parse_rating_fallback(rating_text: str) -> Optional[float]:
    """Fallback: extract first number like '5' or '4.5' from rating text."""
    if not rating_text:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", rating_text)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None

async def _click_reviews_tab(page: Page) -> None:
    """
    Try to navigate to the Reviews section/tab on the app detail page.
    Best-effort selectors; may need tuning.
    """
    selectors = [
        "a[href*='#reviews']",
        "a[role='tab'][data-region-name*='reviews']",
        "button[role='tab'][*='Review']",
        "button[role='tab'][*='review']",
        "a[role='tab']:has-text('Reviews')",
        "button:has-text('Reviews')",
    ]

    for sel in selectors:
        try:
            btn = await page.query_selector(sel)
        except Exception:
            btn = None
        if btn:
            log.info("[REVIEWS] Clicking reviews tab via '%s'", sel)
            await btn.click()
            await page.wait_for_timeout(3000)
            return

    log.info("[REVIEWS] No explicit reviews tab found; assuming reviews are visible by default.")


async def _collect_review_cards(page: Page) -> List[ElementHandle]:
    """
    Try several possible review-card selectors and return the first non-empty set.
    """
    review_selectors = [
        "article[data-review-id]",
        "div[data-review-id]",
        "div[class*='review-card']",
        "div[class*='review']",
        "article",
    ]

    for sel in review_selectors:
        try:
            cards = await page.query_selector_all(sel)
        except Exception:
            cards = []
        # Heuristic: only accept selector if count is reasonable (>0 but not thousands)
        if cards and len(cards) < 1000:
            log.info("[REVIEWS] Found %d review cards with selector '%s'", len(cards), sel)
            return cards

    log.warning("[REVIEWS] No review cards found with known selectors.")
    return []


async def _first_text(root: Any, selectors: List[str]) -> str:
    """
    Generic helper: first non-empty text among selectors.
    Works for Page or ElementHandle.
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


def _parse_rating(rating_raw: str) -> Optional[float]:
    if not rating_raw:
        return None
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)", rating_raw)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _make_review_key(app_name: str, reviewer_name: str, date_text: str, review_text: str) -> str:
    snippet = (review_text or "")[:80]
    return f"{app_name}::{reviewer_name}::{date_text}::{snippet}"

def _make_review_permalink(page_url: str, review_key: str) -> str:
    """
    Build a stable hash-based permalink for a single review.

    Example:
      base page_url: https://...appxListingDetail?...&tab=r
      review_key:    app_name::Tonya Lee::06/02/2025::Great app...

    Result:
      https://...appxListingDetail?...&tab=r#rev-1a2b3c4d5e
    """
    base_url = page_url.split("#", 1)[0]
    digest = hashlib.sha1(review_key.encode("utf-8", "ignore")).hexdigest()[:10]
    return f"{base_url}#rev-{digest}"


# ------------------------
# Core extraction for one app
# ------------------------

async def scrape_reviews_for_app(
    page: Page,
    app_name: str,
    app_url: str,
    max_reviews: int,
    seen_keys: Set[str],
    minimal: bool = False,
) -> List[Dict[str, Any]]:

    """
    Open app detail page, go to Reviews section, and extract up to max_reviews reviews.
    Pushes each review to the Apify dataset via Actor.push_data().
    Returns list of dicts for optional Excel export.
    """
    log.info("[REVIEWS] Opening app detail for reviews: %s", app_url)
    await page.goto(app_url, wait_until="networkidle", timeout=60000)

    await _click_reviews_tab(page)

    collected: List[Dict[str, Any]] = []
    last_count = -1

    while len(collected) < max_reviews:
        cards = await _collect_review_cards(page)
        if not cards:
            break

        # Extract from current visible cards
        for card in cards:
            try:
                # We still query "title" in case some apps have it,
                # but we WON'T export it as a separate column.
                _unused_title = await _first_text(
                    card,
                    [
                        "h3",
                        "h2",
                        "div[class*='title']",
                        "span[class*='title']",
                    ],
                )

                review_text = await _first_text(
                    card,
                    [
                        "div[class*='body']",
                        "div[class*='text']",
                        "p",
                    ],
                )

                reviewer_name = await _first_text(
                    card,
                    [
                        "span[type-style='body-2'][class*='bolded']",  # bolded / bolded-mobile
                        "span[class*='reviewer']",
                        "span[class*='user']",
                        "div[class*='author']",
                    ],
                )

                rating_text = await _first_text(
                    card,
                    [
                        "wds-star-ratings span.label[type-style='body-3']",  # "5 out of 5 stars"
                        "wds-star-ratings span.label",
                        "span[aria-label*='out of 5']",
                        "span[class*='rating']",
                    ],
                )

                date_text = await _first_text(
                    card,
                    [
                        "div.right-align-mobile[type-style='body-3'] a",  # 06/02/2025
                        "wds-link[data-testid='review-date-link'] a",
                        "time",
                        "span[class*='date']",
                        "div[class*='date']",
                    ],
                )

                # -------------------------------------------------
                # Skip non-review / incomplete blocks
                # -------------------------------------------------
                if (
                    not reviewer_name
                    or not review_text
                    or len(review_text.strip()) < 20
                    or not rating_text
                    or not date_text
                ):
                    # This will skip the odd first card that had no rating
                    continue

                rating = _parse_rating(rating_text)
                if rating is None:
                    # Optional fallback: extract number from "5 out of 5 stars"
                    m = re.search(r"(\d+(?:\.\d+)?)", rating_text or "")
                    if m:
                        try:
                            rating = float(m.group(1))
                        except Exception:
                            rating = None

                if rating is None:
                    # If we still don't have rating, better skip this card
                    continue

                # At this point we are sure it's a real review with rating
                key = _make_review_key(app_name, reviewer_name, date_text, review_text)
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                # --- Build clean record (no title, no role/company, no extra URLs) ---
                record = ReviewRecord(
                    app_name=app_name,
                    app_url=app_url,
                    review_text=review_text,
                    reviewer_name=reviewer_name,
                    rating=rating,
                    rating_text=rating_text,
                    date_text=date_text,
                )

                if minimal:
                    # Light dataset: only core review info
                    record_dict = {
                        "app_name": record.app_name,
                        "app_url": record.app_url,
                        "review_text": record.review_text,
                        "rating": record.rating,
                        "rating_text": record.rating_text,
                        "date_text": record.date_text,
                    }
                else:
                    # Full dataset: includes reviewer_name
                    record_dict = asdict(record)

                collected.append(record_dict)

                # Push to Apify dataset
                # Do NOT push reviews to dataset (Store QA stability)
                # await Actor.push_data(record_dict)
                pass

                if len(collected) >= max_reviews:
                    break

            except Exception as e:
                log.warning("[REVIEWS] Failed to parse one review card: %s", e, exc_info=True)
                continue

        # Stopping condition: no growth
        if len(collected) == last_count:
            break
        last_count = len(collected)

        if len(collected) >= max_reviews:
            break

        # Try "Load more" / scroll for more reviews
        load_more_selectors = [
            "button:has-text('Show more')",
            "button:has-text('Load more')",
            "button[aria-label*='More reviews']",
        ]
        clicked = False
        for sel in load_more_selectors:
            try:
                btn = await page.query_selector(sel)
            except Exception:
                btn = None
            if btn:
                log.info("[REVIEWS] Clicking load more via '%s'", sel)
                await btn.click()
                await page.wait_for_timeout(3000)
                clicked = True
                break

        if not clicked:
            # Fallback: scroll the reviews container / page
            try:
                await page.mouse.wheel(0, 2000)
                await page.wait_for_timeout(2000)
            except Exception:
                break

    log.info("[REVIEWS] Collected %d reviews for app '%s'", len(collected), app_name)
    return collected
