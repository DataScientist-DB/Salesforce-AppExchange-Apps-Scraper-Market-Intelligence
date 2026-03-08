# reviews/reviews_html_runner.py

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pandas as pd
from apify import Actor
from playwright.async_api import Page, Locator

from logger import get_logger
from utils.errors import ConfigError
from apps.apps_runner import _to_int, _create_playwright_context  # reuse helpers

log = get_logger(__name__)


# -----------------------------
# Small helpers
# -----------------------------
def _make_review_key(rec: Dict[str, Any]) -> Tuple[Any, ...]:
    """
    Build a deduplication key for a review based on a few stable fields.
    """
    return (
        rec.get("reviewer_name") or "",
        rec.get("review_date") or "",
        rec.get("rating") or "",
        (rec.get("review_text") or "")[:80],
    )


async def _open_reviews_tab(page: Page) -> None:
    """
    Try several strategies to open the 'Reviews' tab on the app detail page.
    This is best-effort and may need minor selector tuning based on DOM.
    """
    # Strategy 1: role=tab with text 'Reviews'
    try:
        tab = page.get_by_role(
            "tab", name=lambda name: name and "review" in name.lower()
        )
        await tab.click()
        await page.wait_for_timeout(2_000)
        log.info("[REVIEWS_HTML] Clicked Reviews tab via ARIA role.")
        return
    except Exception:
        pass

    # Strategy 2: any button/link containing text 'Reviews'
    candidates = [
        "//button[contains(translate(., 'REVIEWS', 'reviews'), 'reviews')]",
        "//a[contains(translate(., 'REVIEWS', 'reviews'), 'reviews')]",
    ]
    for xpath in candidates:
        try:
            btn = page.locator(f"xpath={xpath}").first
            if await btn.count() > 0:
                await btn.click()
                await page.wait_for_timeout(2_000)
                log.info("[REVIEWS_HTML] Clicked Reviews tab via XPath selector.")
                return
        except Exception:
            continue

    log.warning("[REVIEWS_HTML] Could not find a Reviews tab; continuing on main page.")


# -----------------------------
# Per-card mapping helper
# -----------------------------
async def _map_review_card(card: Locator) -> Dict[str, Any]:
    """
    Map one HTML review card into a flat record.

    Uses selectors that match your real DOM, e.g.:

      <div class="flex-container header">
        <div class="flex-container">
          <a ...>
            <span type-style="body-2" class="bolded">Michael Carter</span>
          </a>
        </div>
        <wds-star-ratings>
          <span class="label" type-style="body-3" slot="label">
            3 out of 5 stars
          </span>
        </wds-star-ratings>
      </div>
    """

    async def _safe_text(selector: str) -> str:
        try:
            loc = card.locator(selector).first
            if await loc.count() == 0:
                return ""
            txt = await loc.text_content()
            return (txt or "").strip()
        except Exception:
            return ""

    # -------------------------
    # Reviewer name
    # -------------------------
    # Primary: flex header bolded span (your snippet: Michael Carter)
    reviewer_name = await _safe_text("div.flex-container.header a span.bolded")

    if not reviewer_name:
        # Try the same primary selector with direct locator access
        try:
            primary = card.locator("div.flex-container.header a span.bolded")
            if await primary.count() > 0:
                candidate = (await primary.inner_text() or "").strip()
                if candidate:
                    reviewer_name = candidate
        except Exception:
            pass

    if not reviewer_name:
        # Fallback selectors – includes plain <span class="bolded">
        fallback_selectors = [
            "div.flex-container.header span.bolded",
            "a span.bolded",
            "span.bolded",  # Handles: <span class="bolded">Archana Shirwant</span>
            "[class*='reviewer']",
            "[class*='author']",
            "[class*='user']",
        ]
        for sel in fallback_selectors:
            try:
                loc = card.locator(sel).first
                if await loc.count() == 0:
                    continue
                candidate = (await loc.inner_text() or "").strip()
                # Avoid capturing rating labels or long paragraphs as "name"
                if candidate and len(candidate.split()) <= 5:
                    reviewer_name = candidate
                    break
            except Exception:
                continue

    # -------------------------
    # Review title
    # -------------------------
    review_title = ""
    for sel in [
        "h3",
        "h2",
        "h4",
        "div[class*='title']",
        "span[class*='title']",
    ]:
        review_title = await _safe_text(sel)
        if review_title:
            break

    # -------------------------
    # Review text
    # -------------------------
    # Try more specific containers first, then the first <p>.
    review_text = ""
    for sel in [
        "div[class*='review-body']",
        "div[class*='reviewText']",
        "div[class*='slds-truncate']",
        "p",
    ]:
        review_text = await _safe_text(sel)
        if review_text:
            break

    # -------------------------
    # Review date
    # -------------------------
    review_date = ""
    # Prefer <time> elements if present
    try:
        time_loc = card.locator("time").first
        if await time_loc.count() > 0:
            dt = await time_loc.get_attribute("datetime")
            if dt:
                review_date = dt.strip()
            else:
                txt = await time_loc.text_content()
                if txt:
                    review_date = txt.strip()
    except Exception:
        pass

    if not review_date:
        # Fallback: body-3 text that is NOT the star label
        for sel in [
            "span[type-style='body-3']:not(wds-star-ratings span.label)",
            "span[class*='date']",
        ]:
            review_date = await _safe_text(sel)
            if review_date:
                break

    # -------------------------
    # Rating
    # -------------------------
    rating = None
    try:
        # Typical pattern: "3 out of 5 stars" inside the label/span
        rating_text = await _safe_text(
            "wds-star-ratings span.label, "
            "wds-star-ratings [slot='label'], "
            "span[slot='label']"
        )
        if rating_text:
            import re

            m = re.search(r"([0-9]+(?:\.[0-9]+)?)", rating_text)
            if m:
                rating = float(m.group(1))
    except Exception:
        rating = None

    # -------------------------
    # Likes / helpful count (placeholder, can be improved later)
    # -------------------------
    likes = None
    # Example for later:
    # likes_text = await _safe_text("button[class*='helpful'], span[class*='helpful']")
    # parse integer if needed

    return {
        "review_title": review_title,
        "review_text": review_text,
        "reviewer_name": reviewer_name,
        "review_date": review_date,
        "rating": rating,
        "likes": likes,
    }


async def _extract_reviews_from_dom(page: Page) -> List[Dict[str, Any]]:
    """
    Extract review records from the current DOM.

    - Find review cards (article/div with 'review' in class or data attributes).
    - For each card, call _map_review_card to produce a flat record.
    """
    reviews: List[Dict[str, Any]] = []

    # First, try specific selectors, then fall back to generic ones.
    card_selectors = [
        "article[data-review-id]",
        "div[data-review-id]",
        "article[class*='review']",
        "div[class*='review-card']",
        "div[class*='review']",
    ]

    card_locator: Locator | None = None
    for sel in card_selectors:
        loc = page.locator(sel)
        try:
            count = await loc.count()
        except Exception:
            continue
        if count > 0:
            card_locator = loc
            log.info(
                "[REVIEWS_HTML] Found %d review cards using selector: %s", count, sel
            )
            break

    # Fallback: any <article> elements
    if card_locator is None:
        loc = page.locator("article")
        try:
            count = await loc.count()
        except Exception:
            count = 0
        if count > 0:
            card_locator = loc
            log.info(
                "[REVIEWS_HTML] Falling back to %d generic <article> cards.", count
            )

    if card_locator is None:
        log.warning("[REVIEWS_HTML] No review cards detected on page.")
        return reviews

    count = await card_locator.count()
    for i in range(count):
        card = card_locator.nth(i)
        try:
            rec = await _map_review_card(card)
            # Skip completely empty rows
            if not any(
                rec.get(k) for k in ("review_title", "review_text", "reviewer_name")
            ):
                continue
            reviews.append(rec)
        except Exception as e:
            log.warning("[REVIEWS_HTML] Failed to parse one review card: %s", e)
            continue

    log.info("[REVIEWS_HTML] Parsed %d reviews from DOM.", len(reviews))
    return reviews


# -----------------------------
# Main HTML reviews flow
# -----------------------------
async def run_reviews_html_flow(config: Dict[str, Any], project_root: str) -> None:
    """
    REVIEWS mode (HTML-based, Playwright).

    Reads APPS.csv / APPS_FULL.csv and writes:
      - REVIEWS.csv / REVIEWS.xlsx
      - REVIEWS_APPS/<listing_id>.csv
    """
    project_root_path = Path(project_root)

    # 1) load APPS
    apps_path = project_root_path / "APPS.csv"
    if not apps_path.exists():
        apps_full = project_root_path / "APPS_FULL.csv"
        if apps_full.exists():
            apps_path = apps_full
        else:
            raise ConfigError(
                f"REVIEWS (HTML) mode requires APPS.csv or APPS_FULL.csv in {project_root}."
            )

    df_apps = pd.read_csv(apps_path)
    if df_apps.empty:
        raise ConfigError("REVIEWS (HTML) mode: APPS file is empty.")

    total_apps = len(df_apps)

    start_index = _to_int(config.get("startIndex"), 1)
    end_index = _to_int(config.get("endIndex"), total_apps)
    if start_index < 1:
        start_index = 1
    if end_index > total_apps or end_index == 0:
        end_index = total_apps
    if start_index > end_index:
        raise ConfigError(
            f"REVIEWS (HTML): startIndex ({start_index}) > endIndex ({end_index})."
        )

    max_reviews_per_app = _to_int(config.get("maxReviewsPerApp"), 0)  # 0 = no limit
    max_scroll_steps = _to_int(config.get("maxReviewScrolls", 10), 10)

    log.info(
        "[REVIEWS_HTML] Running for app rows %d..%d (total apps: %d)",
        start_index,
        end_index,
        total_apps,
    )
    log.info(
        "[REVIEWS_HTML] maxReviewsPerApp=%d (0 means no limit), maxReviewScrolls=%d",
        max_reviews_per_app,
        max_scroll_steps,
    )

    per_app_dir = project_root_path / "REVIEWS_APPS"
    per_app_dir.mkdir(parents=True, exist_ok=True)

    storage_dir = Path(os.environ.get("APIFY_LOCAL_STORAGE_DIR", project_root))
    screenshots_dir = storage_dir / "screenshots_reviews"
    screenshots_dir.mkdir(parents=True, exist_ok=True)

    df_slice = df_apps.iloc[start_index - 1 : end_index].copy()

    all_reviews: List[Dict[str, Any]] = []

    headless = bool(config.get("headless", True))
    proxy_settings = config.get("proxySettings") or {}

    pw, browser, context = await _create_playwright_context(
        headless=headless,
        proxy_settings=proxy_settings,
    )

    try:
        page = await context.new_page()

        for offset, (_, row) in enumerate(df_slice.iterrows()):
            row_idx = start_index + offset

            listing_id = str(row.get("listing_id") or "").strip()
            app_url = str(row.get("url") or "").strip()
            app_name = str(row.get("name") or "").strip()

            if not app_url:
                log.warning(
                    "[REVIEWS_HTML] Row %d has no app URL; skipping.",
                    row_idx,
                )
                continue

            log.info(
                "[REVIEWS_HTML] === [%d] %s (listing_id=%s) ===",
                row_idx,
                app_name or "<no name>",
                listing_id or "<no id>",
            )

            per_app_reviews: List[Dict[str, Any]] = []
            seen_keys: Set[Tuple[Any, ...]] = set()

            try:
                await page.goto(app_url, wait_until="networkidle", timeout=60_000)
            except Exception as e:
                log.error(
                    "[REVIEWS_HTML] Failed to open app URL %s: %s",
                    app_url,
                    e,
                    exc_info=True,
                )
                safe_idx = str(row_idx).zfill(3)
                screenshot_path = screenshots_dir / f"reviews_open_error_{safe_idx}.png"
                try:
                    await page.screenshot(path=str(screenshot_path), full_page=True)
                    log.info(
                        "[REVIEWS_HTML] Saved open error screenshot: %s",
                        screenshot_path,
                    )
                except Exception:
                    pass
                continue

            # Try to open Reviews tab
            await _open_reviews_tab(page)

            # Scroll & extract
            scroll_step = 0
            while True:
                # Extract reviews currently in DOM
                dom_reviews = await _extract_reviews_from_dom(page)

                for r in dom_reviews:
                    key = _make_review_key(r)
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)

                    r["listing_id"] = listing_id
                    r["app_name"] = app_name
                    r["app_url"] = app_url

                    per_app_reviews.append(r)
                    all_reviews.append(r)

                    if max_reviews_per_app and len(per_app_reviews) >= max_reviews_per_app:
                        break

                if max_reviews_per_app and len(per_app_reviews) >= max_reviews_per_app:
                    log.info(
                        "[REVIEWS_HTML] Reached maxReviewsPerApp (%d) for %s",
                        max_reviews_per_app,
                        listing_id or "<no id>",
                    )
                    break

                scroll_step += 1
                if scroll_step > max_scroll_steps:
                    log.info(
                        "[REVIEWS_HTML] Reached maxReviewScrolls (%d) for %s",
                        max_scroll_steps,
                        listing_id or "<no id>",
                    )
                    break

                # Scroll down to load more reviews
                await page.mouse.wheel(0, 2000)
                await page.wait_for_timeout(1_500)

            log.info(
                "[REVIEWS_HTML] Extracted %d reviews for %s (%s)",
                len(per_app_reviews),
                listing_id or "<no id>",
                app_name or "<no name>",
            )

            if per_app_reviews:
                app_df = pd.DataFrame(per_app_reviews)
                per_app_csv = per_app_dir / f"{listing_id or 'no_id'}.csv"
                app_df.to_csv(per_app_csv, index=False, encoding="utf-8-sig")
                log.info("[REVIEWS_HTML] Per-app CSV written: %s", per_app_csv)
            else:
                log.warning(
                    "[REVIEWS_HTML] No reviews collected for %s; no per-app file.",
                    listing_id or "<no id>",
                )

    finally:
        try:
            await browser.close()
        except Exception:
            pass
        try:
            await pw.stop()
        except Exception:
            pass

    if not all_reviews:
        log.warning("[REVIEWS_HTML] No reviews extracted for any app.")
        return

    df_reviews = pd.DataFrame(all_reviews)
    csv_path = project_root_path / "REVIEWS.csv"
    xlsx_path = project_root_path / "REVIEWS.xlsx"

    df_reviews.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df_reviews.to_excel(xlsx_path, index=False)

    log.info("[REVIEWS_HTML] Exported %d reviews total", len(df_reviews))
    log.info("[REVIEWS_HTML] CSV  : %s", csv_path)
    log.info("[REVIEWS_HTML] XLSX : %s", xlsx_path)
    log.info("[REVIEWS_HTML] Per-app dir: %s", per_app_dir)
