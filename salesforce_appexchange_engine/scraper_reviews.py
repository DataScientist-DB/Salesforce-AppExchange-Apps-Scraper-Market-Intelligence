from typing import Any, Dict, Set, Tuple

from apify import Actor
from playwright.async_api import async_playwright, Page, BrowserContext


# ----------- Browser / Context helpers -----------


async def _create_context(headless: bool, proxy_settings: Dict[str, Any]) -> BrowserContext:
    """Create a Playwright browser context, with optional Apify Proxy."""
    playwright = await async_playwright().start()

    browser_args = {"headless": headless}
    context_kwargs: Dict[str, Any] = {}

    if proxy_settings and proxy_settings.get("useApifyProxy"):
        try:
            proxy_cfg = await Actor.create_proxy_configuration(
                {
                    "groups": proxy_settings.get("apifyProxyGroups") or None,
                    "country_code": proxy_settings.get("apifyProxyCountry") or None,
                }
            )
            proxy_url = await proxy_cfg.new_url()
            Actor.log.info(f"[REVIEWS] USING_APIFY_PROXY: {proxy_url}")
            context_kwargs["proxy"] = {"server": proxy_url}
        except Exception as e:
            Actor.log.warning(f"[REVIEWS] Failed to configure Apify Proxy, continuing without. Error: {e}")

    browser = await playwright.chromium.launch(**browser_args)
    context = await browser.new_context(**context_kwargs)
    context.__dict__["_apify_playwright"] = playwright
    return context


async def _close_context(context: BrowserContext) -> None:
    """Gracefully close browser and Playwright."""
    playwright = context.__dict__.get("_apify_playwright")
    await context.close()
    if playwright:
        await playwright.stop()


# ----------- Page helpers -----------


async def _goto_reviews_tab(page: Page, listing_url: str) -> Tuple[str, str]:
    """
    Open the listing detail page and make sure the Reviews tab is active.

    Returns (canonical_listing_url, app_name).
    """
    # Ensure we are on the reviews tab: ?tab=r
    if "tab=" not in listing_url:
        if "?" in listing_url:
            listing_url = listing_url + "&tab=r"
        else:
            listing_url = listing_url + "?tab=r"

    Actor.log.info(f"[REVIEWS] Navigating to listing URL: {listing_url}")
    await page.goto(listing_url, wait_until="networkidle")

    # Let the UI finish rendering
    await page.wait_for_timeout(4000)

    # Try to extract app name (best-effort, may need selector tuning)
    app_name = ""
    try:
        # Common patterns: main heading at top of listing
        name_loc = page.locator("h1").first
        if await name_loc.is_visible():
            app_name = (await name_loc.inner_text()).strip()
    except Exception:
        pass

    Actor.log.info(f"[REVIEWS] App name detected: '{app_name}'")
    return listing_url, app_name


async def _click_reviews_load_more(page: Page) -> bool:
    """
    Try to click a 'Show more reviews / Load more' button.
    Returns True if clicked, False if not found.
    """
    candidates = [
        "button:has-text('Show more')",
        "button:has-text('More reviews')",
        "button:has-text('Load more')",
        "button[aria-label*='More reviews']",
        "button[aria-label*='Show more']",
    ]

    for sel in candidates:
        btn = page.locator(sel).first
        if await btn.is_visible():
            Actor.log.info(f"[REVIEWS] Clicking reviews pagination button via selector: {sel}")
            await btn.click()
            await page.wait_for_timeout(3000)
            return True

    Actor.log.info("[REVIEWS] No more review pagination button found.")
    return False

async def _scrape_user_profile(context: BrowserContext, profile_url: str) -> Dict[str, str]:
    """
    Open a reviewer's profile URL in a new tab and extract basic fields.
    This is best-effort; you'll likely want to refine selectors after
    inspecting a real profile page HTML.
    """
    profile_data: Dict[str, str] = {
        "reviewer_profile_url": profile_url,
        "profile_name": "",
        "profile_company": "",
        "profile_title": "",
        "profile_location": "",
    }

    try:
        page = await context.new_page()
        Actor.log.info(f"[REVIEWS] Opening reviewer profile: {profile_url}")
        await page.goto(profile_url, wait_until="networkidle")
        await page.wait_for_timeout(3000)

        # Save debug HTML for the LAST visited profile (for tuning)
        try:
            html = await page.content()
            with open("appex_profile_debug.html", "w", encoding="utf-8") as f:
                f.write(html)
            Actor.log.info("[REVIEWS] Saved last profile HTML to appex_profile_debug.html")
        except Exception:
            pass

        # --- BEST-EFFORT SELECTORS ---
        # You should open appex_profile_debug.html in a browser
        # and adjust these selectors to match the real DOM.
        try:
            # Name – often main heading or highlighted text
            name_loc = page.locator("h1, h2, .slds-text-heading_large, .profile-name").first
            if await name_loc.is_visible():
                profile_data["profile_name"] = (await name_loc.inner_text()).strip()
        except Exception:
            pass

        try:
            # Company / organization
            company_loc = page.locator(".profile-company, .slds-text-title, [data-profile-company]").first
            if await company_loc.is_visible():
                profile_data["profile_company"] = (await company_loc.inner_text()).strip()
        except Exception:
            pass

        try:
            # Title / role
            title_loc = page.locator(".profile-title, .slds-text-body_small, [data-profile-title]").first
            if await title_loc.is_visible():
                profile_data["profile_title"] = (await title_loc.inner_text()).strip()
        except Exception:
            pass

        try:
            # Location
            loc_loc = page.locator(".profile-location, [data-profile-location]").first
            if await loc_loc.is_visible():
                profile_data["profile_location"] = (await loc_loc.inner_text()).strip()
        except Exception:
            pass

        await page.close()

    except Exception as e:
        Actor.log.warning(f"[REVIEWS] Failed to scrape reviewer profile {profile_url}: {e}")

    return profile_data

# ----------- Review extraction -----------


async def _extract_reviews(
    context: BrowserContext,
    page: Page,
    listing_url: str,
    app_name: str,
    max_reviews: int,
    seen_keys: Set[str],
) -> int:
    """
    Extract review headers + details from AppExchange review pages.
    This version uses stable review header structure detected from real DOM.
    """

    # Save debug HTML
    html = await page.content()
    with open("appex_reviews_debug.html", "w", encoding="utf-8") as f:
        f.write(html)
    Actor.log.info("[REVIEWS] Saved current reviews HTML to appex_reviews_debug.html")

    # ------------------------------------------------------------
    # 1) DETECT REAL APPEXCHANGE REVIEW HEADERS
    # Each review header looks like:
    #   <div class="flex-container">
    #       ... contains <wds-link data-testid='review-date-link'>
    # ------------------------------------------------------------
    header_loc = page.locator(
        "div.flex-container:has(wds-link[data-testid='review-date-link'])"
    )
    header_count = await header_loc.count()
    Actor.log.info(f"[REVIEWS] Header selector matched {header_count} elements.")

    if header_count == 0:
        Actor.log.warning("[REVIEWS] No review headers detected!")
        return 0

    cards = [header_loc.nth(i) for i in range(header_count)]
    Actor.log.info(f"[REVIEWS] Using stable header selector with {len(cards)} items.")

    pushed = 0

    for idx, card in enumerate(cards):
        if pushed >= max_reviews:
            break

        try:
            # -----------------------------------------
            # REVIEWER PROFILE URL (trailblazer.me)
            # -----------------------------------------
            reviewer_profile_url = ""
            try:
                profile_link = card.locator("a[href*='trailblazer.me']").first
                if await profile_link.is_visible():
                    href = await profile_link.get_attribute("href")
                    if href:
                        reviewer_profile_url = href.strip()

            except:
                pass

            # -----------------------------------------
            # REVIEWER NAME
            # <span class="bolded">NAME</span>
            # -----------------------------------------
            reviewer_name = ""
            try:
                name_loc = card.locator("span.bolded").first
                if await name_loc.is_visible():
                    reviewer_name = (await name_loc.inner_text()).strip()
            except:
                pass

            # -----------------------------------------
            # STAR RATING (from text "5 out of 5 stars")
            # inside <wds-star-ratings><span class='label'>...</span>
            # -----------------------------------------
            rating = None
            try:
                rating_loc = card.locator("wds-star-ratings span.label").first
                if await rating_loc.is_visible():
                    text = (await rating_loc.inner_text()).strip()
                    # Example: "5 out of 5 stars"
                    num = text.split(" ")[0]
                    if num.isdigit():
                        rating = int(num)
            except:
                pass

            # -----------------------------------------
            # REVIEW DATE
            # <wds-link data-testid="review-date-link"><a>MM/DD/YYYY</a>
            # -----------------------------------------
            review_date = ""
            try:
                date_loc = card.locator("wds-link[data-testid='review-date-link'] a").first
                if await date_loc.is_visible():
                    review_date = (await date_loc.inner_text()).strip()
            except:
                pass

            # -----------------------------------------
            # FULL REVIEW TEXT (temporary)
            # For now: header text; later, body container will be added.
            # -----------------------------------------
            full_text = (await card.inner_text()).strip()

            # Skip summary blocks
            if full_text.startswith("Showing ") and "Reviews" in full_text:
                continue

            # -----------------------------------------
            # REVIEW TITLE (not visible in sample yet)
            # Leave blank until we detect titles in DOM.
            # -----------------------------------------
            review_title = ""

            # -----------------------------------------
            # Duplicate protection
            # -----------------------------------------
            key = f"{reviewer_name}|{review_date}|{rating}|{full_text[:80]}"
            if key in seen_keys:
                continue
            seen_keys.add(key)

            # -----------------------------------------
            # BUILD ITEM
            # -----------------------------------------
            item = {
                "type": "review",
                "listing_url": listing_url,
                "app_name": app_name,
                "reviewer_name": reviewer_name,
                "review_title": review_title,
                "review_date": review_date,
                "rating_stars": rating,
                "review_text": full_text,
                "reviewer_profile_url": reviewer_profile_url,
            }

            # -----------------------------------------
            # (Optional) Scrape profile page for company, role, etc.
            # -----------------------------------------
            if reviewer_profile_url:
                try:
                    profile_data = await _scrape_user_profile(context, reviewer_profile_url)
                    item.update(profile_data)
                except Exception as e:
                    Actor.log.warning(f"[PROFILE] Failed to scrape profile: {e}")

            # Push to dataset
            # await Actor.push_data(item)  # disabled for Store QA

            pushed += 1

        except Exception as e:
            Actor.log.warning(f"[REVIEWS] Error in review block: {e}")

    Actor.log.info(f"[REVIEWS] Pushed {pushed} NEW reviews from current page.")
    return pushed

# ----------- Public entrypoint for reviews -----------


async def scrape_reviews_for_listing(
    listing_url: str,
    max_reviews: int,
    headless: bool,
    proxy_settings: Dict[str, Any],
) -> int:
    """
    Scrape up to `max_reviews` reviews from a single AppExchange listing.
    """
    context = await _create_context(headless=headless, proxy_settings=proxy_settings)
    page = await context.new_page()

    total = 0
    seen_keys: Set[str] = set()

    try:
        # Go to reviews tab and detect canonical URL + app name
        canonical_url, app_name = await _goto_reviews_tab(page, listing_url)

        # ----- First page -----
        new_count = await _extract_reviews(
            context=context,
            page=page,
            listing_url=canonical_url,
            app_name=app_name,
            max_reviews=max_reviews - total,
            seen_keys=seen_keys,
        )
        total += new_count
        Actor.log.info(f"[REVIEWS] First page: scraped {new_count} reviews (total={total}).")

        # ----- Pagination loop -----
        while total < max_reviews:
            clicked = await _click_reviews_load_more(page)
            if not clicked:
                Actor.log.info("[REVIEWS] No more 'load more reviews' button, stopping.")
                break

            new_count = await _extract_reviews(
                context=context,
                page=page,
                listing_url=canonical_url,
                app_name=app_name,
                max_reviews=max_reviews - total,
                seen_keys=seen_keys,
            )

            if new_count == 0:
                Actor.log.info("[REVIEWS] No new reviews on next page, stopping.")
                break

            total += new_count
            Actor.log.info(f"[REVIEWS] Next page: scraped {new_count} reviews (total={total}).")

    except Exception as e:
        Actor.log.exception(f"[REVIEWS] Error while scraping listing '{listing_url}': {e}")
    finally:
        await _close_context(context)

    return total

