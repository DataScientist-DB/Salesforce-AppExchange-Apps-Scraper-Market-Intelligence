import os
from typing import Any, Dict, List

from apify import Actor

from .scraper_apps import scrape_apps_for_term
from .scraper_reviews import scrape_reviews_for_listing


async def run_multi(actor_input: Dict[str, Any]) -> None:
    """
    Main orchestrator for Salesforce AppExchange Discovery Engine.

    Modes:
      - "apps": search + pagination for app listings
      - "reviews": scrape reviews from specific listing URLs
      - "consultants": (not implemented yet)
    """
    Actor.log.info("Salesforce AppExchange Discovery Engine starting...")

    storage_dir = os.environ.get("APIFY_LOCAL_STORAGE_DIR")
    Actor.log.info(f"APIFY_LOCAL_STORAGE_DIR (inside actor): {storage_dir}")

    Actor.log.info(f"RAW INPUT: {actor_input}")

    mode: str = actor_input.get("mode", "apps")
    headless: bool = bool(actor_input.get("headless", True))

    # Accept both "proxy" (new schema) and "proxySettings" (old naming)
    proxy_settings: Dict[str, Any] = actor_input.get("proxy") or actor_input.get("proxySettings") or {}

    Actor.log.info(f"Mode: {mode}")
    Actor.log.info(f"Headless: {headless}")
    Actor.log.info(f"Proxy settings: {proxy_settings}")

    # ------------------------------------------------------------------
    # MODE: APPS (search results, already working)
    # ------------------------------------------------------------------
    if mode == "apps":
        search_terms: List[str] = actor_input.get("searchTerms") or []
        max_results: int = int(actor_input.get("maxResults") or 100)

        if not search_terms:
            Actor.log.warning("No searchTerms provided. Nothing to scrape in apps mode.")
            return

        Actor.log.info(f"Search terms: {search_terms}")
        Actor.log.info(f"Max results: {max_results}")

        total_items = 0
        for term in search_terms:
            if total_items >= max_results:
                Actor.log.info(f"Reached max_results={max_results}, stopping further terms.")
                break

            remaining = max_results - total_items
            Actor.log.info(f"Scraping APPS for term='{term}' with up to {remaining} results...")

            new_items = await scrape_apps_for_term(
                term=term,
                limit=remaining,
                headless=headless,
                proxy_settings=proxy_settings,
            )

            total_items += new_items
            Actor.log.info(
                f"Finished term='{term}'. New items: {new_items}. Total so far: {total_items}"
            )

        Actor.log.info(f"Done (apps mode). Total items scraped: {total_items}.")
        return

    # ------------------------------------------------------------------
    # MODE: REVIEWS (NEW)
    # ------------------------------------------------------------------
    if mode == "reviews":
        listing_urls: List[str] = actor_input.get("listingUrls") or []
        max_reviews: int = int(actor_input.get("maxReviews") or 300)

        if not listing_urls:
            Actor.log.warning("No listingUrls provided. Nothing to scrape in reviews mode.")
            return

        Actor.log.info(f"Listing URLs: {listing_urls}")
        Actor.log.info(f"Max reviews per listing: {max_reviews}")

        grand_total = 0
        for url in listing_urls:
            Actor.log.info(f"Scraping REVIEWS for listing: {url}")
            count = await scrape_reviews_for_listing(
                listing_url=url,
                max_reviews=max_reviews,
                headless=headless,
                proxy_settings=proxy_settings,
            )
            grand_total += count
            Actor.log.info(
                f"Finished listing '{url}'. Reviews scraped: {count}. Grand total so far: {grand_total}"
            )

        Actor.log.info(f"Done (reviews mode). Total reviews scraped: {grand_total}.")
        return

    # ------------------------------------------------------------------
    # MODE: CONSULTANTS (not implemented yet)
    # ------------------------------------------------------------------
    if mode == "consultants":
        Actor.log.warning("Mode 'consultants' is not implemented yet.")
        return

    # ------------------------------------------------------------------
    # UNKNOWN MODE
    # ------------------------------------------------------------------
    Actor.log.warning(f"Unknown mode '{mode}'. Nothing to do.")
