# reviews/reviews_runner.py

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from urllib import request, parse, error as urlerror

from apify import Actor
from logger import get_logger
from utils.errors import ConfigError


log = get_logger(__name__)

async def _store_file_to_kv(key: str, path: Path, content_type: str) -> None:
    try:
        with path.open("rb") as f:
            await Actor.set_value(key, f.read(), content_type=content_type)
        log.info("[REVIEWS] Stored file to KV: %s (key=%s)", path, key)
    except Exception as e:
        log.warning("[REVIEWS] Failed to store %s to KV: %s", path, e)

API_BASE = "https://api.appexchange.salesforce.com/services/apexrest/reviews"

from typing import Any, Dict, List


def _clean_text(text: str | None) -> str:
    """Collapse whitespace and avoid None."""
    if not text:
        return ""
    return " ".join(str(text).split()).strip()


def _extract_reviewer_name(review: Dict[str, Any]) -> str:
    """
    Try to get a human-readable reviewer name from review['user'].
    Sometimes 'user' is a dict, sometimes a string.
    """
    user = review.get("user")
    if isinstance(user, dict):
        # Adjust keys if needed once you inspect one payload
        return _clean_text(
            user.get("name")
            or user.get("fullName")
            or user.get("displayName")
            or ""
        )
    if user is None:
        return ""
    return _clean_text(str(user))


def _extract_review_body(review: Dict[str, Any]) -> str:
    """
    Build a main text body for the review.

    Priority:
      1) 'comments' field if present
      2) Join all free-text responses from 'questionResponses'
    """
    # 1) Direct comments field
    comments = review.get("comments")
    if comments:
        return _clean_text(comments)

    # 2) Fallback: questionResponses
    qr = review.get("questionResponses") or []
    parts: List[str] = []

    if isinstance(qr, list):
        for item in qr:
            if not isinstance(item, dict):
                # e.g. plain string
                parts.append(str(item))
                continue

            # Common key patterns: responseText, responseValue, answer, value
            txt = (
                item.get("responseText")
                or item.get("responseValue")
                or item.get("answer")
                or item.get("value")
            )
            if txt:
                parts.append(str(txt))

    if not parts:
        return ""
    return _clean_text(" ".join(parts))


# ---------- small helpers ----------
def _to_int(v: Any, default: int) -> int:
    try:
        if v is None or v == "":
            return default
        return int(v)
    except Exception:
        return default


def _build_reviews_url(listing_id: str, page_len: int, page_num: int) -> str:
    params = {
        "listingId": listing_id,
        "pageLength": str(page_len),
        "pageNumber": str(page_num),
        "sort": "mr",  # most recent
    }
    return f"{API_BASE}?{parse.urlencode(params)}"


def _http_get_json(url: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
    headers = {
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
    }
    req = request.Request(url, headers=headers, method="GET")
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                log.warning("[REVIEWS] Non-200 status %s for %s", resp.status, url)
                return None
            data = resp.read()
    except urlerror.HTTPError as e:
        log.warning("[REVIEWS] HTTP error %s for %s", e, url)
        return None
    except Exception as e:
        log.warning("[REVIEWS] Request failed for %s: %s", url, e)
        return None

    try:
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        log.warning("[REVIEWS] JSON decode failed for %s: %s", url, e)
        return None


def _normalise_listing_id(raw_url: str, fallback_id: str = "") -> str:
    """
    If listing_id is missing in CSV, try to extract from URL query.
    """
    if fallback_id:
        return fallback_id
    if not raw_url:
        return ""
    try:
        parsed = parse.urlparse(raw_url)
        qs = parse.parse_qs(parsed.query)
        lid = qs.get("listingId", [""])[0]
        return lid or ""
    except Exception:
        return ""


# ---------- review extraction from JSON ----------
def _extract_reviews_from_payload(
    payload: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Robust search for review objects inside arbitrary JSON.

    We recursively walk the JSON tree and treat any list where
    at least one element looks like a review as the reviews list.
    """
    REVIEW_KEYS = {
        "reviewText",
        "rating",
        "reviewerName",
        "reviewDate",
        "reviewTitle",
        "title",
        "text",
        "body",
    }

    def looks_like_review_obj(obj: Any) -> bool:
        if not isinstance(obj, dict):
            return False
        keys = set(obj.keys())
        return bool(keys & REVIEW_KEYS)

    def walk(node: Any) -> List[Dict[str, Any]]:
        if isinstance(node, list):
            dict_items = [x for x in node if isinstance(x, dict)]
            if dict_items and any(looks_like_review_obj(x) for x in dict_items):
                return dict_items
            for item in node:
                found = walk(item)
                if found:
                    return found
            return []

        if isinstance(node, dict):
            for v in node.values():
                found = walk(v)
                if found:
                    return found
            return []

        return []

    reviews = walk(payload)
    return reviews


def _map_review_record(
    raw: Dict[str, Any],
    listing_id: str,
    app_name: str,
    app_url: str,
) -> Dict[str, Any]:
    """
    Map one raw review dict into a flat record.

    - If the real review fields are nested (e.g. raw["review"] or raw["detail"]),
      we first unwrap that.
    - Then we try multiple possible keys for each logical column.
    - We keep only a single main text field: review_text.
    """

    # 1) Try to unwrap a nested review object, if present.
    candidate_inner = None
    for key in ("review", "reviewDetail", "detail", "reviewData"):
        if isinstance(raw.get(key), dict):
            candidate_inner = raw[key]
            break

    inner = candidate_inner or raw

    def g_any(obj: Dict[str, Any], keys: list[str], default: Any = "") -> Any:
        for k in keys:
            if k in obj and obj[k] not in (None, ""):
                return obj[k]
        return default

    # Candidate keys for each logical field
    text_keys = [
        "reviewText",
        "text",
        "body",
        "comment",
        "commentText",
        "reviewBody",
        "Body",
        "comment__c",
        "comments",  # Salesforce API often uses this
    ]
    name_keys = [
        "reviewerName",
        "author",
        "userName",
        "createdByName",
        "userFullName",
        "profileName",
    ]
    date_keys = [
        "reviewDate",
        "date",
        "createdAt",
        "createdDate",
        "lastModifiedDate",
    ]
    rating_keys = [
        "rating",
        "ratingValue",
        "stars",
        "rating__c",
    ]
    likes_keys = [
        "likes",
        "helpfulCount",
        "upvotes",
        "helpfulCount__c",
        "likeCount",  # present in current payload
    ]

    # 2) Extract numeric fields
    rating = g_any(inner, rating_keys, default=None)
    try:
        if rating is not None:
            rating = float(rating)
    except Exception:
        rating = None

    likes = g_any(inner, likes_keys, default=None)
    try:
        if likes is not None:
            likes = int(likes)
    except Exception:
        likes = None

    # 3) Extract main text body
    review_text = g_any(inner, text_keys, default="")
    if not review_text:
        # Try to build from comments/questionResponses
        review_text = _extract_review_body(inner)
        if not review_text:
            review_text = _extract_review_body(raw)
    review_text = _clean_text(review_text)

    # 4) Reviewer name: try flat keys, then fall back to 'user' object/string
    reviewer_name = g_any(inner, name_keys, default="")
    if not reviewer_name:
        reviewer_name = _extract_reviewer_name(inner) or _extract_reviewer_name(raw)
    reviewer_name = _clean_text(reviewer_name)

    # 5) Date
    review_date = g_any(inner, date_keys, default="")
    review_date = _clean_text(review_date)

    rec = {
        "listing_id": listing_id,
        "app_name": app_name,
        "app_url": app_url,
        "review_text": review_text,
        "reviewer_name": reviewer_name,
        "review_date": review_date,
        "rating": rating,
        "likes": likes,
    }
    return rec

# ---------- main flow ----------
async def run_reviews_flow(config: Dict[str, Any], project_root: str) -> None:
    """
    REVIEWS mode (API-based).

    Reads APPS.csv / APPS_FULL.csv and writes:
      - REVIEWS.csv / REVIEWS.xlsx
      - REVIEWS_APPS/<listing_id>.csv
      - debug_reviews_payloads/<listing_id>_page<N>.json (when no reviews detected)
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
                f"REVIEWS mode requires APPS.csv or APPS_FULL.csv in {project_root}."
            )

    df_apps = pd.read_csv(apps_path)
    if df_apps.empty:
        raise ConfigError("REVIEWS mode: APPS file is empty.")

    total_apps = len(df_apps)

    start_index = _to_int(config.get("startIndex"), 1)
    end_index = _to_int(config.get("endIndex"), total_apps)
    if start_index < 1:
        start_index = 1
    if end_index > total_apps or end_index == 0:
        end_index = total_apps
    if start_index > end_index:
        raise ConfigError(
            f"REVIEWS: startIndex ({start_index}) > endIndex ({end_index})."
        )

    max_reviews_per_app = _to_int(config.get("maxReviewsPerApp"), 0)  # 0 = no limit
    page_len_default = 50

    reviews_mode = (config.get("reviewsMode") or "all").strip().lower()
    if reviews_mode not in ("all", "skipifnonew"):
        reviews_mode = "all"

    log.info(
        "[REVIEWS] Running for app rows %d..%d (total apps: %d)",
        start_index,
        end_index,
        total_apps,
    )
    log.info(
        "[REVIEWS] maxReviewsPerApp=%d (0 means no limit), reviewsMode=%s",
        max_reviews_per_app,
        reviews_mode,
    )

    per_app_dir = project_root_path / "REVIEWS_APPS"
    per_app_dir.mkdir(parents=True, exist_ok=True)

    debug_dir = project_root_path / "debug_reviews_payloads"
    debug_dir.mkdir(parents=True, exist_ok=True)

    df_slice = df_apps.iloc[start_index - 1 : end_index].copy()

    all_reviews: List[Dict[str, Any]] = []

    for offset, (_, row) in enumerate(df_slice.iterrows()):
        row_idx = start_index + offset

        raw_listing_id = str(row.get("listing_id") or "").strip()
        app_url = str(row.get("url") or "").strip()
        listing_id = _normalise_listing_id(app_url, fallback_id=raw_listing_id)
        app_name = str(row.get("name") or "").strip()

        if not listing_id:
            log.warning(
                "[REVIEWS] Row %d (%s) has no listing_id; skipping.",
                row_idx,
                app_name or "<no name>",
            )
            continue

        per_app_csv = per_app_dir / f"{listing_id}.csv"
        if reviews_mode == "skipifnonew" and per_app_csv.exists():
            log.info(
                "[REVIEWS] [%d] %s already has per-app file (%s); skipping due to reviewsMode=skipIfNoNew.",
                row_idx,
                listing_id,
                per_app_csv,
            )
            continue

        log.info(
            "[REVIEWS] === [%d] %s (listing_id=%s) ===",
            row_idx,
            app_name or "<no name>",
            listing_id,
        )

        app_reviews: List[Dict[str, Any]] = []
        page_number = 1
        total_for_app = 0

        while True:
            if max_reviews_per_app and total_for_app >= max_reviews_per_app:
                log.info(
                    "[REVIEWS] Reached maxReviewsPerApp (%d) for %s",
                    max_reviews_per_app,
                    listing_id,
                )
                break

            remaining = (
                max_reviews_per_app - total_for_app
                if max_reviews_per_app
                else page_len_default
            )
            page_len = (
                min(page_len_default, remaining) if remaining > 0 else page_len_default
            )

            url = _build_reviews_url(listing_id, page_len, page_number)
            log.info(
                "[REVIEWS] Requesting page %d for %s (pageLength=%d)",
                page_number,
                listing_id,
                page_len,
            )

            payload = _http_get_json(url)
            if not payload:
                log.warning(
                    "[REVIEWS] Empty/invalid response for %s page %d; stopping.",
                    listing_id,
                    page_number,
                )
                break

            raw_reviews = _extract_reviews_from_payload(payload)

            if not raw_reviews:
                # Save payload so we can inspect shape if needed
                debug_file = debug_dir / f"{listing_id}_page{page_number}.json"
                try:
                    with debug_file.open("w", encoding="utf-8") as f:
                        json.dump(payload, f, ensure_ascii=False, indent=2)
                    log.warning(
                        "[REVIEWS] No reviews found in payload for %s page %d. "
                        "Saved JSON to %s",
                        listing_id,
                        page_number,
                        debug_file,
                    )
                except Exception as e:
                    log.warning(
                        "[REVIEWS] Failed to write debug payload for %s: %s",
                        listing_id,
                        e,
                    )
                break

            for idx_r, raw in enumerate(raw_reviews, start=1):
                if max_reviews_per_app and total_for_app >= max_reviews_per_app:
                    break

                if idx_r == 1:
                    # Log keys of first review on this app for debugging
                    log.info(
                        "[REVIEWS] First review keys for %s: %s",
                        listing_id,
                        list(raw.keys()),
                    )

                rec = _map_review_record(raw, listing_id, app_name, app_url)

                app_reviews.append(rec)
                all_reviews.append(rec)

                # ✅ Push to Apify default dataset (this is what makes Dataset non-empty on Apify)
                # await Actor.push_data(rec)  # disabled: keep dataset stable for Store QA

                # await Actor.push_data(rec)

                total_for_app += 1

            if len(raw_reviews) < page_len:
                log.info(
                    "[REVIEWS] Page %d for %s returned %d (<%d); last page.",
                    page_number,
                    listing_id,
                    len(raw_reviews),
                    page_len,
                )
                break

            page_number += 1

        log.info(
            "[REVIEWS] Extracted %d reviews for %s (%s)",
            len(app_reviews),
            listing_id,
            app_name or "<no name>",
        )

        if app_reviews:
            app_df = pd.DataFrame(app_reviews)
            app_df.to_csv(per_app_csv, index=False, encoding="utf-8-sig")
            log.info("[REVIEWS] Per-app CSV written: %s", per_app_csv)
        else:
            log.warning(
                "[REVIEWS] No reviews collected for %s; no per-app file.",
                listing_id,
            )

    if not all_reviews:
        log.warning("[REVIEWS] No reviews extracted for any app.")
        return

    df_reviews = pd.DataFrame(all_reviews)
    csv_path = project_root_path / "REVIEWS.csv"
    xlsx_path = project_root_path / "REVIEWS.xlsx"

    df_reviews.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df_reviews.to_excel(xlsx_path, index=False)

    # ✅ Store combined outputs in Apify Key-Value Store for easy download
    await _store_file_to_kv("REVIEWS.csv", csv_path, "text/csv")
    await _store_file_to_kv(
        "REVIEWS.xlsx",
        xlsx_path,
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    log.info("[REVIEWS] Exported %d reviews total", len(df_reviews))
    log.info("[REVIEWS] CSV  : %s", csv_path)
    log.info("[REVIEWS] XLSX : %s", xlsx_path)
    log.info("[REVIEWS] Per-app dir: %s", per_app_dir)
    log.info("[REVIEWS] Debug payloads dir: %s", debug_dir)



