"""
Module 3: Scheduler - Automated Daily Audit Scheduler
Completely separate from Module 1 (Manual Audit) and Module 2 (VPS Mass Automation).

Features:
- Daily audit quantity limit (failures don't count)
- PKT timezone scheduling (run on specific days/times)
- Persistent log history (JSON file)
- Live logs with Store URL + Audit Link
- Concurrency like Module 2 (fire-and-forget webhooks)
"""

import concurrent.futures
import threading
import logging
import time
import json
import os
from datetime import datetime, date
import pandas as pd
import pytz
import AmazonStoreScraper
import WebhookHandler
import GoogleSheetHandler

# ── Timezone ─────────────────────────────────────────────────────────────────
PKT = pytz.timezone("Asia/Karachi")

# ── Persistent History File ───────────────────────────────────────────────────
HISTORY_FILE = "scheduler_history.json"
MAX_HISTORY  = 2000   # keep last 2000 log entries


# ─────────────────────────────────────────────────────────────────────────────
#  History helpers
# ─────────────────────────────────────────────────────────────────────────────

def load_history() -> list:
    """Load all past log entries from disk."""
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return []
    return []


def save_log_entry(entry: dict):
    """Append a single log entry and persist to disk (thread-safe)."""
    history = load_history()
    history.append(entry)
    history = history[-MAX_HISTORY:]          # trim old entries
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=2, default=str)
    except Exception as e:
        logging.warning(f"[Scheduler] Could not save history: {e}")


def get_today_stats() -> dict:
    """Return today's success/failure counts from history."""
    today = date.today().isoformat()
    history = load_history()
    today_entries = [e for e in history if e.get("date") == today]
    success = sum(1 for e in today_entries if e.get("status") == "success")
    failed  = sum(1 for e in today_entries if e.get("status") == "failed")
    return {"success": success, "failed": failed, "total": len(today_entries)}


# ─────────────────────────────────────────────────────────────────────────────
#  Core: webhook background task (waits for n8n response, updates sheet)
# ─────────────────────────────────────────────────────────────────────────────

def _send_webhook_and_get_url(row_idx: int, webhook_url: str,
                               my_asin: str, comp1: str, comp2: str,
                               sheet, ctx=None) -> str | None:
    """
    Blocking call that sends webhook and waits for audit URL.
    Returns audit URL string on success, None on failure.
    Runs inside a background thread.
    """
    if ctx:
        try:
            from streamlit.runtime.scriptrunner import add_script_run_ctx
            add_script_run_ctx(ctx=ctx)
        except Exception:
            pass

    max_retries = 2
    for attempt in range(max_retries):
        try:
            success, response_data = WebhookHandler.send_audit_data(
                webhook_url, my_asin, comp1, comp2
            )
            if success and response_data:
                sheet_url = None
                if isinstance(response_data, dict):
                    # Deep-search for docs key
                    for k1, v1 in response_data.items():
                        if "docs" in k1.lower() and isinstance(v1, dict):
                            for k2, v2 in v1.items():
                                if isinstance(v2, dict):
                                    for k3 in v2:
                                        sheet_url = f"{k1}.{k2}.{k3}"
                                        break
                                    break
                            break
                    if not sheet_url:
                        sheet_url = response_data.get("sheet_url", "")

                if sheet_url:
                    GoogleSheetHandler.update_audit_link(sheet, row_idx, sheet_url)
                    logging.info(f"[Scheduler] Row {row_idx} ✅ Audit URL: {sheet_url}")
                    return sheet_url
                else:
                    raise Exception("Webhook ok, but no audit URL in response.")
            else:
                raise Exception("Webhook call failed or empty response.")

        except Exception as e:
            logging.warning(f"[Scheduler] Webhook retry {attempt+1}/{max_retries} Row {row_idx}: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)

    logging.error(f"[Scheduler] Row {row_idx} ❌ All webhook retries exhausted.")
    return None


# ─────────────────────────────────────────────────────────────────────────────
#  Core: scrape a single row
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_row(row_idx: int, df_idx: int, df: pd.DataFrame,
                webhook_url: str, sheet,
                webhook_executor: concurrent.futures.ThreadPoolExecutor,
                ctx=None):
    """
    Scrapes one row headlessly and fires webhook in background.
    Returns (store_url, audit_url_future, success_bool).
    The audit_url_future is a Future[str|None] — caller can .result() it.
    """
    if ctx:
        try:
            from streamlit.runtime.scriptrunner import add_script_run_ctx
            add_script_run_ctx(ctx=ctx)
        except Exception:
            pass

    store_url = str(df.iloc[df_idx].get("Store URL", "")).strip()
    if not store_url or store_url.lower() == "nan":
        return store_url, None, False

    driver = None
    max_scrape_retries = 2
    for attempt in range(max_scrape_retries):
        try:
            driver = AmazonStoreScraper.setup_driver(headless=True)
            AmazonStoreScraper.set_delivery_location(driver, "10001")

            my_asin, title, keyword, comp1, comp2 = AmazonStoreScraper.process_store(
                driver, store_url
            )

            if my_asin and comp1 and comp2:
                # Fire webhook in background; don't block the scrape thread
                future = webhook_executor.submit(
                    _send_webhook_and_get_url,
                    row_idx, webhook_url, my_asin, comp1, comp2, sheet, ctx
                )
                return store_url, future, True
            else:
                raise Exception(f"Scrape incomplete: ASIN={my_asin} C1={comp1} C2={comp2}")

        except Exception as e:
            logging.warning(f"[Scheduler] Scrape retry {attempt+1}/{max_scrape_retries} Row {row_idx}: {e}")
            if attempt < max_scrape_retries - 1:
                time.sleep(3)
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = None
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

    return store_url, None, False


# ─────────────────────────────────────────────────────────────────────────────
#  Public: run_daily_batch
# ─────────────────────────────────────────────────────────────────────────────

def run_daily_batch(df: pd.DataFrame,
                    start_row: int,
                    daily_limit: int,
                    concurrency: int,
                    webhooks: list,
                    sheet,
                    log_callback=None,
                    stop_event: threading.Event = None,
                    ctx=None) -> int:
    """
    Runs automation until `daily_limit` SUCCESSFUL audits are created.
    Failures are logged but do NOT count toward the limit.

    Parameters:
        df           – DataFrame from Google Sheet
        start_row    – Sheet row number (2-indexed) to start from
        daily_limit  – Target number of successful audits for today
        concurrency  – Number of parallel scrape threads
        webhooks     – List of webhook URLs (round-robin)
        sheet        – gspread Sheet object
        log_callback – fn(level, message, store_url, audit_url) for UI updates
        stop_event   – threading.Event to gracefully stop
        ctx          – Streamlit script run context (for background threads)

    Returns:
        Number of successful audits created in this run.
    """
    if not webhooks:
        if log_callback:
            log_callback("error", "No webhooks selected!", None, None)
        return 0

    today_str    = date.today().isoformat()
    successful   = 0
    current_row  = start_row          # sheet row (2-indexed)
    webhook_idx  = 0                  # round-robin pointer
    total_df_rows = len(df)

    if log_callback:
        log_callback("info", f"🚀 Starting daily batch | Target: {daily_limit} audits | Concurrency: {concurrency}", None, None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as webhook_executor:
        while successful < daily_limit:
            if stop_event and stop_event.is_set():
                if log_callback:
                    log_callback("warning", "🛑 Stopped by user.", None, None)
                break

            # ── Build a batch of rows (up to `concurrency` rows) ──────────
            batch: list[tuple] = []
            tmp_row = current_row
            while len(batch) < concurrency:
                df_idx = tmp_row - 2
                if df_idx < 0 or df_idx >= total_df_rows:
                    break
                url_val = df.iloc[df_idx].get("Store URL", "")
                if url_val and str(url_val).strip() not in ("", "nan"):
                    wh_url = webhooks[webhook_idx % len(webhooks)]
                    batch.append((tmp_row, df_idx, wh_url))
                    webhook_idx += 1
                tmp_row += 1

            current_row = tmp_row      # advance pointer for next batch

            if not batch:
                if log_callback:
                    log_callback("warning", "⚠️ No more valid rows to process.", None, None)
                break

            # ── Scrape batch in parallel ───────────────────────────────────
            scrape_futures: dict[concurrent.futures.Future, tuple] = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as scrape_executor:
                for r_idx, d_idx, w_url in batch:
                    f = scrape_executor.submit(
                        _scrape_row, r_idx, d_idx, df, w_url, sheet, webhook_executor, ctx
                    )
                    scrape_futures[f] = (r_idx, w_url)

                for f in concurrent.futures.as_completed(scrape_futures):
                    r_idx, _ = scrape_futures[f]
                    try:
                        store_url, audit_future, scraped_ok = f.result()

                        if scraped_ok and audit_future is not None:
                            # Wait up to 3 minutes for webhook to return audit URL
                            try:
                                audit_url = audit_future.result(timeout=180)
                            except concurrent.futures.TimeoutError:
                                audit_url = None

                            if audit_url:
                                successful += 1
                                entry = {
                                    "date": today_str,
                                    "time": datetime.now(PKT).strftime("%I:%M %p"),
                                    "row": r_idx,
                                    "store_url": store_url,
                                    "audit_url": audit_url,
                                    "status": "success",
                                }
                                save_log_entry(entry)
                                if log_callback:
                                    log_callback(
                                        "success",
                                        f"✅ Row {r_idx} | {store_url}",
                                        store_url, audit_url
                                    )
                            else:
                                entry = {
                                    "date": today_str,
                                    "time": datetime.now(PKT).strftime("%I:%M %p"),
                                    "row": r_idx,
                                    "store_url": store_url,
                                    "audit_url": None,
                                    "status": "failed",
                                }
                                save_log_entry(entry)
                                if log_callback:
                                    log_callback(
                                        "error",
                                        f"❌ Row {r_idx} | Webhook failed | {store_url}",
                                        store_url, None
                                    )
                        else:
                            entry = {
                                "date": today_str,
                                "time": datetime.now(PKT).strftime("%I:%M %p"),
                                "row": r_idx,
                                "store_url": store_url,
                                "audit_url": None,
                                "status": "failed",
                            }
                            save_log_entry(entry)
                            if log_callback:
                                log_callback(
                                    "error",
                                    f"❌ Row {r_idx} | Scrape failed | {store_url}",
                                    store_url, None
                                )

                    except Exception as ex:
                        logging.error(f"[Scheduler] Unexpected error row {r_idx}: {ex}")
                        if log_callback:
                            log_callback("error", f"❌ Row {r_idx} | Unexpected error: {ex}", None, None)

                    if successful >= daily_limit:
                        if log_callback:
                            log_callback(
                                "success",
                                f"🎯 Daily limit of {daily_limit} reached! Stopping batch.",
                                None, None
                            )
                        break   # stop processing futures in this batch

    if log_callback:
        log_callback(
            "info",
            f"📊 Batch done | ✅ {successful} successful | Target was {daily_limit}",
            None, None
        )
    return successful


# ─────────────────────────────────────────────────────────────────────────────
#  Scheduler: background thread that watches PKT time
# ─────────────────────────────────────────────────────────────────────────────

class DailyScheduler:
    """
    Runs run_daily_batch at a specified PKT time on specified days of the week.
    Stops after the daily limit is met and waits for the next scheduled day.
    """

    DAY_MAP = {
        "Monday":    0,
        "Tuesday":   1,
        "Wednesday": 2,
        "Thursday":  3,
        "Friday":    4,
        "Saturday":  5,
        "Sunday":    6,
    }

    def __init__(self):
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._status = "Idle"
        self._last_run_date = None

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def status(self) -> str:
        return self._status

    def start(self, df, start_row, daily_limit, concurrency, webhooks, sheet,
              days: list, run_time_str: str, log_callback=None, ctx=None):
        """
        Start the background scheduler.
        run_time_str: "HH:MM" in 24-hour format (we convert PKT display to this internally)
        days: list of day names e.g. ["Monday", "Wednesday", "Friday"]
        """
        if self.is_running:
            return

        self._stop_event.clear()
        self._status = "Running"

        def _worker():
            while not self._stop_event.is_set():
                now = datetime.now(PKT)
                today_name = now.strftime("%A")
                today_date = now.date()

                # Check if today is a scheduled day
                if today_name in days:
                    # Parse scheduled time
                    try:
                        sched_h, sched_m = map(int, run_time_str.split(":"))
                    except Exception:
                        sched_h, sched_m = 0, 0

                    sched_time = now.replace(hour=sched_h, minute=sched_m, second=0, microsecond=0)

                    # If we haven't run today and it's past the scheduled time
                    if self._last_run_date != today_date and now >= sched_time:
                        self._last_run_date = today_date
                        self._status = f"Running batch ({today_name} {now.strftime('%I:%M %p')} PKT)"
                        if log_callback:
                            log_callback(
                                "info",
                                f"⏰ Scheduler triggered | {today_name} {now.strftime('%I:%M %p')} PKT",
                                None, None
                            )
                        run_daily_batch(
                            df=df,
                            start_row=start_row,
                            daily_limit=daily_limit,
                            concurrency=concurrency,
                            webhooks=webhooks,
                            sheet=sheet,
                            log_callback=log_callback,
                            stop_event=self._stop_event,
                            ctx=ctx,
                        )
                        self._status = f"Waiting for next scheduled day..."

                # Sleep 30 seconds between checks
                self._stop_event.wait(30)

            self._status = "Stopped"

        self._thread = threading.Thread(target=_worker, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        self._status = "Stopping..."


# ─────────────────────────────────────────────────────────────────────────────
#  Singleton scheduler instance (persists across Streamlit reruns)
# ─────────────────────────────────────────────────────────────────────────────
_scheduler_instance: DailyScheduler | None = None

def get_scheduler() -> DailyScheduler:
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = DailyScheduler()
    return _scheduler_instance
