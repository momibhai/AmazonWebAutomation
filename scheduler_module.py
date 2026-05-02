import os
import json
import time
import threading
import logging
import pandas as pd
import concurrent.futures
from datetime import datetime
import pytz

import AmazonStoreScraper
import WebhookHandler
import GoogleSheetHandler

SCHEDULES_FILE = "schedules.json"
PKT = pytz.timezone('Asia/Karachi')

# ─────────────────────────────────────────────────────────────────────────────
#  Persistence Helpers
# ─────────────────────────────────────────────────────────────────────────────

def load_schedules():
    if not os.path.exists(SCHEDULES_FILE):
        return []
    try:
        with open(SCHEDULES_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return []

def save_schedules(schedules):
    try:
        with open(SCHEDULES_FILE, "w") as f:
            json.dump(schedules, f, indent=4)
    except Exception as e:
        logging.error(f"[Scheduler] Failed to save schedules: {e}")

def add_schedule(target_date, target_time, start_row, daily_limit, concurrency, webhooks, repeat_daily=False):
    import uuid
    schedules = load_schedules()
    job = {
        "id": str(uuid.uuid4())[:8],
        "created_at": datetime.now(PKT).isoformat(),
        "target_date": target_date,
        "target_time": target_time,
        "start_row": start_row,
        "daily_limit": daily_limit,
        "concurrency": concurrency,
        "webhooks": webhooks,
        "repeat_daily": repeat_daily,
        "status": "Pending",
        "progress_success": 0,
        "progress_failed": 0,
        "start_time": None,
        "end_time": None,
        "next_run": None,
        "logs": []
    }
    schedules.append(job)
    save_schedules(schedules)
    return job

def update_schedule(sched_id, updates):
    schedules = load_schedules()
    for s in schedules:
        if s["id"] == sched_id:
            s.update(updates)
            break
    save_schedules(schedules)

def delete_schedule(sched_id):
    schedules = [s for s in load_schedules() if s["id"] != sched_id]
    save_schedules(schedules)

def _append_log(sched_id, level, message, store_url=None, audit_url=None):
    """Thread-safe log append with atomic read-modify-write."""
    schedules = load_schedules()
    for s in schedules:
        if s["id"] == sched_id:
            s["logs"].append({
                "time": datetime.now(PKT).strftime("%I:%M:%S %p"),
                "level": level,
                "message": message,
                "store_url": store_url,
                "audit_url": audit_url
            })
            s["logs"] = s["logs"][-150:]
            break
    save_schedules(schedules)

def _increment_counter(sched_id, field):
    """Thread-safe increment of progress_success or progress_failed."""
    schedules = load_schedules()
    for s in schedules:
        if s["id"] == sched_id:
            s[field] = s.get(field, 0) + 1
            break
    save_schedules(schedules)

# ─────────────────────────────────────────────────────────────────────────────
#  Webhook Worker  (identical logic to vps_automation.send_webhook_and_update_sheet)
#  Runs in background thread — DOES NOT BLOCK scraping
# ─────────────────────────────────────────────────────────────────────────────

def _webhook_worker(row_idx, webhook_url, my_asin, comp1, comp2, sheet, sched_id, store_url):
    """Fire-and-forget webhook + sheet update. Same logic as Module 2."""
    max_retries = 2
    for attempt in range(max_retries):
        try:
            success, response_data = WebhookHandler.send_audit_data(webhook_url, my_asin, comp1, comp2)
            if success and response_data:
                sheet_url = None
                if isinstance(response_data, dict):
                    for k1, v1 in response_data.items():
                        if "docs" in str(k1).lower() and isinstance(v1, dict):
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
                    _increment_counter(sched_id, "progress_success")
                    _append_log(sched_id, "success",
                                f"✅ Row {row_idx} done. Audit URL: {sheet_url}",
                                store_url, sheet_url)
                    logging.info(f"[Scheduler] Row {row_idx} SUCCESS — {sheet_url}")
                    return  # done
                else:
                    raise Exception("Webhook OK but no sheet_url in response")
            else:
                raise Exception("Webhook failed or bad response")
        except Exception as e:
            logging.warning(f"[Scheduler Webhook] Retry {attempt+1}/{max_retries} for Row {row_idx}: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)

    # All retries exhausted
    _increment_counter(sched_id, "progress_failed")
    _append_log(sched_id, "error", f"❌ Row {row_idx} Webhook FAILED after retries.", store_url)

# ─────────────────────────────────────────────────────────────────────────────
#  Scrape Worker  (identical logic to vps_automation.process_single_row)
#  Frees up immediately after scraping — webhook dispatched to background pool
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_worker(row_idx, df_idx, df, webhook_url, sheet, webhook_executor, sched_id):
    """
    Scrapes one row, then immediately dispatches webhook to background.
    Returns quickly so scrape_executor can start the next row.
    """
    store_url = str(df.iloc[df_idx].get("Store URL", "")).strip()
    if not store_url or store_url.lower() in ("nan", ""):
        return f"Row {row_idx}: Skipped (No URL)"

    logging.info(f"[Scheduler] Processing Row {row_idx}: {store_url}")

    max_retries = 2
    for attempt in range(max_retries):
        driver = None
        try:
            driver = AmazonStoreScraper.setup_driver(headless=True)
            if not AmazonStoreScraper.set_delivery_location(driver, "10001"):
                logging.warning(f"[Scheduler] Row {row_idx}: Location set failed, continuing anyway...")

            my_asin, title, keyword, comp1, comp2 = AmazonStoreScraper.process_store(driver, store_url)

            if my_asin:
                if comp1 and comp2:
                    # Dispatch to background — DO NOT WAIT
                    webhook_executor.submit(_webhook_worker,
                                           row_idx, webhook_url, my_asin,
                                           comp1, comp2, sheet, sched_id, store_url)
                    return f"Row {row_idx}: Scraped OK — webhook dispatched."
                else:
                    _increment_counter(sched_id, "progress_failed")
                    _append_log(sched_id, "warning",
                                f"⚠️ Row {row_idx} skipped (no competitors found).", store_url)
                    return f"Row {row_idx}: Skipped (Missing Competitors)"
            else:
                raise Exception("ASIN not found")

        except Exception as e:
            logging.warning(f"[Scheduler Scrape] Retry {attempt+1}/{max_retries} Row {row_idx}: {e}")
            if attempt < max_retries - 1:
                time.sleep(3)
            else:
                _increment_counter(sched_id, "progress_failed")
                _append_log(sched_id, "error",
                            f"❌ Row {row_idx} scrape FAILED: {e}", store_url)
                return f"Row {row_idx}: ERROR — {e}"
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

    return f"Row {row_idx}: ERROR"

# ─────────────────────────────────────────────────────────────────────────────
#  Main Job Runner  (mirrors vps_automation.run_vps_batch exactly)
# ─────────────────────────────────────────────────────────────────────────────

def run_job(sched_id: str):
    """Execute one scheduled job. Runs in its own daemon thread."""
    schedules = load_schedules()
    job = next((s for s in schedules if s.get("id") == sched_id), None)
    if not job:
        return

    update_schedule(sched_id, {
        "status": "Running",
        "start_time": datetime.now(PKT).strftime("%I:%M %p")
    })
    _append_log(sched_id, "info", f"🚀 Starting schedule {sched_id}")

    try:
        # Connect to sheet independently (no Streamlit session needed)
        with open("config.json", "r") as f:
            config = json.load(f)
        sheet_name = config.get("sheet_name", "Our Listings")
        sheet_obj = GoogleSheetHandler.connect_to_sheet(
            "credentials.json", sheet_name, worksheet_name=sheet_name
        )
        df = GoogleSheetHandler.get_sheet_data(sheet_obj)

        start_row   = job["start_row"]
        daily_limit = job["daily_limit"]
        concurrency = job["concurrency"]
        webhooks    = job["webhooks"]
        total_rows  = len(df)
        repeat_daily = job.get("repeat_daily", False)

        # Build row list — exactly daily_limit valid rows (no over-submission)
        rows_to_process = []
        r = start_row
        while len(rows_to_process) < daily_limit:
            df_idx = r - 2
            if df_idx < 0 or df_idx >= total_rows:
                break
            url_val = str(df.iloc[df_idx].get("Store URL", "")).strip()
            if url_val and url_val.lower() not in ("nan", ""):
                rows_to_process.append((r, df_idx))
            r += 1
        next_start_row = r  # first unprocessed row for next day

        if not rows_to_process:
            update_schedule(sched_id, {"status": "Failed"})
            _append_log(sched_id, "error", "No valid rows found in sheet.")
            return

        _append_log(sched_id, "info",
                    f"Found {len(rows_to_process)} rows to process (limit={daily_limit}, concurrency={concurrency})")

        completed = 0

        # ── EXACT same pattern as vps_automation.run_vps_batch ──────────────
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as webhook_executor:
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as scrape_executor:
                future_to_row = {}

                for i, (r_idx, d_idx) in enumerate(rows_to_process):
                    # Only check stop signal — NOT progress_success (webhooks fire async)
                    curr = next((s for s in load_schedules() if s["id"] == sched_id), None)
                    if not curr or curr.get("status") == "Stopped":
                        _append_log(sched_id, "warning", "🛑 Stopped by user.")
                        break

                    webhook_to_use = webhooks[i % len(webhooks)]
                    future = scrape_executor.submit(
                        _scrape_worker,
                        r_idx, d_idx, df, webhook_to_use,
                        sheet_obj, webhook_executor, sched_id
                    )
                    future_to_row[future] = r_idx

                for future in concurrent.futures.as_completed(future_to_row):
                    curr = next((s for s in load_schedules() if s["id"] == sched_id), None)
                    if not curr or curr.get("status") == "Stopped":
                        break

                    completed += 1
                    try:
                        result = future.result()
                        logging.info(f"[Scheduler] ({completed}) {result}")
                    except Exception as exc:
                        logging.error(f"[Scheduler] Row exception: {exc}")
        # webhook_executor exits here — waits for all background webhooks to finish

        final = next((s for s in load_schedules() if s["id"] == sched_id), None)
        if final and final.get("status") not in ("Stopped",):
            update_schedule(sched_id, {
                "status": "Completed",
                "end_time": datetime.now(PKT).strftime("%I:%M %p")
            })
            _append_log(sched_id, "info",
                        f"✅ Job complete. Success={final.get('progress_success',0)}, Failed={final.get('progress_failed',0)}")

            # ── Auto-schedule next day if repeat_daily is enabled ──
            if repeat_daily:
                from datetime import date, timedelta
                next_date = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
                next_time_display = datetime.strptime(job["target_time"], "%H:%M").strftime("%I:%M %p")
                new_job = add_schedule(
                    target_date  = next_date,
                    target_time  = job["target_time"],
                    start_row    = next_start_row,
                    daily_limit  = job["daily_limit"],
                    concurrency  = job["concurrency"],
                    webhooks     = job["webhooks"],
                    repeat_daily = True
                )
                # Mark current job with next run info
                update_schedule(sched_id, {
                    "next_run": f"{next_date} at {next_time_display} PKT"
                })
                _append_log(sched_id, "info",
                            f"🔁 Repeat Daily ON — Next job ({new_job['id']}) scheduled for {next_date} at {next_time_display} PKT. Next start row: {next_start_row}")
                logging.info(f"[Scheduler] Repeat Daily: created job {new_job['id']} for {next_date}")

    except Exception as e:
        logging.exception(f"[Scheduler] Critical error in job {sched_id}: {e}")
        update_schedule(sched_id, {"status": "Failed"})
        _append_log(sched_id, "error", f"💥 Critical error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
#  Background Daemon  (checks every 30s if any job is due)
# ─────────────────────────────────────────────────────────────────────────────

_daemon_running = False
_daemon_lock = threading.Lock()

def _background_scheduler_loop():
    while True:
        try:
            now = datetime.now(PKT)
            current_date = now.strftime("%Y-%m-%d")
            current_time = now.strftime("%H:%M")

            for s in load_schedules():
                if (s.get("status") == "Pending"
                        and s.get("target_date") == current_date
                        and current_time >= s.get("target_time", "99:99")):
                    logging.info(f"[Daemon] Triggering job {s['id']}")
                    threading.Thread(target=run_job, args=(s["id"],), daemon=True).start()
        except Exception as e:
            logging.error(f"[Daemon] Loop error: {e}")
        time.sleep(30)

def start_daemon_if_needed():
    global _daemon_running
    with _daemon_lock:
        if not _daemon_running:
            t = threading.Thread(target=_background_scheduler_loop, daemon=True)
            t.start()
            _daemon_running = True
            logging.info("[Daemon] Background scheduler daemon started.")
