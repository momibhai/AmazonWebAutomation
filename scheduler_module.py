"""
Module 3: Advanced Scheduler with CRUD & Detached Background Processing

Features:
- CRUD operations for Schedules (stored in schedules.json)
- Detached background thread: survives tab closes.
- Connects to Google Sheets independently.
- Updates schedules.json with real-time progress (success/failed counts).
- Logs are stored per-schedule.
"""

import concurrent.futures
import threading
import logging
import time
import json
import os
import uuid
from datetime import datetime, date
import pandas as pd
import pytz

import AmazonStoreScraper
import WebhookHandler
import GoogleSheetHandler

PKT = pytz.timezone("Asia/Karachi")
SCHEDULES_FILE = "schedules.json"

# ─────────────────────────────────────────────────────────────────────────────
#  CRUD Operations for Schedules
# ─────────────────────────────────────────────────────────────────────────────

def load_schedules() -> list:
    if os.path.exists(SCHEDULES_FILE):
        try:
            with open(SCHEDULES_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return []
    return []

def save_schedules(schedules: list):
    try:
        with open(SCHEDULES_FILE, "w") as f:
            json.dump(schedules, f, indent=2, default=str)
    except Exception as e:
        logging.error(f"[Scheduler] Could not save schedules: {e}")

def add_schedule(target_date: str, target_time: str, start_row: int, daily_limit: int, concurrency: int, webhooks: list, repeat_daily: bool = False):
    schedules = load_schedules()
    new_id = str(uuid.uuid4())[:8]
    schedule = {
        "id": new_id,
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
    schedules.append(schedule)
    save_schedules(schedules)
    return new_id

def delete_schedule(sched_id: str):
    schedules = load_schedules()
    schedules = [s for s in schedules if s.get("id") != sched_id]
    save_schedules(schedules)

def update_schedule(sched_id: str, updates: dict):
    schedules = load_schedules()
    for s in schedules:
        if s.get("id") == sched_id:
            s.update(updates)
            break
    save_schedules(schedules)

def add_log_to_schedule(sched_id: str, level: str, message: str, store_url: str = None, audit_url: str = None):
    schedules = load_schedules()
    ts = datetime.now(PKT).strftime("%I:%M:%S %p")
    for s in schedules:
        if s.get("id") == sched_id:
            if "logs" not in s:
                s["logs"] = []
            s["logs"].append({
                "time": ts,
                "level": level,
                "message": message,
                "store_url": store_url,
                "audit_url": audit_url
            })
            # Keep only last 100 logs to prevent file bloat
            s["logs"] = s["logs"][-100:]
            break
    save_schedules(schedules)

# ─────────────────────────────────────────────────────────────────────────────
#  Worker Logic (Completely independent of Streamlit UI)
# ─────────────────────────────────────────────────────────────────────────────

def _send_webhook_and_get_url(row_idx: int, webhook_url: str, my_asin: str, comp1: str, comp2: str, sheet) -> str | None:
    max_retries = 2
    for attempt in range(max_retries):
        try:
            success, response_data = WebhookHandler.send_audit_data(webhook_url, my_asin, comp1, comp2)
            if success and response_data:
                sheet_url = None
                if isinstance(response_data, dict):
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
                    return sheet_url
        except Exception:
            time.sleep(5)
    return None

def _scrape_row(row_idx: int, df_idx: int, df: pd.DataFrame, webhook_url: str, sheet, webhook_executor: concurrent.futures.ThreadPoolExecutor):
    store_url = str(df.iloc[df_idx].get("Store URL", "")).strip()
    if not store_url or store_url.lower() == "nan":
        return store_url, None, False

    driver = None
    for attempt in range(2):
        try:
            driver = AmazonStoreScraper.setup_driver(headless=True)
            AmazonStoreScraper.set_delivery_location(driver, "10001")
            my_asin, title, keyword, comp1, comp2 = AmazonStoreScraper.process_store(driver, store_url)

            if my_asin and comp1 and comp2:
                future = webhook_executor.submit(_send_webhook_and_get_url, row_idx, webhook_url, my_asin, comp1, comp2, sheet)
                return store_url, future, True
        except Exception:
            time.sleep(3)
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

    return store_url, None, False

def run_job(sched_id: str):
    """Executes a single scheduled job from start to finish."""
    schedules = load_schedules()
    job = next((s for s in schedules if s.get("id") == sched_id), None)
    if not job:
        return

    update_schedule(sched_id, {"status": "Running", "start_time": datetime.now(PKT).strftime("%I:%M %p")})
    add_log_to_schedule(sched_id, "info", f"🚀 Starting schedule {sched_id}")

    try:
        # 1. Connect to Google Sheets independently
        with open("config.json", "r") as f:
            config = json.load(f)
        sheet_name = config.get("sheet_name", "Our Listings")
        sheet_obj = GoogleSheetHandler.connect_to_sheet("credentials.json", sheet_name, worksheet_name=sheet_name)
        df = GoogleSheetHandler.get_sheet_data(sheet_obj)
        
        start_row = job["start_row"]
        daily_limit = job["daily_limit"]
        concurrency = job["concurrency"]
        webhooks = job["webhooks"]
        
        current_row = start_row
        successful = 0
        failed = 0
        total_df_rows = len(df)
        webhook_idx = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as webhook_executor:
            while successful < daily_limit:
                # Check if user stopped it from UI
                curr_job = next((s for s in load_schedules() if s.get("id") == sched_id), None)
                if not curr_job or curr_job.get("status") == "Stopped":
                    add_log_to_schedule(sched_id, "warning", "🛑 Stopped by user.")
                    break
                
                batch = []
                tmp_row = current_row
                while len(batch) < concurrency:
                    df_idx = tmp_row - 2
                    if df_idx < 0 or df_idx >= total_df_rows:
                        break
                    url_val = df.iloc[df_idx].get("Store URL", "")
                    if url_val and str(url_val).strip() not in ("", "nan"):
                        batch.append((tmp_row, df_idx, webhooks[webhook_idx % len(webhooks)]))
                        webhook_idx += 1
                    tmp_row += 1
                
                current_row = tmp_row
                if not batch:
                    add_log_to_schedule(sched_id, "warning", "No more rows found in sheet.")
                    break
                
                scrape_futures = {}
                with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as scrape_executor:
                    for r_idx, d_idx, w_url in batch:
                        f = scrape_executor.submit(_scrape_row, r_idx, d_idx, df, w_url, sheet_obj, webhook_executor)
                        scrape_futures[f] = r_idx
                    
                    for f in concurrent.futures.as_completed(scrape_futures):
                        r_idx = scrape_futures[f]
                        try:
                            store_url, audit_future, scraped_ok = f.result()
                            if scraped_ok and audit_future is not None:
                                audit_url = audit_future.result(timeout=180)
                                if audit_url:
                                    successful += 1
                                    update_schedule(sched_id, {"progress_success": successful})
                                    add_log_to_schedule(sched_id, "success", f"✅ Row {r_idx} successful.", store_url, audit_url)
                                else:
                                    failed += 1
                                    update_schedule(sched_id, {"progress_failed": failed})
                                    add_log_to_schedule(sched_id, "error", f"❌ Row {r_idx} Webhook failed.", store_url)
                            else:
                                failed += 1
                                update_schedule(sched_id, {"progress_failed": failed})
                                add_log_to_schedule(sched_id, "error", f"❌ Row {r_idx} Scrape failed.", store_url)
                        except Exception as e:
                            failed += 1
                            update_schedule(sched_id, {"progress_failed": failed})
                            add_log_to_schedule(sched_id, "error", f"❌ Row {r_idx} Error: {e}")
                            
                        if successful >= daily_limit:
                            break
                            
        # Finished
        final_job = next((s for s in load_schedules() if s.get("id") == sched_id), None)
        if final_job and final_job.get("status") != "Stopped":
            update_schedule(sched_id, {"status": "Completed", "end_time": datetime.now(PKT).strftime("%I:%M %p")})
            add_log_to_schedule(sched_id, "info", f"🎯 Schedule Completed! Success: {successful}, Failed: {failed}")
        else:
            update_schedule(sched_id, {"end_time": datetime.now(PKT).strftime("%I:%M %p")})
            
    except Exception as e:
        update_schedule(sched_id, {"status": "Failed", "end_time": datetime.now(PKT).strftime("%I:%M %p")})
        add_log_to_schedule(sched_id, "error", f"💥 Critical error connecting to sheet or running job: {e}")

# ─────────────────────────────────────────────────────────────────────────────
#  Background Daemon
# ─────────────────────────────────────────────────────────────────────────────

def _background_scheduler_loop():
    while True:
        try:
            schedules = load_schedules()
            now = datetime.now(PKT)
            current_date_str = now.strftime("%Y-%m-%d")
            current_time_str = now.strftime("%H:%M")
            
            for sched in schedules:
                if sched.get("status") == "Pending":
                    # Check if date and time have passed
                    t_date = sched.get("target_date")
                    t_time = sched.get("target_time")
                    
                    if current_date_str > t_date or (current_date_str == t_date and current_time_str >= t_time):
                        # Start job in a detached thread so loop can continue monitoring others
                        threading.Thread(target=run_job, args=(sched["id"],), daemon=True).start()
                        
        except Exception as e:
            logging.error(f"Scheduler daemon error: {e}")
            
        time.sleep(30)

_daemon_started = False
def start_daemon_if_needed():
    global _daemon_started
    if not _daemon_started:
        threading.Thread(target=_background_scheduler_loop, daemon=True).start()
        _daemon_started = True
