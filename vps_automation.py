import logging
import concurrent.futures
import time
import pandas as pd
import streamlit as st
import AmazonStoreScraper
import WebhookHandler
import GoogleSheetHandler

from streamlit.runtime.scriptrunner import add_script_run_ctx

def send_webhook_and_update_sheet(row_idx, webhook_url, my_asin, comp1, comp2, sheet, log_placeholder, ctx=None):
    """Background task to wait for webhook response and update sheet."""
    if ctx:
        add_script_run_ctx(ctx=ctx)

    max_retries = 2
    for attempt in range(max_retries):
        try:
            success, response_data = WebhookHandler.send_audit_data(webhook_url, my_asin, comp1, comp2)
            if success and response_data:
                sheet_url = None
                if isinstance(response_data, dict):
                    for key1 in response_data.keys():
                        if 'docs' in key1.lower():
                            nested1 = response_data[key1]
                            if isinstance(nested1, dict):
                                for key2 in nested1.keys():
                                    nested2 = nested1[key2]
                                    if isinstance(nested2, dict):
                                        for key3 in nested2.keys():
                                            sheet_url = f"{key1}.{key2}.{key3}"
                                            break
                                    break
                            break
                    if not sheet_url:
                        sheet_url = response_data.get('sheet_url', '')
                        
                if sheet_url:
                    GoogleSheetHandler.update_audit_link(sheet, row_idx, sheet_url)
                    logging.info(f"[VPS Webhook] Row {row_idx} SUCCESS! Audit URL: {sheet_url}")
                    st.toast(f"✅ Row {row_idx} Audit Ready!")
                    return # Exit on success
                else:
                    raise Exception("Webhook success, but no Audit URL returned.")
            else:
                raise Exception("Failed to send webhook or bad response.")
        except Exception as e:
            logging.warning(f"[VPS Webhook] Retry {attempt+1}/{max_retries} for Row {row_idx}: {e}")
            if attempt == max_retries - 1:
                logging.error(f"[VPS Webhook] Final Error on Row {row_idx} after {max_retries} attempts: {e}")
            time.sleep(5)

def process_single_row(row_idx, df_idx, df, webhook_url, sheet, webhook_executor, log_placeholder, stop_event=None, ctx=None):
    """Processes a single row for VPS automation. Frees up quickly after scraping."""
    if ctx:
        add_script_run_ctx(ctx=ctx)
        
    if stop_event and stop_event.is_set():
        return f"Row {row_idx}: Stopped by user"
        
    current_url = df.iloc[df_idx]['Store URL']
    if not current_url or pd.isna(current_url):
        return f"Row {row_idx}: Skipped (No URL)"
        
    logging.info(f"[VPS] Processing Row {row_idx}: {current_url} | Webhook: {webhook_url}")
    
    max_retries = 2
    for attempt in range(max_retries):
        driver = None
        try:
            driver = AmazonStoreScraper.setup_driver(headless=True)
            
            # Set Location (non-blocking - Amazon may block on VPS IPs)
            if not AmazonStoreScraper.set_delivery_location(driver, "10001"):
                logging.warning(f"[VPS] Row {row_idx}: Location set failed, proceeding with scraping anyway...")
                
            # Scrape
            my_asin, title, keyword, comp1, comp2 = AmazonStoreScraper.process_store(driver, current_url)
            
            if my_asin:
                if comp1 and comp2:
                    # Disptach webhook task to background and immediately return
                    webhook_executor.submit(send_webhook_and_update_sheet, row_idx, webhook_url, my_asin, comp1, comp2, sheet, log_placeholder, ctx)
                    return f"Row {row_idx}: Scraped Successfully! Webhook pushed to background."
                else:
                    return f"Row {row_idx}: Skipped Webhook (Missing Competitors: {comp1}, {comp2})"
            else:
                raise Exception("Failed to scrape (ASIN not found)")
                
        except Exception as e:
            logging.warning(f"[VPS] Retry {attempt+1}/{max_retries} for Row {row_idx}: {e}")
            if attempt == max_retries - 1:
                logging.error(f"[VPS] Final Error on Row {row_idx} after {max_retries} attempts: {e}")
                return f"Row {row_idx}: ERROR - {e}"
            time.sleep(3)
        finally:
            if driver:
                driver.quit()

from streamlit.runtime.scriptrunner import get_script_run_ctx

def run_vps_batch(df, start_row, end_row, concurrency, webhooks, sheet, log_placeholder, progress_bar, stop_event=None):
    """Runs the automation in parallel using ThreadPoolExecutor."""
    rows_to_process = []
    
    for row_idx in range(start_row, end_row + 1):
        df_idx = row_idx - 2
        if 0 <= df_idx < len(df):
            rows_to_process.append((row_idx, df_idx))
            
    total = len(rows_to_process)
    if total == 0:
        log_placeholder.warning("No rows to process in the selected range.")
        return
        
    if not webhooks:
        log_placeholder.error("No Webhooks selected!")
        return
        
    log_placeholder.info(f"🚀 Starting VPS Mass Automation on {total} rows with scrape concurrency {concurrency}...")
    completed = 0
    
    ctx = get_script_run_ctx()
    
    # We use a large thread pool for webhooks so they can wait for 2 mins without blocking scraping
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as webhook_executor:
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as scrape_executor:
            future_to_row = {}
            for i, (r_idx, d_idx) in enumerate(rows_to_process):
                webhook_to_use = webhooks[i % len(webhooks)]
                future = scrape_executor.submit(process_single_row, r_idx, d_idx, df, webhook_to_use, sheet, webhook_executor, log_placeholder, stop_event, ctx)
                future_to_row[future] = r_idx
            
            for future in concurrent.futures.as_completed(future_to_row):
                if stop_event and stop_event.is_set():
                    break
                    
                row_idx = future_to_row[future]
                completed += 1
                try:
                    result = future.result()
                    if "Successfully" in result:
                        st.toast(f"✅ {result}")
                    elif "ERROR" in result or "Failed" in result:
                        st.toast(f"⚠️ {result}")
                        
                    log_placeholder.info(f"({completed}/{total}) {result}")
                except Exception as exc:
                    logging.error(f"[VPS] Row {row_idx} generated an exception: {exc}")
                    log_placeholder.error(f"Row {row_idx} exception: {exc}")
                    
                progress_bar.progress(completed / total)
                
    if stop_event and stop_event.is_set():
        log_placeholder.warning("🛑 VPS Mass Automation Stopped by User.")
    else:
        log_placeholder.success("🎉 All Scraping complete! Waiting for any remaining background webhooks to finish...")
