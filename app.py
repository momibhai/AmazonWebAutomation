import streamlit as st
import pandas as pd
import time
import logging
import os
import json
import AmazonStoreScraper
import GoogleSheetHandler
import WebhookHandler

# Configure Logging
LOG_FILE = "scraper.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s', force=True)

# Load config
def load_config():
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except:
        return {
            "sheet_name": "Our Listings",
            "store_url_column": "Store URL",
            "audit_links_column": "Audit Links"
        }

config = load_config()

st.set_page_config(page_title="Amazesst - Amazon Audit Automation", layout="wide", page_icon="🚀")

# Custom CSS - Amazesst Theme (Light Blue + Green)
st.markdown("""
<style>
    /* Import Google Font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
    
    /* Global Styles */
    * {
        font-family: 'Inter', sans-serif !important;
    }
    
    /* Main Background */
    .stApp {
        background: linear-gradient(135deg, #e0f7fa 0%, #f1f8e9 100%);
    }
    
    /* Header Styling */
    h1 {
        background: linear-gradient(90deg, #00bcd4 0%, #4caf50 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 700 !important;
        font-size: 2.5rem !important;
        letter-spacing: -0.5px;
    }
    
    h2, h3 {
        color: #00796b !important;
        font-weight: 600 !important;
    }
    
    /* Sidebar Styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #b2ebf2 0%, #c8e6c9 100%);
        border-right: 2px solid #4caf50;
    }
    
    [data-testid="stSidebar"] h2 {
        color: #00695c !important;
    }
    
    /* Button Styling */
    .stButton > button {
        background: linear-gradient(90deg, #00bcd4 0%, #4caf50 100%) !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 0.75rem 2rem !important;
        font-weight: 600 !important;
        box-shadow: 0 4px 15px rgba(0, 188, 212, 0.3) !important;
        transition: all 0.3s ease !important;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 20px rgba(0, 188, 212, 0.4) !important;
    }
    
    /* Input Fields */
    .stTextInput > div > div > input {
        border-radius: 8px !important;
        border: 2px solid #b2ebf2 !important;
        padding: 0.75rem !important;
    }
    
    .stTextInput > div > div > input:focus {
        border-color: #00bcd4 !important;
        box-shadow: 0 0 0 2px rgba(0, 188, 212, 0.2) !important;
    }
    
    /* Success/Error Messages */
    .stSuccess {
        background-color: #c8e6c9 !important;
        border-left: 4px solid #4caf50 !important;
        border-radius: 8px !important;
    }
    
    .stError {
        background-color: #ffcdd2 !important;
        border-left: 4px solid #f44336 !important;
        border-radius: 8px !important;
    }
    
    /* Progress Bar */
    .stProgress > div > div > div {
        background: linear-gradient(90deg, #00bcd4 0%, #4caf50 100%) !important;
    }
    
    /* Code Blocks */
    .stCodeBlock {
        background-color: #e0f2f1 !important;
        border-radius: 8px !important;
        border: 1px solid #b2dfdb !important;
    }
    
    /* Dataframe */
    .stDataFrame {
        border-radius: 12px !important;
        overflow: hidden !important;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1) !important;
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        background-color: #b2ebf2 !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
    }
    
    /* Fix dropdown text overflow */
    .stTextInput > div > div > input {
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
    }
    
    /* Number input styling */
    .stNumberInput > div > div > input {
        border-radius: 8px !important;
        border: 2px solid #b2ebf2 !important;
        padding: 0.75rem !important;
    }
</style>
""", unsafe_allow_html=True)

# Header with Branding
st.markdown("""
<div style='text-align: center; padding: 1rem 0 2rem 0;'>
    <h1 style='font-size: 3rem; margin-bottom: 0.5rem;'>🚀 AMAZESST</h1>
    <p style='color: #00796b; font-size: 1.2rem; font-weight: 500;'>Smart, Scalable, Data-Driven Amazon Growth</p>
    <p style='color: #00897b; font-size: 0.9rem;'>Amazon Store Audit Automation System</p>
</div>
""", unsafe_allow_html=True)

import vps_automation
import threading

# --- Sidebar Configuration ---
st.sidebar.header("⚙️ Configuration")
mode = st.sidebar.radio("Module Selection", ["Manual Audit", "VPS Mass Automation (24/7)"])
st.sidebar.markdown("---")

creds_file = st.sidebar.text_input("Credentials File Path", "credentials.json")
sheet_name = st.sidebar.text_input("Google Sheet Name", config.get("sheet_name", "Our Listings"))

st.sidebar.markdown("### 🔗 Webhook Configuration")
workflow_urls = [
    "https://n8n.srv1186513.hstgr.cloud/webhook/workflow1",
    "https://n8n.srv1186513.hstgr.cloud/webhook/workflow2",
    "https://n8n.srv1186513.hstgr.cloud/webhook/workflow3",
    "https://n8n.srv1186513.hstgr.cloud/webhook/workflow4",
    "https://n8n.srv1186513.hstgr.cloud/webhook/workflow5"
]

is_vps_mode = (mode == "VPS Mass Automation (24/7)")

selected_webhooks = []
for i, url in enumerate(workflow_urls, start=1):
    # Default: Manual selects only WF1, VPS selects all
    default_val = True if is_vps_mode else (i == 1)
    if st.sidebar.checkbox(f"Use Workflow {i}", value=default_val):
        selected_webhooks.append(url)

# --- Connect to Sheet ---
if st.button("🔌 Connect to Google Sheet"):
    if not os.path.exists(creds_file):
        st.error(f"Credentials file not found: {creds_file}")
    else:
        try:
            # Pass sheet_name as both file name and worksheet name
            sheet = GoogleSheetHandler.connect_to_sheet(creds_file, sheet_name, worksheet_name=sheet_name)
            if sheet:
                st.session_state['sheet'] = sheet
                st.success(f"Connected to '{sheet_name}'")
                
                # Load Data
                df = GoogleSheetHandler.get_sheet_data(sheet)
                st.session_state['df'] = df
                st.write("### Data Preview")
                st.dataframe(df.head())
            else:
                st.error("Failed to connect. Check logs.")
        except Exception as e:
            st.error(f"Connection Error: {e}")

# --- Automation Section ---
if 'df' in st.session_state:
    df = st.session_state['df']
    sheet_obj = st.session_state['sheet']
    
    st.write("---")
    
    if mode == "Manual Audit":
        st.subheader("🚀 Run Manual Audit")
        col1, col2 = st.columns(2)
        with col1:
            start_row = st.number_input("Start Row (2-index)", min_value=2, value=2)
        with col2:
            end_row = st.number_input("End Row", min_value=2, value=max(2, len(df)+1))
            
        if st.button("▶️ Start Manual Scraper"):
            if not selected_webhooks:
                st.warning("Please select at least one Webhook Workflow.")
            else:
                status_container = st.empty()
                progress_bar = st.progress(0)
                log_area = st.empty()
                
                # --- SCRAPER LOGIC START ---
                try:
                    status_container.info("Initializing Browser...")
                    driver = AmazonStoreScraper.setup_driver(headless=True)
                    
                    # Check Location
                    status_container.info("Setting Location to New York...")
                    if not AmazonStoreScraper.set_delivery_location(driver, "10001"):
                         status_container.error("Failed to set location. Aborting.")
                         driver.quit()
                    else:
                        status_container.success("Location Set! Starting Batch...")
                        
                        rows_to_process = range(start_row, end_row + 1)
                        total = len(rows_to_process)
                        
                        import concurrent.futures
                        from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx
                        
                        ctx = get_script_run_ctx()
                        
                        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as webhook_executor:
                            for i, row_idx in enumerate(rows_to_process):
                                try:
                                    df_idx = row_idx - 2
                                    if df_idx < 0 or df_idx >= len(df):
                                        continue # Out of bounds
                                        
                                    current_url = df.iloc[df_idx]['Store URL'] # Adjust Column Name if needed
                                    if not current_url or pd.isna(current_url):
                                        continue

                                    # Select webhook to use based on index (Round Robin distribution)
                                    webhook_to_use = selected_webhooks[i % len(selected_webhooks)]

                                    status_container.info(f"Processing ({i+1}/{total}): {current_url} | Target: {webhook_to_use.split('/')[-1]}")
                                    
                                    # 1. Scrape with Retry
                                    max_retries = 2
                                    scrape_success = False
                                    
                                    for attempt in range(max_retries):
                                        try:
                                            my_asin, title, keyword, comp1, comp2 = AmazonStoreScraper.process_store(driver, current_url)
                                            if my_asin:
                                                scrape_success = True
                                                log_area.code(f"Found: {my_asin} | Title: {title[:30]}...")
                                                
                                                # 2. Webhook - ONLY if all 3 ASINs exist
                                                if comp1 and comp2:
                                                    # Dispatch webhook to background to avoid 2 minute wait
                                                    def bg_webhook_task(row_num, w_url, a, c1, c2, s_obj, log):
                                                        add_script_run_ctx(ctx=ctx)
                                                        for w_attempt in range(max_retries):
                                                            try:
                                                                success, response_data = WebhookHandler.send_audit_data(w_url, a, c1, c2)
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
                                                                        GoogleSheetHandler.update_audit_link(s_obj, row_num, sheet_url)
                                                                        log.success(f"📝 Updated Row {row_num} with Audit URL")
                                                                        return # Exit on success
                                                                    else:
                                                                        raise Exception("Could not extract sheet URL from response")
                                                                else:
                                                                    raise Exception(f"Failed to send webhook to {w_url}")
                                                            except Exception as we:
                                                                log.warning(f"⚠️ Webhook Retry {w_attempt+1}/{max_retries} for Row {row_num}: {we}")
                                                                if w_attempt == max_retries - 1:
                                                                    log.error(f"❌ Final Webhook Error on Row {row_num}: {we}")
                                                                import time
                                                                time.sleep(5)
                                                    
                                                    webhook_executor.submit(bg_webhook_task, row_idx, webhook_to_use, my_asin, comp1, comp2, sheet_obj, log_area)
                                                    st.toast(f"⏳ {my_asin} Scraped! Webhook pushing to background...")
                                                else:
                                                    log_area.warning(f"⚠️ Skipping Webhook: Missing competitors for {my_asin} (Comp1: {comp1}, Comp2: {comp2})")
                                                break # Break out of scrape retry loop
                                            else:
                                                raise Exception("ASIN not found")
                                        except Exception as e:
                                            log_area.warning(f"⚠️ Scrape Retry {attempt+1}/{max_retries} for Row {row_idx}: {e}")
                                            if attempt == max_retries - 1:
                                                log_area.error(f"❌ Final Scrape Error on Row {row_idx}: {e}")
                                            import time
                                            time.sleep(3)
                                            # Refresh driver on retry
                                            driver.quit()
                                            driver = AmazonStoreScraper.setup_driver(headless=True)
                                            AmazonStoreScraper.set_delivery_location(driver, "10001")
                                        
                                    progress_bar.progress((i + 1) / total)
                                    
                                except Exception as e:
                                    st.error(f"Error on Row {row_idx}: {e}")
                            
                            status_container.success("Batch Processing Complete! 🎉 Waiting for webhooks to finish...")
                        driver.quit()
                        
                except Exception as e:
                    status_container.error(f"Critical Error: {e}")

    elif mode == "VPS Mass Automation (24/7)":
        st.subheader("⚡ VPS Mass Automation (24/7)")
        st.info("Runs in headless mode with concurrency for fast, mass-scale processing. Automatically skips errors and writes to the sheet.")
        
        st.warning("To Stop the automation while it is running, press the **Stop** button in the top right corner of Streamlit.")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            start_row = st.number_input("Start Row (2-index)", min_value=2, value=2)
        with col2:
            end_row = st.number_input("End Row", min_value=2, value=max(2, len(df)+1))
        with col3:
            concurrency = st.number_input("Concurrency (Threads)", min_value=1, max_value=20, value=5)
            
        if st.button("🚀 Start VPS Automation"):
            if not selected_webhooks:
                st.warning("Please select at least one Webhook Workflow.")
            else:
                log_placeholder = st.empty()
                progress_bar = st.progress(0)
                
                try:
                    vps_automation.run_vps_batch(
                        df=df, 
                        start_row=start_row, 
                        end_row=end_row, 
                        concurrency=concurrency, 
                        webhooks=selected_webhooks, 
                        sheet=sheet_obj,
                        log_placeholder=log_placeholder,
                        progress_bar=progress_bar,
                        stop_event=None
                    )
                except Exception as e:
                    st.error(f"VPS Automation Error: {e}")

# --- Live Logs Section ---
st.write("---")
st.subheader("📋 Live Logs")
with st.expander("", expanded=False):
    if st.button("🔄 Refresh Logs"):
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, "r") as f:
                log_content = f.read()[-3000:]  # Last 3000 characters
                st.code(log_content, language=None)
        else:
            st.warning("No log file found.")
