import time
import re
import csv
import requests
import logging
import random
from urllib.parse import urljoin

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

# --- Configuration ---
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY", "")

BASE_URL = "https://api.openai.com/v1"
MODEL_ID = "gpt-4o-mini"   # Primary
FALLBACK_MODEL_ID = "gpt-4o" # Fallback

# INPUT_FILE = "stores.txt"
# OUTPUT_FILE = "results.csv"

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log", encoding='utf-8', mode='w'),
        logging.StreamHandler()
    ]
)

def get_main_keyword(title):
    """Extracts main keyword using OpenRouter API with a refined prompt and retry logic."""
    if not title or title == "Unknown":
        return "None"
    
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    prompt = f"""
    Analyze this Amazon Product Title and extract the single most relevant SEARCH KEYWORD that a customer would type to find this specific product.
    
    Product Title: "{title}"
    
    Guidelines:
    1. Output ONLY the keyword. No explanations, no quotes, no labels.
    2. Keep it short (2-4 words max).
    3. Remove brand names, colors, sizes, and pack counts.
    4. Focus on the generic product name (e.g., "Office Chair Wheels", "Shea Butter", "Shower Cap").
    
    Example:
    Title: "LINCO 3” Rollerblade Office Chair Wheel - Set of 5"
    Output: Office Chair Wheels
    """
    
    data = {
        "model": MODEL_ID,
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1
    }
    
    for attempt in range(3): # Retry logic
        # Try fallback model if previous attempt failed due to limit
        if attempt > 0:
            logging.info(f"Switching to fallback model: {FALLBACK_MODEL_ID}")
            data["model"] = FALLBACK_MODEL_ID
            
        try:
            response = requests.post(f"{BASE_URL}/chat/completions", headers=headers, json=data, timeout=45)
            if response.status_code == 200:
                result = response.json()
                try:
                    keyword = result["choices"][0]["message"]["content"].strip()
                except KeyError:
                    keyword = result["choices"][0]["content"][0]["text"].strip()
                
                # Cleanup
                keyword = re.sub(r"<\/?s>", "", keyword).strip()
                keyword = keyword.replace("[/s]", "").replace("<s>", "").strip()
                keyword = keyword.replace('"', '').replace("'", "")
                
                if len(keyword.split()) > 6:
                    keyword = " ".join(keyword.split()[:4])
                    
                return keyword
            else:
                logging.error(f"API Error (Attempt {attempt+1}): {response.status_code} - {response.text}")
                time.sleep(5)
        except Exception as e:
            logging.error(f"Error extracting keyword (Attempt {attempt+1}): {e}")
            time.sleep(5)
            
    logging.warning("All attempts to extract keyword failed due to API limits or errors.")
    return "None"

import threading
driver_lock = threading.Lock()

def setup_driver(headless=False):
    """Sets up Undetected Chrome WebDriver with Thread-Safety."""
    options = uc.ChromeOptions()
    options.add_argument("--start-maximized")
    
    if headless:
        options.add_argument("--headless=new")
        
    # Critical arguments for Docker and Headless execution
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-extensions")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--disable-blink-features=AutomationControlled")
        
    # Disable images for speed
    # prefs = {"profile.managed_default_content_settings.images": 2}
    # options.add_experimental_option("prefs", prefs)
    
    with driver_lock:
        driver = uc.Chrome(options=options, version_main=147, headless=headless)
    return driver

def extract_asin_from_url(url):
    """Extracts ASIN from a URL using regex."""
    if not url: return None
    match = re.search(r"/dp/([A-Z0-9]{10})", url)
    if match:
        return match.group(1)
    return None
def set_delivery_location(driver, zip_code="10001"):
    """
    Sets Amazon delivery location (default 10001 = New York).
    3-method fallback chain — works on VPS headless Chrome.
    Always returns True so scraping is never blocked by location failure.
    """
    try:
        logging.info(f"Setting Amazon delivery location to zip: {zip_code}")
        driver.get("https://www.amazon.com")
        time.sleep(random.uniform(3, 4))

        # Already set?
        try:
            loc = driver.find_element(By.ID, "glow-ingress-line2").text
            if zip_code in loc or "New York" in loc or "10001" in loc:
                logging.info(f"Location already set: {loc}")
                return True
        except Exception:
            pass

        # Method 1: Location popup UI
        try:
            loc_btn = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "nav-global-location-popover-link"))
            )
            driver.execute_script("arguments[0].click();", loc_btn)
            time.sleep(2)
            zip_input = WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.ID, "GLUXZipUpdateInput"))
            )
            driver.execute_script("arguments[0].value = '';", zip_input)
            zip_input.send_keys(zip_code)
            time.sleep(0.5)
            for btn_id in ["GLUXZipUpdate", "GLUXZipUpdate-announce"]:
                try:
                    driver.execute_script("arguments[0].click();",
                                         driver.find_element(By.ID, btn_id))
                    break
                except Exception:
                    pass
            time.sleep(2.5)
            try:
                loc = driver.find_element(By.ID, "glow-ingress-line2").text
                logging.info(f"Location after popup: {loc}")
            except Exception:
                pass
            logging.info("Method 1 (popup) done.")
            return True
        except Exception as e:
            logging.warning(f"Method 1 (popup) failed: {e}")

        # Method 2: Direct AJAX address-change URL
        try:
            driver.get(
                f"https://www.amazon.com/gp/delivery/ajax/address-change.html"
                f"?locationType=LOCATION_INPUT&zipCode={zip_code}"
                f"&storeContext=generic&deviceType=web&pageType=Gateway&actionSource=gw"
            )
            time.sleep(2)
            driver.get("https://www.amazon.com")
            time.sleep(2)
            try:
                loc = driver.find_element(By.ID, "glow-ingress-line2").text
                logging.info(f"Location after AJAX method: {loc}")
                if zip_code in loc or "New York" in loc:
                    logging.info(f"Location set via AJAX URL.")
                    return True
            except Exception:
                pass
        except Exception as e:
            logging.warning(f"Method 2 (AJAX URL) failed: {e}")

        logging.info("Location set failed — VPS is US (Boston), Amazon serves US content by default.")

    except Exception as e:
        logging.warning(f"set_delivery_location outer error: {e}")

    return True

def process_store(driver, store_url):
    """Process a single store URL with refined logic for Seller Profiles and Brand Stores."""
    
    # Retry loop for the entire store processing
    for attempt in range(3):
        logging.info(f"Processing Store: {store_url} (Attempt {attempt+1}/3)")
        
        try:
            driver.get(store_url)
            time.sleep(random.uniform(4, 6))

            # Early exit if "No results for your search query." is found
            try:
                page_text = driver.find_element(By.TAG_NAME, "body").text
                if "No results for your search query." in page_text or "No results for" in page_text:
                    logging.warning(f"Store has no results: {store_url}")
                    return None, None, None, None, None
            except:
                pass
            
            product_link = None
            
            # --- Store Type 1: Seller Profile (/sp?seller=XXXX) ---
            # Direct approach: build s?me=SELLERID URL instead of clicking "Visit storefront"
            if "/sp?" in store_url:
                try:
                    from urllib.parse import urlparse, parse_qs
                    parsed = urlparse(store_url)
                    qs = parse_qs(parsed.query)
                    seller_id = qs.get("seller", [None])[0]

                    if seller_id:
                        seller_search_url = f"https://www.amazon.com/s?me={seller_id}&marketplaceID=ATVPDKIKX0DER"
                        logging.info(f"Seller Profile detected. Navigating directly to: {seller_search_url}")
                        driver.get(seller_search_url)
                        time.sleep(random.uniform(4, 6))

                        # Check for 'No results' on seller search page
                        try:
                            page_text = driver.find_element(By.TAG_NAME, "body").text
                            if "No results for your search query" in page_text or "No results for" in page_text:
                                logging.warning(f"Seller search has no results: {seller_search_url}")
                                return None, None, None, None, None
                        except:
                            pass

                        try:
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-component-type='s-search-result']"))
                            )
                            results = driver.find_elements(By.CSS_SELECTOR, "div[data-component-type='s-search-result']")
                            for res in results:
                                try:
                                    # Skip sponsored
                                    sponsored = res.find_elements(By.CSS_SELECTOR, ".puis-sponsored-label-text, .s-sponsored-label-text")
                                    if sponsored or "Sponsored" in res.text:
                                        continue
                                    link_elem = res.find_element(By.CSS_SELECTOR, "h2 a")
                                    href = link_elem.get_attribute("href")
                                    if href and "/dp/" in href:
                                        product_link = href
                                        logging.info(f"Found product via seller search: {product_link}")
                                        break
                                except Exception:
                                    continue
                        except Exception as e:
                            logging.warning(f"No search results found for seller {seller_id}: {e}")
                    else:
                        logging.warning(f"Could not extract seller ID from URL: {store_url}")
                except Exception as e:
                    logging.error(f"Error in Seller Profile flow: {e}")


            # --- Store Type 2: Brand Store (/stores/) ---
            # Also handles the "Supernal" case which is a Brand Store page.
            # --- Store Type 2: Brand Store (/stores/) ---
            if not product_link:
                logging.info("Trying Brand Store / Generic product detection...")
                
                # Helper to check all strategies
                def check_strategies(d):
                    link = None
                    
                    # Strategy C: Overlays (High Priority - specific for Moment/Modern Stores)
                    # These are often transparent (rgba(0,0,0,0)) so is_displayed() might fail or be tricky.
                    # We trust the class name implies it's a product link wrapper.
                    try:
                        overlays = d.find_elements(By.CSS_SELECTOR, "a[class*='Overlay__overlay'], a[class*='ProductGridItem__overlay']")
                        for l in overlays:
                            h = l.get_attribute("href")
                            if h and "/dp/" in h:
                                logging.info(f"Found Overlay Link: {h}")
                                return h
                    except: pass

                    # Strategy B: Tricky Shoppable Images & Hovers
                    try:
                        actions = ActionChains(d)
                        # Explicit targets
                        triggers = d.find_elements(By.CSS_SELECTOR, "div[data-testid='grid-item-image'], li[data-testid='product-grid-item']")
                        triggers.extend(d.find_elements(By.CSS_SELECTOR, "[class*='shoppable'], [class*='point'], [data-testid*='shoppable']"))
                        triggers.extend(d.find_elements(By.XPATH, "//*[contains(text(), 'See Products')]"))
                        
                        for t in triggers:
                            try:
                                actions.move_to_element(t).perform()
                                time.sleep(0.1)
                            except: pass
                        
                        # Check links again after hover
                        potential_links = d.find_elements(By.CSS_SELECTOR, "a[href*='/dp/']")
                        for l in potential_links:
                            h = l.get_attribute("href")
                            if not h: continue
                            if any(x in h for x in ["customerReviews", "offer-listing", "qa", "questions", "signin", "footer", "bestsellers"]): continue
                            if l.is_displayed(): return h
                    except: pass
                    
                    # Strategy A: Standard Product Tiles (Fallback)
                    try:
                        potential_links = d.find_elements(By.CSS_SELECTOR, "a[href*='/dp/']")
                        for l in potential_links:
                            h = l.get_attribute("href")
                            if not h: continue
                            if any(x in h for x in ["customerReviews", "offer-listing", "qa", "questions", "signin", "review", "vote", "footer", "bestsellers"]): continue
                            
                            # Just return the URL, whether visible or not.
                            return h
                    except: pass
                    
                    # Strategy D: Quick Look Buttons (New - Observation only for now)
                    try:
                        quick_looks = d.find_elements(By.CSS_SELECTOR, "button[class*='QuickLook'], button[aria-label*='Quick look']")
                        # Logic to click could be added here if extraction fails
                    except: pass
                    
                    return None

                # Try finding product with scrolling (up to 5 page scrolls)
                for scroll_attempt in range(5):
                    product_link = check_strategies(driver)
                    if product_link: break
                    
                    logging.info(f"Product not found yet. Scrolling down (Attempt {scroll_attempt+1}/5)...")
                    driver.execute_script("window.scrollBy(0, 800);")
                    time.sleep(4.0)
                
                # Navigation Fallback: Try clicking "Products" or "Shop" if strictly nothing found
                if not product_link and attempt == 0: # Only on first main attempt
                    try:
                        logging.info("Trying Store Navigation (Products/Shop)...")
                        nav_items = driver.find_elements(By.XPATH, "//*[contains(text(), 'Products') or contains(text(), 'Shop All') or contains(text(), 'Shop')]")
                        for nav in nav_items:
                            try:
                                if nav.is_displayed():
                                    nav.click()
                                    time.sleep(5)
                                    product_link = check_strategies(driver)
                                    if product_link: break
                            except: pass
                    except: pass

            if not product_link:
                logging.warning(f"No product link found. (Attempt {attempt+1}/3)")
                if attempt < 2:
                    logging.info("Retrying store...")
                    continue # Retry the loop
                else:
                    return None, None, None, None, None

            logging.info(f"Found Product: {product_link}")
            driver.get(product_link)
            time.sleep(random.uniform(3, 5))
            
            # --- Product Details ---
            try:
                title = driver.find_element(By.ID, "productTitle").text.strip()
            except:
                title = "Unknown"

            my_asin = extract_asin_from_url(driver.current_url)
            if not my_asin:
                my_asin = extract_asin_from_url(product_link)
            
            if not my_asin:
                 logging.warning(f"ASIN Not found on product page. (Attempt {attempt+1}/3)")
                 if attempt < 2: continue
                 return None, None, None, None, None

            logging.info(f"ASIN: {my_asin}")
            logging.info(f"Title: {title[:60]}...")

            # --- Keyword Extraction ---
            keyword = get_main_keyword(title)
            logging.info(f"Keyword Extracted: {keyword}")

            # If title is Unknown, skip — we can't search competitors without a real title
            if not title or title == "Unknown":
                logging.warning(f"Row has no valid product title. Skipping competitor search.")
                return my_asin, title, None, None, None

            # If keyword extraction failed, use title words as fallback
            if not keyword or keyword == "None":
                title_words = [w for w in title.split() if len(w) > 3][:3]
                keyword = " ".join(title_words) if title_words else None
                if not keyword:
                    logging.warning("No usable keyword — skipping competitor search.")
                    return my_asin, title, None, None, None
                logging.warning(f"Keyword API failed. Using fallback: '{keyword}'")

            # --- Competitor Search ---
            search_url = f"https://www.amazon.com/s?k={keyword.replace(' ', '+')}"
            logging.info(f"Searching Competitors: {search_url}")
            driver.get(search_url)
            time.sleep(random.uniform(4, 6))

            competitors = []
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-component-type='s-search-result']"))
                )
                results = driver.find_elements(By.CSS_SELECTOR, "div[data-component-type='s-search-result']")
                for i, res in enumerate(results):
                    if len(competitors) >= 2: break
                    try:
                        if "Sponsored" in res.text: continue
                        sponsored_labels = res.find_elements(By.CSS_SELECTOR, ".puis-sponsored-label-text, .s-sponsored-label-text")
                        if sponsored_labels: continue
                        classes = res.get_attribute("class")
                        if "AdHolder" in classes or "s-result-item-placeholder" in classes: continue
                        
                        asin = res.get_attribute("data-asin")
                        if asin and asin != my_asin:
                            competitors.append(asin)
                    except: continue
            except Exception as e:
                logging.error(f"Error searching competitors: {e}")

            comp1 = competitors[0] if len(competitors) > 0 else None
            comp2 = competitors[1] if len(competitors) > 1 else None

            logging.info(f"Competitors: comp1={comp1}, comp2={comp2}")

            # Success!
            return my_asin, title, keyword, comp1, comp2
            
        except Exception as e:
            logging.error(f"Critical error in process_store (Attempt {attempt+1}): {e}")
            if attempt < 2:
                logging.info("Waiting 10 seconds before retry to allow page to stabilize...")
                time.sleep(10)
            else:
                return None, str(e), "Error", None, None

    return None, None, None, None, None

def main():
    driver = setup_driver()
    
    try:
        # STRICT CHECK: If location fails, DO NOT PROCEED
        if not set_delivery_location(driver, "10001"):
            logging.error("Aborting script because location could not be set to New York/10001.")
            return

        with open(INPUT_FILE, "r") as f:
            urls = [line.strip() for line in f if line.strip()]

        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Store URL", "My ASIN", "Title", "Keyword", "Competitor 1", "Competitor 2"])
            
            for url in urls:
                try:
                    my_asin, title, keyword, comp1, comp2 = process_store(driver, url)
                    writer.writerow([url, my_asin, title, keyword, comp1, comp2])
                    csvfile.flush()
                except Exception as e:
                    logging.error(f"Failed {url}: {e}")
                    writer.writerow([url, "Error", str(e), "", "", ""])

    finally:
        driver.quit()
        logging.info("Done.")

if __name__ == "__main__":
    main()
