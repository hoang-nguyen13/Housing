import time
import psutil
import threading
import csv
import os
import re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from tqdm import tqdm
import gc
from datetime import datetime
import socket

duplicate_cache = set()

def check_internet_connection():
    try:
        socket.create_connection(("8.8.8.8", 53), timeout=5)
        return True
    except OSError:
        return False

def wait_for_internet():
    was_disconnected = False
    if check_internet_connection():
        return
    max_wait = 300
    check_interval = 10
    elapsed = 0
    while elapsed < max_wait:
        print(f"Internet connection lost. Waiting up to {max_wait - elapsed} seconds...")
        was_disconnected = True
        time.sleep(check_interval)
        elapsed += check_interval
        if check_internet_connection():
            if was_disconnected:
                print("Internet connection restored. Resuming operation.")
            return
    print("Internet still unavailable after 5 minutes. Continuing to wait...")

def print_memory_usage(stop_event):
    process = psutil.Process()
    while not stop_event.is_set():
        mem_info = process.memory_info()
        rss_mb = mem_info.rss / 1024 / 1024
        vms_mb = mem_info.vms / 1024 / 1024
        print(f"Memory Usage - RSS: {rss_mb:.2f} MB, VMS: {vms_mb:.2f} MB")
        time.sleep(480)
    return None

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
    )
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    service = Service('/usr/local/bin/chromedriver')
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    })
    return driver

def load_csv_to_cache(csv_file_path_merged):
    global duplicate_cache
    if os.path.exists(csv_file_path_merged):
        with open(csv_file_path_merged, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            initial_size = len(duplicate_cache)
            sample_entries = []
            for i, row in enumerate(reader):
                if "Id" in row and "Date Posted" in row and row["Id"] and row["Date Posted"]:
                    entry = (str(row["Id"]).strip(), str(row["Date Posted"]).strip())
                    duplicate_cache.add(entry)
                    if i < 5:
                        sample_entries.append(entry)
                else:
                    print(f"Skipping invalid row: {row}")
    else:
        print(f"CSV file not found at {csv_file_path_merged}")

def check_duplicate_in_csv(product_id, date_element):
    check_tuple = (product_id, date_element)
    is_duplicate = check_tuple in duplicate_cache
    if is_duplicate:
        print(f"Duplicate found: ID={product_id}, Date={date_element}")
    return is_duplicate

def fetch_coordinates(driver, full_href, product_id):
    coordinates = "N/A"
    try:
        wait_for_internet()
        driver.get(full_href)
        WebDriverWait(driver, 45).until(EC.presence_of_element_located((By.CLASS_NAME, "lazyload")))
        detail_source = driver.page_source
        detail_soup = BeautifulSoup(detail_source, "html.parser")
        iframe = detail_soup.find("iframe", class_="lazyload")
        if iframe and "data-src" in iframe.attrs:
            match = re.search(r"q=([-+]?\d+\.\d+),([-+]?\d+\.\d+)", iframe["data-src"])
            if match:
                coordinates = f"{match.group(1)}, {match.group(2)}"
        del detail_source
        del detail_soup
        if 'iframe' in locals():
            del iframe
        if 'match' in locals():
            del match
        gc.collect()
    except Exception as e:
        print(f"Failed to load {full_href}: {type(e).__name__}: {str(e)}. Coordinates set to N/A.")
        if 'detail_source' in locals():
            del detail_source
        if 'detail_soup' in locals():
            del detail_soup
        if 'iframe' in locals():
            del iframe
        if 'match' in locals():
            del match
        gc.collect()
    return coordinates

def scrape_district(district):
    print("Scraping for", district)
    driver = setup_driver()
    first_page_url = f"https://batdongsan.com.vn/ban-can-ho-chung-cu-{district}"
    paginated_url = f"https://batdongsan.com.vn/ban-can-ho-chung-cu-{district}/p{{}}"
    page = 1
    district_data = []
    headers = ["Id", "Date Posted", "Product Title", "Price", "Area", "Price per m²", "Bedrooms", "Toilets", "Location", "Coordinates"]
    current_date = datetime.now()
    month_abbr = current_date.strftime('%b').lower()
    year = current_date.strftime('%Y')
    data_dir = f"data/{month_abbr}_{year}"
    data_dir_relevance = f"data/relevance"
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(data_dir_relevance, exist_ok=True)
    csv_file_path = os.path.join(data_dir, f"filtered_real_estate_listings_{district}.csv")
    csv_file_path_merged = os.path.join(data_dir_relevance, f"merged_real_estate_listings.csv")
    if not os.path.exists(csv_file_path):
        with open(csv_file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
    stop_event = threading.Event()
    memory_thread = threading.Thread(target=print_memory_usage, args=(stop_event,))
    memory_thread.daemon = True
    memory_thread.start()
    load_csv_to_cache(csv_file_path_merged)
    try:  
        wait_for_internet()
        driver.get(first_page_url)
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        pagination = soup.find("div", class_="re__pagination-group")
        next_button = pagination.find("a", class_="re__pagination-icon") if pagination else None
        max_page = 1
        if next_button:
            last_page_link = next_button.find_previous("a", class_="re__pagination-number")
            if last_page_link and "pid" in last_page_link.attrs:
                max_page = int(last_page_link["pid"])
        print("Estimated max page for", district, "is", max_page)
        pbar = tqdm(total=max_page, desc=f"Scraping {district}", unit="page")
        while True:
            url = first_page_url if page == 1 else paginated_url.format(page)
            retries = 1
            while True:
                try:
                    wait_for_internet()
                    driver.get(url)
                    WebDriverWait(driver, 60).until(
                        lambda driver: driver.find_elements(By.CLASS_NAME, "re__srp-list") or 
                                       driver.find_elements(By.CLASS_NAME, "re__srp-empty")
                    )
                    break
                except TimeoutException:
                    if retries > 0:
                        print(f"Timeout on page {page}. Retrying (attempts left: {retries})...")
                        retries -= 1
                        continue
                    else:
                        print(f"Timeout on page {page} after retry. Stopping.")
                        break
            if retries == 0:
                break
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, "html.parser")
            empty_results = soup.find("div", class_="re__srp-empty")
            if empty_results and "Không có kết quả nào phù hợp" in empty_results.get_text():
                print(f"No more results found on page {page}. Stopping scraping for {district}.")
                break
            srp_list = soup.find("div", class_="re__srp-list")
            if not srp_list:
                print(f"No listings on page {page}. Assuming end of results.")
                break
            srp_list = soup.find("div", class_="re__srp-list")
            all_cards = srp_list.find_all("div", class_="js__card", recursive=True)
            for card in all_cards:
                card_classes = " ".join(card.get("class", []))
                if ("promoted-ads-appearance-position" in card_classes or card.get("prid", "0") == "0"):
                    continue
                if card.find_parent("div", class_="re__listing-verified-similar-v2"):
                    continue
                product_link = card.find("a", class_="js__product-link-for-product-id")
                if product_link and "js__product-link-promotion-ads" in " ".join(product_link.get("class", [])):
                    continue
                product_id = product_link["data-product-id"] if product_link else "N/A"
                date_element = card.find("span", class_="re__card-published-info-published-at")
                date_element = date_element["aria-label"] if date_element else "N/A"
                if check_duplicate_in_csv(product_id, date_element):
                    continue
                info_div = card.find("div", class_="re__card-info")
                if not info_div:
                    continue
                product_title = card.find("span", class_="pr-title js__card-title")
                product_title = product_title.text.strip() if product_title else "N/A"
                price = card.find("span", class_="re__card-config-price js__card-config-item")
                price = price.text.strip() if price else "N/A"
                area = card.find("span", class_="re__card-config-area js__card-config-item")
                area = area.text.strip() if area else "N/A"
                price_per_m2 = card.find("span", class_="re__card-config-price_per_m2 js__card-config-item")
                price_per_m2 = price_per_m2.text.strip() if price_per_m2 else "N/A"
                bedroom = card.find("span", class_="re__card-config-bedroom js__card-config-item")
                bedroom = bedroom.text.strip() if bedroom else "N/A"
                toilet = card.find("span", class_="re__card-config-toilet js__card-config-item")
                toilet = toilet.text.strip() if toilet else "N/A"
                location = card.find("div", class_="re__card-location")
                location = location.find("span").text.strip() if location else "N/A"
                href = product_link["href"] if product_link else "N/A"
                full_href = f"https://batdongsan.com.vn{href}" if href != "N/A" else "N/A"
                coordinates = fetch_coordinates(driver, full_href, product_id)
                district_data.append([product_id, date_element, product_title, price, area, 
                                     price_per_m2, bedroom, toilet, location, coordinates])
            if district_data:
                df_page = pd.DataFrame(district_data, columns=headers)
                df_page.to_csv(csv_file_path, mode="a", header=False, index=False)
                for entry in district_data:
                    duplicate_cache.add((entry[0], entry[1]))
                district_data = []
                del df_page
                del page_source
                del soup
                gc.collect()
            page += 1
            pbar.update(1)
        pbar.close()
    finally:
        stop_event.set()
        memory_thread.join(timeout=1)
        driver.quit()
        print(f"Finished scraping for {district}. Data saved.")