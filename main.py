import time
from scraping import scrape_district, districts, pre_patch_chromedriver

def main():
    response = input("Do you want to start scraping? (yes/no): ").strip().lower()
    if response != "yes":
        print("Scraping aborted.")
        return

    driver_choice = input("Use undetected_chromedriver instead of Safari? (yes/no): ").strip().lower()
    use_chrome = driver_choice == "yes"
    
    if use_chrome:
        pre_patch_chromedriver()  # Ensure ChromeDriver is ready before scraping
    
    for district in districts:
        scrape_district(district, use_chrome=use_chrome)
        print(f"Completed {district}. Waiting 10 seconds before next district...")
        time.sleep(10)  # Wait 10 seconds between districts

if __name__ == "__main__":
    main()