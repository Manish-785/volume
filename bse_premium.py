import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import re

def clean_numeric_value(text_value):
    """
    Cleans a string to a float, handling commas and non-numeric values.
    """
    if isinstance(text_value, str) and text_value.strip() not in ['-', '']:
        try:
            return float(text_value.replace(',', ''))
        except ValueError:
            return 0.0
    return 0.0

def scrape_bse_daily_futures_turnover():
    """
    Scraper #1: Fetches daily Futures Turnover from the BSE Market Statistics page.
    This involves clicking through Year -> Month to get to the daily data.
    """
    print("-> Starting BSE Futures Scraper...")
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless")
    options.add_argument("--window-size=1920,1200")
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)
    
    # This is the correct URL for the Futures statistics page
    url = "https://www.bseindia.com/markets/keystatics/Keystat_turnover_deri.aspx?expandable=0"
    driver.get(url)
    scraped_data = []

    try:
        # Step 1: Click the most recent year link
        # The page renders the year as an <a> where id starts with 'ContentPlaceHolder1_gvReport' and text like '2025-2026'.
        # Try multiple strategies for robustness: id-prefix, exact/text contains, then class fallback.
        def try_click(element):
            try:
                element.click()
                return True
            except Exception:
                # JS fallback
                try:
                    driver.execute_script("arguments[0].click();", element)
                    return True
                except Exception:
                    return False

        year_link = None

        # 1) Try id prefix (most specific)
        try:
            year_link = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[starts-with(@id, 'ContentPlaceHolder1_gvReport') and contains(@id, 'Linkbtn')]") ))
        except Exception:
            pass

        # 2) Try link text containing the year range (e.g. '2025-2026')
        if year_link is None:
            try:
                year_link = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(),'2025-2026')]") ))
            except Exception:
                pass

        # 3) Fallback to class selector
        if year_link is None:
            try:
                year_link = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.tablebluelink")))
            except Exception:
                pass

        if year_link is None:
            raise Exception('Could not find year link on BSE page')

        if try_click(year_link):
            print("Clicked year link, loading month data...")
        else:
            raise Exception('Failed to click year link')
        time.sleep(2)  # Wait for the month table to load

        # Step 2: Click the most recent month link from the new table
        # The month anchor is rendered alongside hidden inputs for year/month. Example HTML:
        # <input id='...hdnYear_0' value='2025'>
        # <input id='...hdnMonth_0' value='10'>
        # <a id='...lnkMonth_T_0' class='tablebluelink'>Oct-25</a>
        # Try multiple strategies to locate the correct month link robustly.
        month_link = None

        # 1) id prefix containing lnkMonth
        try:
            month_link = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[starts-with(@id, 'ContentPlaceHolder1_gvYearwise') and contains(@id, 'lnkMonth')]") ))
        except Exception:
            pass

        # 2) link text for the expected month label (e.g. 'Oct-25')
        if month_link is None:
            try:
                month_link = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(),'Oct-25')]") ))
            except Exception:
                pass

        # 3) find an input hidden month field and click its following anchor
        if month_link is None:
            try:
                month_link = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[contains(@id, 'hdnMonth')]/following-sibling::a[1]")))
            except Exception:
                pass

        # 4) fallback to generic class-based selector
        if month_link is None:
            try:
                month_link = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.tablebluelink")))
            except Exception:
                pass

        if month_link is None:
            raise Exception('Could not find month link on BSE page')

        if try_click(month_link):
            print("Clicked month link, loading daily data...")
        else:
            raise Exception('Failed to click month link')

        time.sleep(2)  # Wait for the daily table to load

        # Step 3: Scrape the final daily table
        daily_table = wait.until(EC.visibility_of_element_located(
            (By.ID, "ContentPlaceHolder1_gvdaliy_T_new")
        ))

        year_label = driver.find_element(By.ID, "ContentPlaceHolder1_lbl_year").text
        year_match = re.search(r'\d{4}', year_label)
        current_year = year_match.group(0) if year_match else "YYYY"
        
        rows = daily_table.find_elements(By.XPATH, ".//tbody/tr[position()>2]") # Skip header rows

        for row in rows:
            print(row)
            cols = [td.text.strip() for td in row.find_elements(By.TAG_NAME, "td")]
            full_date = f"{cols[0]} {current_year}"
            options_turnover = clean_numeric_value(cols[7])  # 'Index Options Premium Turnover' is the 7th column
            
            scraped_data.append({
                'Date': pd.to_datetime(full_date, format='%b %d %Y').strftime('%Y-%m-%d'),
                'BSE Options Premium': options_turnover,
            })
            
        print("-> BSE Futures Scraper: Success.")
        return pd.DataFrame(scraped_data)

    except Exception as e:
        print(f"-> BSE Futures Scraper: FAILED. Error: {e}")
        return None
    finally:
        driver.quit()

def scrape_nse_daily_turnover():
    """
    Scraper #2: Fetches daily 'Index Options Premium Turnover' from NSE India.
    """
    print("-> Starting NSE Options Scraper...")
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless")
    options.add_argument("--window-size=1920,1200")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)
    
    url = "https://www.nseindia.com/market-data/business-growth-fo-segment"
    time.sleep(2)  # Initial wait for any anti-bot measures
    driver.get(url)
    scraped_data = []

    try:
        # Helper to click with JS fallback
        def click_with_fallback(el):
            try:
                el.click()
                return True
            except Exception:
                try:
                    driver.execute_script("arguments[0].click();", el)
                    return True
                except Exception:
                    return False

        # Click year link robustly, then wait for month anchors to appear
        year_el = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.year_link")))
        if not click_with_fallback(year_el):
            raise Exception('Failed to click NSE year link')

        # Wait longer for month anchors to be present
        wait_long = WebDriverWait(driver, 30)
        try:
            wait_long.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a.month_link")))
        except Exception:
            # fallback: any anchor with class in the page
            wait_long.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a.tablebluelink, a.month_link")))

        month_el = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.month_link")))
        if not click_with_fallback(month_el):
            raise Exception('Failed to click NSE month link')

        # Wait for the daily table to be visible
        daily_table = wait_long.until(EC.visibility_of_element_located((By.XPATH, "//table[contains(@class, 'common_table')][3]")))
        
        headers = ["Date", "IFC", "IFT", "SFC", "SFT", "IOC", "Index Options Premium Turnover", "SOC", "SOPT", "TC", "TT"]
        rows = daily_table.find_elements(By.XPATH, ".//tbody/tr")
        
        for row in rows:
            cols = [td.text.strip() for td in row.find_elements(By.TAG_NAME, "td")]
            if len(cols) == len(headers):
                scraped_data.append(dict(zip(headers, cols)))
        print("-> NSE Options Scraper: Success.")
    except Exception as e:
        print(f"-> NSE Options Scraper: FAILED. Error: {e}")
        return None
    finally:
        driver.quit()
        
    df = pd.DataFrame(scraped_data)
    df['Date'] = pd.to_datetime(df['Date'], format='%d-%b-%Y').dt.strftime('%Y-%m-%d')
    df['Index Options Premium Turnover'] = df['Index Options Premium Turnover'].apply(clean_numeric_value)
    return df[['Date', 'Index Options Premium Turnover']]

# --- Main Execution Block ---
if __name__ == "__main__":
    print("--- Starting Data Aggregation Process ---")
    # bse_df = scrape_bse_daily_futures_turnover()
    nse_df = scrape_nse_daily_turnover()

    print(nse_df)
    # if bse_df is not None and nse_df is not None:
    #     print("\n--- Merging and Transforming Data ---")
        
    #     # Merge the two dataframes on the 'Date' column
    #     combined_df = pd.merge(bse_df, nse_df, on='Date', how='inner')

    #     if combined_df.empty:
    #          print("\n❌ Process failed. No matching dates found between BSE and NSE data.")
    #     else:
    #         # Rename the columns to be more descriptive for the final output
    #         combined_df.rename(columns={
    #             'BSE Futures Turnover': 'Futures Turnover (BSE)',
    #             'Index Options Premium Turnover': 'Index Options Premium Turnover (NSE)'
    #         }, inplace=True)

    #         # Create the final calculated columns
    #         combined_df['Date_dt'] = pd.to_datetime(combined_df['Date'])
    #         combined_df['Day'] = combined_df['Date_dt'].dt.day_name()
    #         combined_df['Total'] = combined_df['Futures Turnover (BSE)'] + combined_df['Index Options Premium Turnover (NSE)']
    #         combined_df['Date'] = combined_df['Date_dt'].dt.strftime('%d-%m-%Y')
            
    #         # Select and reorder columns for the final report
    #         final_columns = ['Date', 'Day', 'Futures Turnover (BSE)', 'Index Options Premium Turnover (NSE)', 'Total']
    #         final_df = combined_df[final_columns]

    #         # Save the result to an Excel file
    #         output_filename = "daily_bse_nse_turnover_summary.xlsx"
    #         final_df.to_excel(output_filename, index=False, sheet_name='DailySummary')
            
    #         print(f"\n✅ Success! Combined data saved to {output_filename}")
    #         print("\n--- Final Data Preview (All values in ₹ Crores) ---")
    #         print(final_df.to_string(index=False))
            
    # else:
    #     print("\n❌ Process failed. One or both scrapers could not retrieve data.")