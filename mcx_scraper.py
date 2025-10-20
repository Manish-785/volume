import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import warnings

# Suppress warnings from pandas about chained assignment, which is handled correctly here.
# pandas moved SettingWithCopyWarning in some versions; try the public location first, fall back
# to the older internal location, and finally to a generic Warning if neither exists.
try:
    from pandas.errors import SettingWithCopyWarning
except Exception:
    try:
        SettingWithCopyWarning = pd.core.common.SettingWithCopyWarning
    except Exception:
        SettingWithCopyWarning = Warning

warnings.simplefilter(action='ignore', category=SettingWithCopyWarning)

def scrape_mcx_data(start_date, end_date):
    """
    Scrapes historical commodity data from the MCX India website for a given date range.
    (This function is from the previous response)
    """
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1920,1200")
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)
    
    url = "https://www.mcxindia.com/market-data/historical-data"
    print(f"Navigating to {url}...")
    driver.get(url)

    all_rows_data = []
    headers = []

    try:
        print("Configuring search parameters...")
        wait.until(EC.element_to_be_clickable((By.ID, "Datewise"))).click()
        time.sleep(1)

        from_date_display = f"{start_date['day']}/{start_date['month']}/{start_date['year']}"
        from_date_hidden = f"{start_date['year']}{start_date['month']}{start_date['day']}"
        to_date_display = f"{end_date['day']}/{end_date['month']}/{end_date['year']}"
        to_date_hidden = f"{end_date['year']}{end_date['month']}{end_date['day']}"

        driver.execute_script(f"document.getElementById('txtFromDate').value = '{from_date_display}';")
        driver.execute_script(f"document.getElementById('cph_InnerContainerRight_C004_txtFromDate_hid_val').value = '{from_date_hidden}';")
        driver.execute_script(f"document.getElementById('txtToDate').value = '{to_date_display}';")
        driver.execute_script(f"document.getElementById('cph_InnerContainerRight_C004_txtToDate_hid_val').value = '{to_date_hidden}';")
        
        print("Fetching data...")
        driver.find_element(By.ID, "btnDetail").click()

        wait.until(EC.visibility_of_element_located((By.ID, "tblDatewiseDetail")))

        header_elements = driver.find_elements(By.XPATH, "//table[@id='tblDatewiseDetail']/thead/tr/th")
        headers = [header.text.strip() for header in header_elements]
        print(f"Found table with columns: {headers}")

        while True:
            rows = driver.find_elements(By.XPATH, "//table[@id='tblDatewiseDetail']/tbody/tr")
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                all_rows_data.append([col.text.strip() for col in cols])
            
            try:
                current_page = int(driver.find_element(By.CSS_SELECTOR, "#ddlPagerDetails > option:checked").text)
                total_pages = int(driver.find_element(By.ID, "pagerDetailsCount1").text)
                
                print(f"Scraped page {current_page} of {total_pages}.")

                if current_page >= total_pages:
                    break
                
                driver.find_element(By.ID, "aNextDetails").click()
                wait.until(EC.text_to_be_present_in_element_value((By.ID, 'ddlPagerDetails'), str(current_page + 1)))

            except (NoSuchElementException, TimeoutException):
                break

    except TimeoutException:
        print("A timeout occurred while scraping.")
    except Exception as e:
        print(f"An unexpected error occurred during scraping: {e}")
    finally:
        driver.quit()

    if not all_rows_data:
        return None
    
    return pd.DataFrame(all_rows_data, columns=headers)

def transform_data_to_wide_format(df):
    """
    Transforms the scraped data from long format to the specified wide format.
    """
    print("\nTransforming data into wide format...")
    
    # --- 1. Pre-process Data ---
    # Convert 'Total Value (Lacs)' to a numeric type, removing commas
    df['Total Value (Lacs)'] = df['Total Value (Lacs)'].str.replace(',', '', regex=False).astype(float)
    
    # Convert 'Date' column to datetime objects
    df['Date'] = pd.to_datetime(df['Date'], format='%d %b %Y')
    
    # Standardize commodity names to uppercase for consistent matching
    df['Commodity'] = df['Commodity'].str.upper()
    df['Segment'] = df['Segment'].str.upper()

    # --- 2. Create the Base DataFrame (one row per date) ---
    final_df = pd.DataFrame(df['Date'].unique(), columns=['Date'])
    final_df['Year'] = final_df['Date'].dt.year

    # --- 3. Calculate Instrument-level Totals ---
    instrument_totals = df.groupby(['Date', 'Instrument'])['Total Value (Lacs)'].sum().unstack(fill_value=0)
    instrument_totals = instrument_totals.add_suffix(' LKH')
    
    # Merge instrument totals into the final DataFrame
    final_df = pd.merge(final_df, instrument_totals, on='Date', how='left')
    
    # --- 4. Pivot FUTCOM Commodities ---
    futcom_df = df[df['Instrument'] == 'FUTCOM']
    futcom_pivot = futcom_df.pivot_table(index='Date', columns='Commodity', values='Total Value (Lacs)', aggfunc='sum', fill_value=0)
    futcom_pivot = futcom_pivot.add_prefix('FUTCOM_')
    
    # Merge pivoted FUTCOM data
    final_df = pd.merge(final_df, futcom_pivot, on='Date', how='left')

    # --- 5. Pivot OPTFUT Commodities ---
    optfut_df = df[df['Instrument'] == 'OPTFUT']
    optfut_pivot = optfut_df.pivot_table(index='Date', columns='Commodity', values='Total Value (Lacs)', aggfunc='sum', fill_value=0)
    optfut_pivot = optfut_pivot.add_prefix('OPTFUT_')
    
    # Merge pivoted OPTFUT data
    final_df = pd.merge(final_df, optfut_pivot, on='Date', how='left')
    
    # --- 6. Define Column Lists for Checksums and Final Order ---
    # These are all potential columns based on your request
    FUTCOM_COLS = [f'FUTCOM_{c.upper()}' for c in ['ALUMINIUM', 'ALUMINI', 'CARDAMOM', 'COPPER', 'COTTON', 'COTTONCNDY', 'COTTONOIL', 'CRUDEOIL', 'CRUDEOILM', 'GOLD', 'GOLDM', 'GOLDGUINEA', 'GOLDPETAL', 'KAPAS', 'LEAD', 'LEADMINI', 'MENTHAOIL', 'NATURALGAS', 'NICKEL', 'SILVER', 'SILVERM', 'SILVERMIC', 'STEELREBAR', 'ZINC', 'ZINCMINI']]
    OPTFUT_COLS = [f'OPTFUT_{c.upper()}' for c in ['COPPER', 'CRUDEOIL', 'GOLD', 'GOLDM', 'NATURALGAS', 'NICKEL', 'SILVER', 'SILVERM', 'ZINC']]
    
    # Ensure all potential columns exist in the DataFrame, fill with 0 if not traded on that day
    for col in FUTCOM_COLS + OPTFUT_COLS + ['FUTCOM LKH', 'FUTIDX LKH', 'OPTFUT LKH']:
        if col not in final_df.columns:
            final_df[col] = 0

    # --- 7. Calculate Checksums and Other Summary Columns ---
    final_df['FUTCOM_Checksum'] = final_df[FUTCOM_COLS].sum(axis=1)
    final_df['OPTFUT_Checksum'] = final_df[OPTFUT_COLS].sum(axis=1)

    # Calculate totals in Crores
    final_df['FUTCOM_Cr'] = final_df['FUTCOM LKH'] / 100
    final_df['FUTIDX_Cr'] = final_df['FUTIDX LKH'] / 100
    final_df['OPTFUT_Cr'] = final_df['OPTFUT LKH'] / 100

    # Calculate grand totals
    final_df['Total_Value_Lakhs'] = final_df['FUTCOM LKH'] + final_df['FUTIDX LKH'] + final_df['OPTFUT LKH']
    final_df['Total_Value_Cr'] = final_df['Total_Value_Lakhs'] / 100

    # --- 8. Set Final Column Order ---
    final_column_order = [
        'Date', 'Year', 'FUTCOM LKH', 'FUTIDX LKH', 'OPTFUT LKH'
    ] + FUTCOM_COLS + [
        'FUTCOM_Checksum'
    ] + OPTFUT_COLS + [
        'OPTFUT_Checksum', 'FUTCOM_Cr', 'FUTIDX_Cr', 'OPTFUT_Cr',
        'Total_Value_Lakhs', 'Total_Value_Cr'
    ]
    
    # Filter to only include columns that were actually created
    final_column_order_existing = [col for col in final_column_order if col in final_df.columns]
    
    final_df = final_df[final_column_order_existing]
    
    # Format date for readability in CSV
    final_df['Date'] = final_df['Date'].dt.strftime('%d-%m-%Y')
    
    return final_df

# --- Main Execution ---
if __name__ == "__main__":
    start_date = {'day': '01', 'month': '10', 'year': '2025'}
    end_date = {'day': '20', 'month': '10', 'year': '2025'}

    raw_df = scrape_mcx_data(start_date, end_date)

    if raw_df is not None:
        wide_df = transform_data_to_wide_format(raw_df)
        
        # Save the transformed data to the specified CSV format
        output_filename = "mcx_historical_data_wide_format.csv"
        wide_df.to_csv(output_filename, index=False)
        
        print(f"\nSuccessfully transformed and saved data to {output_filename}")
        print("\n--- Final Data Preview ---")
        print(wide_df.head())
    else:
        print("\nNo data was scraped, so no file was created.")