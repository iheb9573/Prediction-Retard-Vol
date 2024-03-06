import time
import csv
import pandas as pd
from bs4 import BeautifulSoup
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver import Edge
from selenium.webdriver.common.by import By
from datetime import datetime
import os

def get_date_with_year(data):
    current_year = datetime.now().year
    
    numeros_lignes = []
    days = []
    
    # Convert data list to DataFrame
    data_df = pd.DataFrame(data)
    
    for index, row in data_df.iterrows():
        if row[0].split()[0] in ["Monday,", "Tuesday,", "Wednesday,", "Thursday,", "Friday,", "Saturday,", "Sunday,"]:
            numeros_lignes.append(index)
            days.append(row[0] + " " + str(current_year))  # Append current year
    
    # Add 'date' column to the DataFrame
    data_df['date'] = ''
    
    for i in range(len(numeros_lignes) - 1):
        data_df.iloc[numeros_lignes[i] + 1:numeros_lignes[i + 1] + 1, -1] = days[i]
    
    data_df.iloc[numeros_lignes[-1] + 1:-1, -1] = days[-1]
    
    return data_df

# Define the function to click the "Load earlier flights" button until it disappears or after a certain number of attempts
def click_load_earlier_until_disappear(driver, max_attempts=10):
    for _ in range(max_attempts):
        try:
            button = driver.find_element(By.XPATH, '//button[contains(@class, "btn-flights-load") and contains(text(), "Load earlier flights")]')
            button.click()
            time.sleep(2)  # Adjust the sleep time according to your needs
        except ElementNotInteractableException:
            break

# Define the function to click the "Load later flights" button until it disappears or after a certain number of attempts
def click_load_later_until_disappear(driver, max_attempts=10):
    for _ in range(max_attempts):
        try:
            button = driver.find_element(By.XPATH, '//button[contains(@class, "btn-flights-load") and contains(text(), "Load later flights")]')
            button.click()
            time.sleep(2)  # Adjust the sleep time according to your needs
        except ElementNotInteractableException:
            break

def scrape_departures(airport_list, num_rows=None):
    data = []
    
    # Determine the number of rows to iterate based on the provided parameter
    if num_rows is not None:
        airport_list = airport_list.head(num_rows)
    
    # Loop through the specified number of rows in the airport list
    for _, airport in airport_list.iterrows():
        # Append '/departures' to the link
        departure_link = airport['Link'] + '/departures'
        
        # Open the departure link in browser
        driver = Edge()
        driver.get(departure_link)
        
        time.sleep(4)

        # Wait for the button to appear
        button = driver.find_element(By.ID, 'onetrust-accept-btn-handler')

        # Click on the button
        button.click()

        time.sleep(3)
        
        # Call the function to repeat the process 20 times for loading earlier flights
        click_load_earlier_until_disappear(driver)
        
        # Call the function to repeat the process 20 times for loading later flights
        click_load_later_until_disappear(driver)

        # Get the HTML content of the page after loading all flights
        html = driver.page_source
        
        # Parse the HTML content
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find the table element
        table = soup.find('table', class_='table table-condensed table-hover data-table m-n-t-15')
        
        # Extract data from the table
        if table:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if cells:
                    # Ensure consistent structure of each row
                    row_data = [cell.text.strip() for cell in cells]
                    # If row has fewer fields, add empty strings to match the expected number of fields
                    if len(row_data) < 7:
                        row_data.extend([''] * (7 - len(row_data)))
                    data.append(row_data)
        
        # Close the browser
        driver.quit()

    return data

# Get the list of CSV files in the "country_aeroportes" directory
folder_path = "country_aeroportes"
csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

# Create the "country_depart" directory ifelle n'existe pas déjà
output_folder = "country_depart"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Loop through the CSV files in the "country_aeroportes" folder and scrape departures for each country separately
for csv_file in csv_files:
    # Read the CSV file into a DataFrame
    airport_list = pd.read_csv(os.path.join(folder_path, csv_file), header=None, names=["Name", "Link"])

    # Specify the number of rows to scrape (if desired), or leave it as None to scrape all rows
    num_rows_to_scrape = 5  # Change this to the desired number of rows, or set it to None to scrape all rows "None"

    # Call the function with the airport_list and the specified number of rows
    airport_data = scrape_departures(airport_list, num_rows_to_scrape)

    # Get the date with year for the scraped data
    airport_data_with_date = get_date_with_year(airport_data)

    # Construct the output file path
    output_csv_file = os.path.join(output_folder, f"output_{csv_file}")

    # Write the extracted data to a CSV file using pandas DataFrame's to_csv() method
    airport_data_with_date.to_csv(output_csv_file, index=False, header=None)

    # Print a message indicating the CSV file has been created
    print(f"CSV file '{output_csv_file}' created successfully.")