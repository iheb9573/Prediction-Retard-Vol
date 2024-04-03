import time
import os
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver import Edge
from selenium.webdriver.common.by import By


def scrape_weather(airport_list):
    data = []
    
    for _, airport in airport_list.iterrows():
        weather_link = airport['Link'] + '/weather'
        
        # Open the weather link in browser
        driver = Edge()
        driver.get(weather_link)
        
        time.sleep(3)
        
        # Wait for the button to appear
        button = driver.find_element(By.ID, 'onetrust-accept-btn-handler')
        
        # Click on the button
        button.click()
        
        time.sleep(3)
        
        # Get the HTML content of the page
        html = driver.page_source
        
        # Parse the HTML content
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find the table element
        table = soup.find_all('tr', class_='slave')
        
        # Extract data from the table
        if table:
            for row in table:
                ul_elements = row.find_all('ul')
                for ul in ul_elements:
                    li_elements = ul.find_all('li')
                    row_data = {}
                    for li in li_elements:
                        text = li.text.strip()
                        key, value = text.split(':', 1)
                        row_data[key.strip()] = value.strip()
                    data.append(row_data)
        
        # Find the date element
        date_element = soup.find('tr', class_='master expandable')
        if date_element:
            date_element = date_element.find_all('td')[1]
            date = date_element.text.strip()
        
            # Add the date to each row in the data list
            for row in data:
                row['Date'] = date
        
        # Close the browser
        driver.quit()
    
    return data


# Get the list of CSV files in the "country_aeroportes" directory
folder_path = "country_aeroportes"
csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

# Create the "country_weather" directory if it doesn't already exist
output_folder = "country_weather"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Loop through the CSV files in the "country_aeroportes" folder and scrape weather for each country separately
for csv_file in csv_files:
    # Read the CSV file into a DataFrame
    airport_list = pd.read_csv(os.path.join(folder_path, csv_file), header=None, names=["Name", "Link"])

    # Call the function to scrape weather for the airport_list
    weather_data = scrape_weather(airport_list)

    # Construct the output file path
    output_csv_file = os.path.join(output_folder, f"weather_{csv_file}")

    # Write the extracted data to a CSV file using pandas DataFrame's to_csv() method
    pd.DataFrame(weather_data).to_csv(output_csv_file, index=False)

    # Print a message indicating the CSV file has been created
    print(f"CSV file '{output_csv_file}' created successfully.")