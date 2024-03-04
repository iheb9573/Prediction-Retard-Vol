import time
import csv
from bs4 import BeautifulSoup
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver import Edge
from selenium.webdriver.common.by import By

# Replace YOUR-PATH-TO-CHROMEDRIVER with your Edge driver location
driver = Edge()

driver.get('https://www.flightradar24.com/data/airports/united-states')

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

# Find all <a> tags within <td> tags
a_tags = soup.find_all('a')

# Initialize a list to store airport names and links
airport_list = []


# Extract airport names and links that match the pattern and append to the list
for a_tag in a_tags:
    href = a_tag.get('href', '')  # Extract the href attribute
    if href.startswith("https://www.flightradar24.com/data/airports/"):
        airport_name = a_tag.get_text(strip=True)  # Get text without extra spaces and newlines
        airport_list.append([airport_name, href])  # Append as list instead of dictionary

# Close the browser
driver.quit()


# Define the CSV file path
csv_file = 'airport_list.csv'

# Write the airport list to a CSV file without specifying fieldnames
with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    
    # Write each airport's data
    for airport in airport_list:
        writer.writerow(airport)

# Print a message indicating the CSV file has been created
print("CSV file created successfully.")