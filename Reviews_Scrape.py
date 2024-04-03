import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver import Edge
from selenium.webdriver.common.by import By
import os

def scrape_reviews(airport_list):
    data = []
    
    for _, airport in airport_list.iterrows():
        airport_link = airport['Link']
        
        # Open the airport link in the browser
        driver = Edge()
        driver.get(airport_link)
        
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
        
        # Find the div element with class "stars"
        stars_div = soup.find('div', class_='stars')
        
        # Extract the review rating from the title attribute
        if stars_div:
            review_rating = stars_div.get('title')
            airport_data = {'Airport': airport['Name'], 'Review Rating': review_rating}
            data.append(airport_data)
            print(airport_data)  # Print the data on the terminal
        
        # Close the browser
        driver.quit()
    
    return data

# Get the list of CSV files in the "country_aeroportes" directory
folder_path = "country_aeroportes" #country_aeroportes
csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

# Store the review data
review_data = []

for csv_file in csv_files:
    # Read the CSV file into a DataFrame
    airport_list = pd.read_csv(os.path.join(folder_path, csv_file), header=None, names=["Name", "Link"])

    # Call the function to scrape reviews for the airport_list
    review_data.extend(scrape_reviews(airport_list))

# Write the review data to a text file
with open('reviews_data.txt', 'w', encoding='utf-8') as file:
    for data in review_data:
        file.write(str(data) + '\n')

print("Review data saved to 'reviews_data.txt'")