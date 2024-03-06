import os
import time
import csv
from bs4 import BeautifulSoup
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver import Edge
from selenium.webdriver.common.by import By

# Fonction pour extraire les aéroports d'un pays et les enregistrer dans un fichier CSV
def scrape_airports(country_name, url):
    # Replace YOUR-PATH-TO-CHROMEDRIVER with your Edge driver location
    driver = Edge()

    driver.get(url)

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

    # Create the directory if it doesn't exist
    if not os.path.exists('country_aeroportes'):
        os.makedirs('country_aeroportes')

    # Define the CSV file path
    csv_file = f"country_aeroportes/airport_liste_{country_name}.csv"

    # Write the airport list to a CSV file without specifying fieldnames
    with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)

        # Write each airport's data
        for airport in airport_list:
            writer.writerow(airport)

    # Print a message indicating the CSV file has been created
    print(f"CSV file for {country_name} created successfully.")


# Lecture du fichier contenant les noms de pays et les liens
with open('liste_aeroport_TM.csv', 'r', encoding='utf-8') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row

    # Parcours de chaque ligne du fichier
    for row in reader:
        country_name = row[0]
        url = row[1]

        # Appel de la fonction de scraping pour chaque pays
        scrape_airports(country_name, url)