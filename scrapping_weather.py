import time
import os
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver import Edge
from selenium.webdriver.common.by import By
from confluent_kafka import Producer


def scrape_weather(airport_list, producer, topic):
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

        # Find the date element
        date_element = soup.find('tr', class_='master expandable')
        if date_element:
            date_element = date_element.find_all('td')[1]
            date = date_element.text.strip()

        # Find the table element
        table = soup.find_all('tr', class_='slave')

        # Extract data from the table
        data = []
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
                    # Append the row data to the data list
                    row_data['Date'] = date
                    data.append(row_data)

        # Convert the data list to a pandas DataFrame
        data_df = pd.DataFrame(data)

        # Create the output directory if it doesn't exist
        output_folder = "output"
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        # Save the DataFrame to a CSV file
        output_csv_file = os.path.join(output_folder, f"weather_{airport['Name']}.csv")
        data_df.to_csv(output_csv_file, index=False)

        # Print a message indicating that the CSV file has been created
        print(f"CSV file '{output_csv_file}' created successfully.")

        # Produce the message to Kafka
        message = data_df.to_string(index=False)
        producer.produce(topic, message.encode('utf-8'))

        # Close the browser
        driver.quit()


# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic = 'weather_topic'

# Create a Kafka producer
producer = Producer({'bootstrap.servers': bootstrap_servers})

# Get the list of CSV files in the "country_aeroportes" directory
folder_path = "country_aeroportes"
csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

# Loop through the CSV files in the "country_aeroportes" folder and scrape weather for each country separately
for csv_file in csv_files:
    # Read the CSV file into a DataFrame
    airport_list = pd.read_csv(os.path.join(folder_path, csv_file), header=None, names=["Name", "Link"])

    # Call the function to scrape weather for the airport_list and produce the data to Kafka
    scrape_weather(airport_list, producer, topic)

    # Print a message indicating that the data has been produced to Kafka
    print(f"Data produced to Kafka topic '{topic}' for CSV file '{csv_file}'.")

# Flush and close the Kafka producer
producer.flush()
producer.close()