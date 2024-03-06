import time
import csv
from bs4 import BeautifulSoup
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver import Edge
from selenium.webdriver.common.by import By

# Définition de l'URL
url = 'https://www.flightradar24.com/data/airports'
driver = Edge()
driver.get(url)

# Attente de l'apparition du bouton
time.sleep(3)
button = driver.find_element(By.ID, 'onetrust-accept-btn-handler')

# Clic sur le bouton
button.click()

time.sleep(3)

# Récupération du contenu HTML
html = driver.page_source

# Analyse du contenu HTML
soup = BeautifulSoup(html, 'html.parser')

# Initialisation d'une liste pour stocker les noms et liens des aéroports
td_tags = soup.find_all('td')
a_tags = [td.find('a') for td in td_tags if td.find('a')]

airport_list = []

# Extraction des noms et liens d'aéroports correspondant au modèle demandé
for a_tag in a_tags:
    href = a_tag.get('href', '')  # Récupération de l'attribut href
    if href.startswith("https://www.flightradar24.com/data/airports/"):
        country_name = a_tag.get_text(strip=True)  # Récupération du texte sans espaces supplémentaires
        airport_list.append([country_name, href])  # Ajout à la liste sous forme de liste

# Fermeture du navigateur
driver.quit()

# Définition du chemin du fichier CSV
csv_file = 'liste_aeroport_TM.csv'

# Écriture de la liste des aéroports dans un fichier CSV sans spécifier les noms de champ
with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    
    # Écriture des données de chaque aéroport
    for airport in airport_list:
        writer.writerow(airport)

# Ouvrir le fichier CSV en lecture
with open('liste_aeroport_TM.csv', 'r') as file:
    # Lire le contenu du fichier CSV
    csv_reader = csv.reader(file)
    # Créer une liste pour stocker les lignes filtrées
    filtered_rows = []
    # Parcourir chaque ligne du fichier CSV
    for row in csv_reader:
        # Vérifier si le nom du pays est présent dans la ligne
        if row[0]:
            # Ajouter la ligne filtrée à la liste
            filtered_rows.append(row)

# Ouvrir le fichier CSV en écriture
with open('liste_aeroport_TM.csv', 'w', newline='') as file:
    # Écrire les lignes filtrées dans le fichier CSV
    csv_writer = csv.writer(file)
    csv_writer.writerows(filtered_rows)

# Affichage d'un message indiquant la création du fichier CSV
print("Fichier CSV créé avec succès.")

print("Nom du fichier CSV:", csv_file)