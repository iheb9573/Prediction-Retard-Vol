import csv

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