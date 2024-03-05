import pandas as pd
import mysql.connector

# Lecture du fichier CSV
df = pd.read_csv('data.csv')

# Remplacer les valeurs NaN par une chaîne vide
df = df.fillna('')

# Connexion à la base de données MySQL
cnx = mysql.connector.connect(user='root', password='',
                              host='localhost', database='flight_data')

# Création d'un curseur pour exécuter des requêtes SQL
cursor = cnx.cursor()

# Insertion des données dans la table MySQL
for row in df.itertuples(index=False):
    sql = "INSERT INTO data_vols (PredictedDeparture, Flight_ID, Destination, Airline, Aircraft_ID, TimeOfDeparture, Date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    values = tuple(row)
    cursor.execute(sql, values)

# Validation des changements dans la base de données
cnx.commit()

# Fermeture de la connexion à la base de données
cursor.close()
cnx.close()