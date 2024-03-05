import csv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType



#
# Définition du schéma
schema = StructType([
    StructField("PredictedDeparture", StringType(), False),
    StructField("Flight_ID", StringType(), False),
    StructField("Destination", StringType(), False),
    StructField("Airline", StringType(), False),
    StructField("Aircraft_ID", StringType(), False),
    StructField("Null", StringType(), False),
    StructField("TimeOfDeparture", StringType(), False),
    StructField("Date", StringType(), False)
])

# Fonction pour nettoyer le fichier CSV
def clean_csv(file_path):
    cleaned_rows = []
    
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        
        for row in reader:
            # Vérifier si la ligne est vide ou contient uniquement des entêtes
            if row and any(row):
                cleaned_rows.append(row)
    
    # Écrire les lignes nettoyées dans un nouveau fichier
    with open('cleaned_file.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(cleaned_rows)

    # Créer une session Spark
    spark = SparkSession.builder.getOrCreate()

    # Lire le fichier nettoyé en tant que DataFrame
    df = spark.read.csv('cleaned_file.csv', schema=schema, header=False)
    # Supprimer la colonne "Null"
    df = df.drop("Null")
    # Supprimer les lignes contenant des valeurs nulles
    df = df.na.drop(how="any", thresh=2)

    # Afficher le contenu du DataFrame
    df.show(100)


# Appel de la fonction de nettoyage du fichier CSV
clean_csv('outputa.csv')

