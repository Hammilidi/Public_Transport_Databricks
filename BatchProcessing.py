import time
from pyspark.sql import SparkSession

# Créez la session Spark
spark = SparkSession.builder \
    .appName("PublicTransport") \
    .getOrCreate()


# Utilisez dbutils.fs.ls pour lister les fichiers CSV bruts
raw_files = dbutils.fs.ls(raw_data_path)
raw_csv_files = [f.path for f in raw_files if f.name.endswith(".csv")]

# Définissez le nombre de fichiers à traiter à chaque itération
batch_size = 2

while raw_csv_files:
    # Prenez les prochains fichiers à traiter dans la liste
    batch_files = raw_csv_files[:batch_size]
    
    # Mettez à jour la liste des fichiers restants
    raw_csv_files = raw_csv_files[batch_size:]
    
    # Boucle sur les fichiers pour effectuer les transformations sur chaque fichier
    for csv_file in batch_files:
        # Chargez le fichier CSV brut dans un DataFrame
        raw_data = spark.read.format('csv').option('header', True).option('delimiter', ',').load(csv_file)

        transformed_data = process_file(csv_file)
        # Enregistrez le DataFrame transformé dans un répertoire de données traitées
        transformed_data.write.mode("overwrite").csv("abfss://datalake@fidelistockage.dfs.core.windows.net/processed/transformed_data_" + csv_file)

    # Attendez 5 minutes avant de traiter le prochain lot de fichiers
    print("Attendez 5 minutes avant de traiter le prochain lot de fichiers !")
    time.sleep(300)

# Fermez la session Spark
spark.stop()
