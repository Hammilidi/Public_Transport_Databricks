from pyspark.sql import SparkSession

# Créez la session Spark
spark = SparkSession.builder \
    .appName("PublicTransport") \
    .getOrCreate()

# Spécifiez le chemin du répertoire raw dans Azure Data Lake Storage Gen2
raw_data_path = "abfss://datalake@fidelistockage.dfs.core.windows.net/raw/"

# Utilisez dbutils.fs.ls pour lister les fichiers CSV bruts
raw_files = dbutils.fs.ls(raw_data_path)
raw_csv_files = [f.path for f in raw_files if f.name.endswith(".csv")]

# Boucle sur les fichiers pour effectuer les transformations sur chaque fichier
for csv_file in raw_csv_files:
    # Chargez le fichier CSV brut dans un DataFrame
    raw_data = spark.read.format('csv').option('header', True).option('delimiter', ',').load(csv_file)

    # Transformations de Date: Extraire l'année, le mois, le jour et le jour de la semaine de la date pour faciliter les requêtes et les rapports.


    from pyspark.sql.functions import year, month, dayofmonth, dayofweek

    transformed_data = raw_data.withColumn("Annee", year("Date")) \
                           .withColumn("Mois", month("Date")) \
                           .withColumn("Jour", dayofmonth("Date"))

    # Catégorisation des retards

    from pyspark.sql.functions import when

# Utilisez la fonction when pour définir les conditions de catégorisation des retards
    transformed_data = transformed_data.withColumn("CatégorieRetard",
        when(transformed_data["Delay"] <= 0, "Pas de Retard")
        .when((transformed_data["Delay"] >= 1) & (transformed_data["Delay"] <= 10), "Retard Court")
        .when((transformed_data["Delay"] >= 11) & (transformed_data["Delay"] <= 20), "Retard Moyen")
        .when(transformed_data["Delay"] > 20, "Long Retard")
        .otherwise("Autre")  # Si aucune des conditions précédentes n'est satisfaite
)

# Détermination des heures de pointe

    from pyspark.sql.functions import when

# Seuil pour le nombre de passagers pour considérer une heure de pointe
    seuil_passagers_pointe = 75

# Utilisez la fonction when pour définir la colonne "HeureDePointe" en fonction du seuil
    transformed_data = transformed_data.withColumn("HeureDePointe",
        when(transformed_data["Passengers"] > seuil_passagers_pointe, "Oui")
        .otherwise("Non"))
    
    # Analyse des itinéraires

    from pyspark.sql import functions as F

# Groupez les données par "Itinéraire"
    transformed_data = transformed_data.groupBy("Route").agg(
        F.mean("Delay").alias("RetardMoyen"),
        F.mean("Passengers").alias("NombreMoyenPassagers"),
        F.count("*").alias("NombreTotalVoyages"))


    # display(transformed_data)


    # Enregistrez le DataFrame transformé dans un répertoire de données traitées
    transformed_data.write.mode("overwrite").csv("abfss://datalake@fidelistockage.dfs.core.windows.net/processed/transformed_data.csv")

# Fermez la session Spark
    spark.stop()
