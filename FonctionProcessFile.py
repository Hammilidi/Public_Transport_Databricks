import os
import time

raw_csv_files = [f.path for f in raw_files if f.name.endswith(".csv")]

# Définissez le nombre de fichiers à traiter à chaque itération
batch_size = 2
count = 1
while raw_csv_files:
    # Prenez les prochains fichiers à traiter dans la liste
    batch_files = raw_csv_files[:batch_size]
    
    # Mettez à jour la liste des fichiers restants
    raw_csv_files = raw_csv_files[batch_size:]
    
    # Boucle sur les fichiers pour effectuer les transformations sur chaque fichier
    for csv_file in batch_files:
        
        # Chargez le fichier CSV brut dans un DataFrame
        raw_data = spark.read.format('csv').option('header', True).option('delimiter', ',').load(csv_file)
        
        analytical_data = process_file(csv_file,count)
        count = count + 1
        print(analytical_data)
        # Extrayez le nom du fichier (data2.csv) à partir du chemin complet
        filename = os.path.basename(csv_file)
        # print(filename)
        # Enregistrez le DataFrame transformé dans un répertoire de données traitées avec le nom de fichier
        analytical_data.write.mode("overwrite").option("header", "true").csv(processed_path + "analyse/" + filename)


    # Attendez 15 secondes avant de traiter le prochain lot de fichiers
    print("Attendez 15 secondes avant de traiter le prochain lot de fichiers !")
    time.sleep(15)

# Fermez la session Spark
# spark.stop()
