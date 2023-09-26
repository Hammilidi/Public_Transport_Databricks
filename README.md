# Public_Transport_Databricks
Ce projet vise à collecter, transformer et gérer les données de transport public en utilisant les services Azure tels que Azure Data Lake Storage Gen2 et Azure Databricks.


Avec l'urbanisation croissante et la demande de mobilité, il est essentiel d'avoir une vue claire des opérations de transport. Les données, allant des horaires des bus aux retards, peuvent offrir des perspectives pour améliorer les services et répondre aux besoins des citoyens.

En tant que developpeur Data, le professionnel en charge de cette situation est sollicité pour mettre en place des solutions basées sur les données pour répondre à ces défis. Cela implique:

​

Conception de l'Architecture Data Lake: Créer un espace de noms hiérarchique dans Azure Data Lake Storage Gen2.

`/public_transport_data/raw/` : pour les données CSV brutes.
`/public_transport_data/processed/` : pour les données traitées.
Intégration de l'Infrastructure Data Lake: Utiliser Azure Databricks avec PySpark pour lire et structurer les données.

​

Processus ETL avec Azure Databricks: En utilisant PySpark , transformer les données, notamment les transformations de date, les calculs de temps, et les analyses de retards, de passagers et d'itinéraires. Voici qlq transformations que vous pouvez envisager :

Transformations de Date: Extraire l'année, le mois, le jour et le jour de la semaine de la date pour faciliter les requêtes et les rapports.

Calculs Temporels: Calculer la durée de chaque voyage en soustrayant l'heure de départ de l'heure d'arrivée.

Analyse des Retards: Catégoriser les retards en groupes tels que 'Pas de Retard', 'Retard Court' (1-10 minutes), 'Retard Moyen' (11-20 minutes) et 'Long Retard' (>20 minutes).

Analyse des Passagers: Identifier les heures de pointe et hors pointe en fonction du nombre de passagers.

Analyse des Itinéraires: Calculer le retard moyen, le nombre moyen de passagers et le nombre total de voyages pour chaque itinéraire.

​

Documentez les transformations, les sources de données et l'utilisation dans un catalogue : La documentation est essentielle pour la gouvernance des données et pour que les utilisateurs comprennent les données. Voici ce que vous devez faire :

Description des Données: Pour chaque ensemble de données, fournir une brève description.

Transformations: Documenter toutes les transformations appliquées aux données brutes.

Lignage des Données: Indiquer la source des données, comme "Données provenant de [Nom de l'Agence de Transport] et traitées à l'aide d'Azure Databricks."

Directives d'Utilisation: Fournir des directives sur la manière d'utiliser les données, les cas d'utilisation potentiels et toutes précautions.

​

Automatisation des Politiques de Conservation : Les politiques de conservation des données garantissent que vous ne stockez que les données pertinentes et conformes à toutes les exigences réglementaires. Planifiez un cahier pour qu'il s'exécute à intervalles réguliers (par exemple, mensuellement) afin d'appliquer les politiques de conservation suivant :

Archivage des Données: Utilisez Databricks pour identifier les anciennes données, puis déplacez-les vers un répertoire ou un conteneur d'archive.

Suppression des Données: Si vous n'avez pas besoin de conserver d'anciennes données, vous pouvez les supprimer.

​

Génération de données à intervalles de lots (Batch Intervals) à l'aide du script joint à ce brief : Créez plusieurs données chacune dans un fichier csv, chaque fichier agira comme de nouvelles données à venir pour une période spécifique de la journée .

​

Batch Processing : Une fois les données collectées dans la journée, elles doivent être traitées selon le Job de traitement automatisé et les nouveaux fichiers seront enregistrés dans le dossier traité dans datalake. Créer un job qui exécute le notebook de Processing . Ce Job peut être planifié pour s'exécuter à des intervalles spécifiques, par exemple quotidiens ou hebdomadaires.

Surveillez les performances et les journaux du job pour vous assurer que le traitement par lots se déroule correctement.


