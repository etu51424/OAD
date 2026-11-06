# Olist — Bronze et Silver

Importer chaque .py comme notebook Databricks. Exécuter les 11 Bronze puis les
11 Silver. Chaque fichier est autonome et requiert spark/display de Databricks.
Pas d'exécution Python locale Windows.

Catalogue : workspace. Schémas : bronze / silver.
Volume par défaut : /Volumes/workspace/default/premierdatasetolist/.
Vérifier les noms et emplacements des CSV, surtout les fichiers marketing.
Adapter SOURCE dans les notebooks Bronze si nécessaire. CSV non fournis.

Bronze conserve les données en texte avec des métadonnées de provenance.
Silver nettoie les espaces/valeurs vides, convertit les nombres et dates,
conserve les codes postaux en texte et les valeurs converties dans *_raw.
Les lignes invalides restent dans les tables <nom>_rejects pour correction.
Seuls les doublons exacts sont supprimés ; plusieurs positions géographiques
par code postal sont conservées. Les règles métier et relations entre tables
restent à compléter selon le cours.

ATTENTION : chaque exécution remplace sa table cible (overwrite).
Chargements complets initiaux, sans traitement incrémental.
Droits nécessaires : lecture du volume et création des schémas/tables.
Les notebooks customers.py et products.py déjà présents dans Bronze sont
conservés, avec leur code de lecture original.
