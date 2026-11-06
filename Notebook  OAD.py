# Databricks notebook source
# MAGIC %md
# MAGIC # Quels canaux d'acquisition recrutent les vendeurs les plus performants ?
# MAGIC
# MAGIC **Travail Business Intelligence** - Auteur : Corentin Demeure, Nicolas Carion, Simon Sekri
# MAGIC
# MAGIC
# MAGIC # Introduction
# MAGIC **Architecture** : Bronze → Silver → Gold → Power BI
# MAGIC
# MAGIC | Section | Contenu |
# MAGIC |---|---|
# MAGIC | 0 | Paramètres et configuration |
# MAGIC | 1 | Contexte business |
# MAGIC | 2 | Sources de données |
# MAGIC | 3 | Architecture et data model |
# MAGIC | 4 | Layer Bronze (données brutes) |
# MAGIC | 5 | Layer Silver (nettoyage + historisation) |
# MAGIC | 6 | Layer Gold (modèle en étoile pour le reporting) |
# MAGIC | 7 | Chargements multiples et reload d'une table de fait |
# MAGIC | 8 | Orchestration (Databricks Jobs) |
# MAGIC | 9 | Contrôles qualité |
# MAGIC | 10 | Requêtes pour Power BI |
# MAGIC | 11 | Limites et pistes d'amélioration |
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paramètres et configuration
# MAGIC  
# MAGIC Paramètres communs à tous les notebooks du Job : catalogue, schémas par layer, identifiant du chargement (`load_id`).
# MAGIC

# COMMAND ----------

# À FAIRE (syllabus ch.6 §7 et ch.7 §7) :
# - load_id : identifiant du lot, fourni par le Job à chaque exécution.
#   Il sert à tracer, rejouer et dédupliquer (un lot rejoué ne doit rien doubler).
# - mode : "incremental" ou "reload" (le reload ne vise qu'une table de fait, voir section 7).
# - Noms du catalogue et des schémas bronze / silver / gold : à définir ici une seule fois,
#   puis réutilisés dans tous les notebooks.
# TODO : code
# dbutils.widgets.text("load_id", "1")
# CATALOG = "..."
# BRONZE, SILVER, GOLD = "bronze", "silver", "gold"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Contexte business
# MAGIC  
# MAGIC ### Problématique
# MAGIC Quels canaux d'acquisition génèrent les vendeurs les plus performants ?
# MAGIC  
# MAGIC ### Question business
# MAGIC Faut-il investir davantage dans certaines origines de leads pour recruter des vendeurs qui génèrent ensuite davantage d'activité ?
# MAGIC  
# MAGIC ### Questions métier
# MAGIC 1. Quels vendeurs sont les plus **actifs** ? (dates et fréquence des ventes)
# MAGIC 2. Quels vendeurs ont le plus gros **chiffre d'affaires** ? (prix de vente × volume dans les commandes)
# MAGIC  
# MAGIC ### Définition de « performant »
# MAGIC Croisement entre un vendeur **actif** et un **CA élevé**.
# MAGIC  
# MAGIC ### Indicateurs à produire
# MAGIC - Taux de conversion des leads (par origine)
# MAGIC - Délai entre le premier contact et la signature
# MAGIC - Nombre de vendeurs recrutés (par origine)
# MAGIC - Volume de commandes et CA associés après recrutement
# MAGIC - Répartition des vendeurs par `activity_level` et par origine
# MAGIC  
# MAGIC ### Limites de l'analyse
# MAGIC - Plus de commandes ne veut pas dire plus de bénéfices.
# MAGIC - Résultats historiques : corrélation, pas causalité.
# MAGIC - Détail des limites en section 11.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC La suite est juste une série d'exercice pour apprendre databrics fait par panda data

# COMMAND ----------

# MAGIC %md
# MAGIC - Supprimer les doublons
# MAGIC - Vérifier les valeurs manquante
# MAGIC - Calculer
# MAGIC - Créer un graphique

# COMMAND ----------

# MAGIC %md
# MAGIC **Attentions au doublon**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC *Analyse des données*
# MAGIC

# COMMAND ----------

# Commantaire
print("Hello World")

# COMMAND ----------

data = [("Alice", 25, "1998-06-12"), ("Bob", 30, "1995-07-02")]
df = spark.createDataFrame(data, ["Nom", "Age", "DateNaissance"])
df.show()

# COMMAND ----------

path = "/Volumes/workspace/default/premierdatasetolist/olist_customers_dataset.csv"
df = (spark.read
  .option("header", True)
  .option("inferSchema", True)
  .csv(path))
df.show(10)

# COMMAND ----------

df.count()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports de toutes les tables
# MAGIC

# COMMAND ----------

# Dataset 1 — Brazilian E-Commerce
from imports.import_dataset_olist.customers import load_customers
from imports.import_dataset_olist.orders import load_orders
from imports.import_dataset_olist.order_items import load_order_items
from imports.import_dataset_olist.products import load_products
from imports.import_dataset_olist.sellers import load_sellers
from imports.import_dataset_olist.payments import load_payments
from imports.import_dataset_olist.reviews import load_reviews
from imports.import_dataset_olist.geolocation import load_geolocation
from imports.import_dataset_olist.category_translation import load_category_translation

# Dataset 2 — Marketing Funnel
from imports.import_marketing_funnel_olist.marketing_qualified_leads import load_marketing_qualified_leads
from imports.import_marketing_funnel_olist.closed_deals import load_closed_deals


# COMMAND ----------

# Reload both modules in your main notebook
# Because Python caches imported modules, execute:
import importlib

from imports.import_marketing_funnel_olist import marketing_qualified_leads
from imports.import_marketing_funnel_olist import closed_deals

importlib.reload(marketing_qualified_leads)
importlib.reload(closed_deals)

load_marketing_qualified_leads = marketing_qualified_leads.load_marketing_qualified_leads
load_closed_deals = closed_deals.load_closed_deals


# COMMAND ----------

# MAGIC %md
# MAGIC ##  Charger les dataframes

# COMMAND ----------

# load all dataframes# Dataset 1

df_customers = load_customers(spark)
df_orders = load_orders(spark)
df_order_items = load_order_items(spark)
df_products = load_products(spark)
df_sellers = load_sellers(spark)
df_payments = load_payments(spark)
df_reviews = load_reviews(spark)
df_geolocation = load_geolocation(spark)
df_category_translation = load_category_translation(spark)

# Dataset 2
df_marketing_qualified_leads = load_marketing_qualified_leads(spark)
df_closed_deals = load_closed_deals(spark)

print("All 11 DataFrames initialized successfully.")

# COMMAND ----------

# check that every dataFrame can actually be read
datasets = {
    "customers": df_customers,
    "orders": df_orders,
    "order_items": df_order_items,
    "products": df_products,
    "sellers": df_sellers,
    "payments": df_payments,
    "reviews": df_reviews,
    "geolocation": df_geolocation,
    "category_translation": df_category_translation,
    "marketing_qualified_leads": df_marketing_qualified_leads,
    "closed_deals": df_closed_deals
}

for name, df in datasets.items():
    try:
        print(f"✓ {name}: {df.count()} rows")
    except Exception as e:
        print(f"✗ {name}: FAILED — {str(e)[:200]}")

# COMMAND ----------

display(df_closed_deals)

# COMMAND ----------

# MAGIC %md
# MAGIC ## tests, recréation des dataframes si échec. optionel

# COMMAND ----------

import inspect

print(inspect.getsource(load_marketing_qualified_leads))
print(inspect.getsource(load_closed_deals))

# COMMAND ----------

# Recreate DataFrames using the corrected functions
df_marketing_qualified_leads = load_marketing_qualified_leads(spark)
df_closed_deals = load_closed_deals(spark)

print("DataFrames recreated.")

# COMMAND ----------

print("Marketing leads:", df_marketing_qualified_leads.count())
print("Closed deals:", df_closed_deals.count())

display(df_marketing_qualified_leads)
display(df_closed_deals)

# COMMAND ----------

# MAGIC %md
# MAGIC