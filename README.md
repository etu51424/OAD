# OAD — Analyse des canaux d’acquisition Olist

Le notebook principal contient le cadrage et les cellules exploratoires exportées de Databricks.
Les 11 modules du dossier imports ont été reconstitués à partir des chemins CSV de Bronze.
L’archive DBC fournie contenait uniquement les dossiers d’import.

Les notebooks Bronze chargent les CSV depuis /Volumes/workspace/default/premierdatasetolist/.
Exécuter Bronze avant Silver. Ces chargements complets remplacent les tables cibles.
Silver écrit les lignes invalides dans les tables suffixées par _rejects.

Gold, l’orchestration et le rapport Power BI restent à réaliser.
