# Lab 2 - Methodologie Pas a Pas

## Principe de travail
Nous avancerons en etapes courtes.
A la fin de chaque etape:
- nous validons les preuves de resultat,
- nous notons ce qui a ete fait,
- puis nous passons a l'etape suivante.

## Etape 1 - Cadrage (faite)

### Ce que nous devons livrer (selon la consigne)

1) Environnement dbt + DuckDB
- dbt-core, dbt-duckdb, duckdb installes
- verification `dbt --version` (plugin duckdb visible)

2) Modelisation analytique (Kimball)
- business process
- grain declare
- dimensions identifiees
- facts identifies (mesures + agregations)
- bus matrix
- star schema valide par les besoins analytiques

3) Projet dbt configure
- projet dbt initialise
- profils DuckDB configures
- structure propre des dossiers:
  - models/staging
  - models/marts
  - data/raw
  - data/warehouse
- conventions de materialisation:
  - staging en views
  - marts en tables

4) Sanity check dbt
- petit modele test (ex: `select 1 as ok`)
- `dbt run` OK
- verification que la base DuckDB est creee

5) Staging layer
- 2 modeles bruts lisant JSONL via DuckDB JSON reader
- 2 modeles staging propres:
  - `stg_playstore_apps`
  - `stg_playstore_reviews`
- renommage, cast, nettoyage minimal, respect du grain
- pas de logique business a ce stade

6) Tests dbt
- tests schema (`not_null`, `unique`, `relationships`, ranges)
- execution `dbt test`
- resultat documente

7) Marts (schema etoile)
- dimensions + fact table(s) issues de la bus matrix
- verification des jointures et du grain
- verification que les questions analytiques sont couvertes

## Ordre d'execution recommande

Etape 2: Setup outillage et verification versions
Etape 3: Initialisation projet dbt + arborescence
Etape 4: Configuration profiles/dbt_project + sanity check
Etape 5: Design Kimball (process, grain, dimensions, facts, bus matrix)
Etape 6: Implementation staging
Etape 7: Tests staging
Etape 8: Implementation marts
Etape 9: Validation finale + documentation

## Critere de passage a l'etape 2
- Vous validez ce cadrage.
