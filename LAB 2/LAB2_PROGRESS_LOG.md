# LAB 2 Progress Log

## 2026-02-22 - Etape 1 Cadrage consignes

Objectif:
- analyser les consignes du Lab 2 et etablir une methodologie pas a pas avec livrables.

Actions:
- lecture de `docs/Lab2_dbtDuckDbETL.pdf`;
- extraction des exigences techniques et metier;
- formalisation du plan d'execution par etapes.

Resultat:
- document de cadrage cree: `LAB2_STEP_BY_STEP_PLAN.md`.

Prochaine etape (a valider):
- Etape 2: setup outillage et verification versions (`duckdb`, `dbt-core`, `dbt-duckdb`, `dbt --version`).

## 2026-02-22 - Controle fichier par fichier de l'espace LAB 2

Objectif:
- verifier le contenu de `LAB 2` et expliquer le role des fichiers observes.

Actions:
- inventaire des fichiers a la racine de `LAB 2`;
- verification du contenu de `docs/`;
- inspection des fichiers `.dist-info` observes dans `.venv_lab2`.

Resultat:
- contenu valide, pas d'erreur de structure detectee;
- confirmation que `INSTALLER`, `METADATA`, `WHEEL`, `RECORD` sont des fichiers standards generes par `pip` dans un environnement virtuel.

Prochaine etape:
- attendre validation utilisateur puis lancer l'Etape 2.

## 2026-02-22 - Etape 2 Setup outillage et verification versions

Objectif:
- verifier que l'environnement Lab 2 est operationnel avec les outils demandes.

Actions:
- verification de la version Python dans `.venv_lab2`;
- verification de `pip`;
- verification des packages installes: `duckdb`, `dbt-core`, `dbt-duckdb`;
- verification de la commande version dbt via module Python.

Resultats:
- Python: `3.10.11`
- pip: `26.0.1`
- duckdb: `1.4.4`
- dbt-core: `1.11.4`
- dbt-duckdb: `1.10.0`
- plugin `duckdb` detecte dans la sortie version dbt.

Note technique:
- la commande `dbt.exe --version` ne renvoie pas de sortie dans cet environnement;
- la commande fiable utilisee est:
  - `python -m dbt.cli.main --version`

Conclusion:
- prerequis outillage valides pour continuer le Lab 2.

Prochaine etape:
- Etape 3: initialisation du projet dbt et structure des dossiers.

## 2026-02-22 - Etape 3 Initialisation projet dbt et arborescence

Objectif:
- initialiser un projet dbt Lab 2 et preparer la structure de travail.

Actions:
- execution de `python -m dbt.cli.main init lab2_playstore --skip-profile-setup --profiles-dir .`;
- creation des dossiers:
  - `lab2_playstore/data/raw`
  - `lab2_playstore/data/warehouse`
  - `lab2_playstore/models/staging`
  - `lab2_playstore/models/marts`

Resultats:
- projet dbt cree: `lab2_playstore`;
- fichiers standards dbt presents (`dbt_project.yml`, `README.md`, dossiers `analyses`, `macros`, `tests`, etc.);
- structure cible de travail en place pour les prochaines etapes.

Prochaine etape:
- Etape 4: configuration `profiles.yml` + `dbt_project.yml` + sanity check DuckDB.

## 2026-02-22 - Etape 4 Configuration dbt + sanity check DuckDB

Objectif:
- connecter proprement dbt a DuckDB et valider l'execution d'un premier modele.

Actions:
- creation de `lab2_playstore/profiles.yml` avec cible DuckDB locale:
  - path: `data/warehouse/lab2_playstore.duckdb`
  - threads: `4`
- mise a jour de `lab2_playstore/dbt_project.yml`:
  - `models/staging` -> materialized `view`
  - `models/marts` -> materialized `table`
- creation du modele de verification:
  - `lab2_playstore/models/staging/stg_sanity_check.sql`
  - contenu: `select 1 as ok, current_timestamp as created_at`
- execution de:
  - `dbt debug` (via `python -m dbt.cli.main debug ...`)
  - `dbt run -s stg_sanity_check`
- verification de la base DuckDB et lecture de la vue creee.

Resultats:
- `dbt debug`: OK (connexion validee);
- `dbt run -s stg_sanity_check`: PASS;
- base creee: `lab2_playstore/data/warehouse/lab2_playstore.duckdb`;
- table/vue presente: `stg_sanity_check` avec donnee de test.

Nettoyage:
- suppression du dossier `lab2_playstore/models/example` (boilerplate dbt non utile au projet).

Note:
- warning normal temporaire sur `models.lab2_playstore.marts` tant qu'aucun modele mart n'est encore implemente.

Prochaine etape:
- Etape 5: design Kimball (business process, grain, dimensions, facts, bus matrix).

## 2026-02-22 - Etape 5 Design Kimball

Objectif:
- definir formellement le modele analytique cible avant implementation SQL.

Actions:
- inspection rapide des datasets bruts Lab 1 (apps/reviews) pour confirmer les champs exploitables;
- definition du business process principal (publication de review);
- declaration du grain de la fact table;
- definition des dimensions (apps, dates, reviewers optionnelle);
- definition des faits/mesures et des aggregations;
- creation de la bus matrix;
- formalisation du star schema cible et de la validation analytique.

Livrable cree:
- `lab2_playstore/docs/kimball_design.md`

Resultats:
- design dimensionnel valide et exploitable pour la suite dbt;
- mapping clair vers les futurs modeles staging et marts.

Prochaine etape:
- Etape 6: implementation du staging layer (raw readers + stg models propres).

## 2026-02-22 - Etape 6 Implementation du staging layer

Objectif:
- rendre les JSONL de Lab 1 requetables dans dbt et produire 2 modeles staging propres au bon grain.

Actions:
- copie des fichiers bruts vers le projet dbt:
  - `lab2_playstore/data/raw/apps.jsonl`
  - `lab2_playstore/data/raw/reviews.jsonl`
- profilage des types avec DuckDB (`describe select * from read_json_auto(...)`) pour verifier les casts;
- creation des modeles d'ingestion brute:
  - `lab2_playstore/models/staging/stg_playstore_apps_raw.sql`
    - role: exposer le JSONL apps en relation SQL sans logique metier;
  - `lab2_playstore/models/staging/stg_playstore_reviews_raw.sql`
    - role: exposer le JSONL reviews en relation SQL sans logique metier;
- creation des modeles staging propres:
  - `lab2_playstore/models/staging/stg_playstore_apps.sql`
    - role: standardiser les noms (`appId` -> `app_id`, etc.), typer les champs, filtrer les cles nulles, aplatir `categories`, creer `app_sk`;
  - `lab2_playstore/models/staging/stg_playstore_reviews.sql`
    - role: standardiser les noms, typer score/date, nettoyage minimal (`trim` du texte), filtrer nulls et scores hors plage, creer `review_sk` et `app_sk`;
- execution de `dbt run` sur la couche staging:
  - commande: `python -m dbt.cli.main run --profiles-dir . --project-dir . --select staging`
- controles post-run dans DuckDB pour verifier volume et grain:
  - comptages raw vs staging;
  - nullite/duplication des cles;
  - plage de `rating_score`.

Resultats:
- `dbt run` staging: PASS (`5/5` modeles);
- volumes:
  - `stg_playstore_apps_raw`: `30`
  - `stg_playstore_apps`: `30`
  - `stg_playstore_reviews_raw`: `18898`
  - `stg_playstore_reviews`: `18898`
- controle qualite:
  - `app_id` null dans apps: `0`
  - duplication `app_id` dans apps: `0`
  - `review_id` null dans reviews: `0`
  - duplication `review_id` dans reviews: `0`
  - scores hors plage [1..5]: `0`

Conclusion:
- la couche staging est en place et respecte le grain defini.
- les modeles sont prets pour l'etape de tests dbt (`schema.yml` + `dbt test`).

Prochaine etape:
- Etape 7: definition des tests schema (`not_null`, `unique`, `relationships`, range`) puis execution `dbt test`.

## 2026-02-22 - Etape 7 Tests schema et validation qualite staging

Objectif:
- verifier que les modeles staging sont fiables avant la construction des marts.

Actions:
- creation du fichier de tests schema:
  - `lab2_playstore/models/staging/schema.yml`
- tests generiques appliques:
  - `not_null` et `unique` sur les cles (`app_id`, `app_sk`, `review_id`, `review_sk`);
  - `relationships` entre reviews et apps (`app_id`, `app_sk`);
  - test de plage sur `rating_score` via `accepted_values` (`1..5`);
- ajout de tests SQL personnalises:
  - `lab2_playstore/tests/stg_playstore_apps_store_score_range.sql`
    - verifie que `store_score`, quand present, reste dans `[0,5]`;
  - `lab2_playstore/tests/stg_playstore_reviews_thumbs_up_non_negative.sql`
    - verifie que `thumbs_up_count` n'est jamais negatif.

Incident observe et correction:
- premier run `dbt test`:
  - `17 PASS / 1 FAIL` sur `not_null` de `store_score`;
  - cause: 1 application sans score store (`com.inoid.notter.ai.note.taker`), cas source legitime;
- correction:
  - suppression du test `not_null` sur `store_score`;
  - remplacement par un controle de plage tolerant les `NULL`.

Execution finale:
- commande executee:
  - `python -m dbt.cli.main test --profiles-dir . --project-dir .`
- resultat final:
  - `PASS=19`, `ERROR=0`, `WARN=0`.

Conclusion:
- la couche staging est validee techniquement;
- les contraintes de structure, d'integrite relationnelle et de qualite de valeurs sont en place.

Prochaine etape:
- Etape 8: implementation des marts (dimensions + fact table) selon le design Kimball.

## 2026-02-22 - Etape 8 Implementation des marts (schema etoile)

Objectif:
- materialiser le schema etoile cible (dimensions + fact table) a partir du staging valide.

Actions:
- creation des modeles marts:
  - `lab2_playstore/models/marts/dim_apps.sql`
  - `lab2_playstore/models/marts/dim_dates.sql`
  - `lab2_playstore/models/marts/dim_reviewers.sql`
  - `lab2_playstore/models/marts/fct_playstore_reviews.sql`
- creation des tests marts:
  - `lab2_playstore/models/marts/schema.yml`
- execution des commandes:
  - `python -m dbt.cli.main run --profiles-dir . --project-dir . --select marts`
  - `python -m dbt.cli.main test --profiles-dir . --project-dir . --select marts`

Incident observe et correction:
- premier run de tests marts:
  - `24 PASS / 2 FAIL`
  - echec `unique_dim_reviewers_reviewer_sk` + `unique_fct_playstore_reviews_review_id`;
- cause:
  - collisions de `reviewer_sk` dues aux variations de casse des noms (`Sachin Kumar` vs `sachin Kumar`);
  - duplication de lignes dans la fact via jointure reviewer non strictement dedoublonnee;
- correction appliquee:
  - normalisation en minuscule + dedoublonnage dans `dim_reviewers`;
  - calcul explicite de `reviewer_sk` dans la fact puis jointure sur cette cle;
- relance `dbt run` + `dbt test` sur marts.

Resultats finaux:
- `dbt run --select marts`: PASS (`4/4`);
- `dbt test --select marts`: PASS (`26/26`);
- volumes des tables:
  - `dim_apps`: `30`
  - `dim_dates`: `736`
  - `dim_reviewers`: `18271`
  - `fct_playstore_reviews`: `18898`
- controle grain fact:
  - `count(*) = count(distinct review_id) = 18898` (grain respecte);
- exemple de KPI derive:
  - part de low ratings (`score <= 2`): `27.76%`.

Conclusion:
- schema etoile operationnel et valide par tests;
- dimensions/fact coherentes avec la bus matrix et les besoins analytiques.

Prochaine etape:
- Etape 9: validation finale des exigences Lab 2 + documentation de livraison (README/rapport).

## 2026-02-22 - Etape 9 Documentation finale et README unique

Objectif:
- centraliser la livraison documentaire dans un README principal unique et ajouter des figures Lab 2.

Actions:
- mise a jour du README principal:
  - `LAB1_Python-DATAPipeline-/README.md`
  - ajout d'une section complete Lab 2 (objectif, structure, methodologie, resultats, commandes, tracabilite);
- generation de figures Lab 2 a partir des marts DuckDB:
  - `LAB1_Python-DATAPipeline-/data/processed/lab2_figure_1_top_apps.png`
  - `LAB1_Python-DATAPipeline-/data/processed/lab2_figure_2_monthly_trend.png`
- export des donnees de figure:
  - `LAB1_Python-DATAPipeline-/data/processed/lab2_top_apps_summary.csv`
  - `LAB1_Python-DATAPipeline-/data/processed/lab2_monthly_trend.csv`
- transformation du README dbt en pointeur vers le README principal:
  - `LAB 2/lab2_playstore/README.md`

Resultats:
- un seul README de reference pour le rendu Labs 1 + 2;
- redaction harmonisee en mode collectif ("nous");
- figures integrees avec liens directs dans le README principal;
- traces detaillees conservees dans les logs Lab 1 / Lab 2.

Conclusion:
- la livraison documentaire est prete pour la phase rapport demandee par le professeur.

Prochaine etape:
- preparation du rapport final selon le format exact que vous nous donnerez.
