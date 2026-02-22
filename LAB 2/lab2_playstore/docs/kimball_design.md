# Lab 2 - Kimball Design (Business Process, Grain, Dimensions, Facts)

## 1) Contexte analytique

Nous analysons les avis utilisateurs des applications "AI note-taking" depuis Google Play.
Le besoin metier principal est de suivre la perception utilisateur par application et dans le temps.

Questions analytiques cibles:
- Quelles applications performent le mieux / le moins bien selon les avis?
- Les notes utilisateur s'ameliorent-elles ou se degradent-elles dans le temps?
- Le volume d'avis est-il tres different selon les applications?
- (extension) Peut-on reperer des contradictions texte/note?

## 2) Etape Kimball 1 - Business process

**Business process retenu:** publication d'un avis utilisateur sur une application.

Pourquoi ce choix:
- c'est l'evenement metier le plus frequent et mesurable;
- il porte directement les mesures utiles (score, thumbs up, contenu);
- il permet les analyses par application, par date et par qualite de feedback.

## 3) Etape Kimball 2 - Grain

**Grain declare:**

"Une ligne dans la table de faits represente un avis unique (`review_id`) publie pour une application (`app_id`) a un instant (`review_ts`)."

Implication:
- pas d'agregation dans la fact table;
- toute metrique analytiques (moyennes, volumes, taux) se calcule en aval via `group by`.

## 4) Etape Kimball 3 - Dimensions

### 4.1 `dim_apps`

Role metier:
- decrire le "quoi" et le "qui" de l'evenement (application, editeur, categorie).

Attributs proposes:
- `app_sk` (surrogate key)
- `app_id` (business key)
- `app_name`
- `developer_name`
- `genre`
- `price`
- `store_score`
- `ratings_count`
- `installs`

Source:
- dataset apps (raw apps jsonl).

### 4.2 `dim_dates`

Role metier:
- decrire le "quand" pour l'analyse temporelle.

Attributs proposes:
- `date_sk` (surrogate key, format YYYYMMDD)
- `date_day`
- `year`
- `quarter`
- `month`
- `week_of_year`
- `day_of_month`
- `day_of_week`
- `is_weekend`

Source:
- derive de `reviews.at`.

### 4.3 `dim_reviewers` (optionnelle mais prevue)

Role metier:
- decrire "qui a ecrit" (niveau utilisateur, si exploitable).

Attributs proposes:
- `reviewer_sk` (surrogate key)
- `user_name`

Source:
- `reviews.userName`.

Note:
- pas d'identifiant utilisateur stable fourni par la source, donc dimension potentiellement bruitée.
- cette dimension reste optionnelle dans l'implementation finale si la qualite est insuffisante.

## 5) Etape Kimball 4 - Facts (mesures)

### 5.1 Fact table principale: `fct_playstore_reviews`

Mesures et indicateurs derives:
- `review_count` (constante 1 par ligne, aggregation: SUM)
- `rating_score` (aggregation: AVG, distribution, pct low score)
- `thumbs_up_count` (aggregation: SUM / AVG)
- `review_text_length` (derive, aggregation: AVG)
- `low_rating_flag` (derive: score <= 2, aggregation: SUM / AVG)

Clés de jointure:
- `app_sk` -> `dim_apps`
- `date_sk` -> `dim_dates`
- `reviewer_sk` -> `dim_reviewers` (si activée)

Degenerate dimension gardee dans la fact:
- `review_id`

## 6) Bus Matrix

| Business Process | dim_apps | dim_dates | dim_reviewers |
| --- | --- | --- | --- |
| App review event (`fct_playstore_reviews`) | X | X | X (optionnel) |

Interpretation:
- un seul process central couvre les besoins analytiques du Lab;
- toutes les analyses demandees sont portees par la fact reviews + dimensions apps/date.

## 7) Star schema propose

- `dim_apps(app_sk, app_id, app_name, developer_name, genre, price, store_score, ratings_count, installs, ...)`
- `dim_dates(date_sk, date_day, year, quarter, month, week_of_year, day_of_month, day_of_week, is_weekend)`
- `dim_reviewers(reviewer_sk, user_name)` (optionnelle)
- `fct_playstore_reviews(review_id, app_sk, date_sk, reviewer_sk?, rating_score, thumbs_up_count, review_text_length, low_rating_flag, review_ts)`

Cardinalites:
- `dim_apps` 1 -> N `fct_playstore_reviews`
- `dim_dates` 1 -> N `fct_playstore_reviews`
- `dim_reviewers` 1 -> N `fct_playstore_reviews` (si utilisee)

## 8) Validation contre les besoins analytiques

Couverture des questions:
- best/worst apps: `AVG(rating_score)` et `SUM(review_count)` par `dim_apps`;
- tendance temporelle: `AVG(rating_score)` et `SUM(review_count)` par `dim_dates`;
- ecarts de volume: `SUM(review_count)` par `dim_apps`;
- low ratings: `AVG(low_rating_flag)` par app/date.

Verification du grain:
- chaque jointure vers les dimensions est many-to-one depuis la fact;
- aucune duplication de mesure si les business keys sont dedoublonnees en staging.

## 9) Mapping implementation dbt (prochaine etape)

Staging attendu:
- `stg_playstore_apps_raw` / `stg_playstore_reviews_raw` (lecture JSONL)
- `stg_playstore_apps` / `stg_playstore_reviews` (noms/types/cleaning)

Marts attendus:
- `dim_apps`
- `dim_dates`
- `fct_playstore_reviews`
- `dim_reviewers` (optionnelle)

Tests cibles:
- `not_null` + `unique` sur business keys de dimensions
- `relationships` fact -> dimensions
- test de range sur `rating_score` (1..5)

## 10) Decision de design

Nous retenons un schema etoile centre sur la fact reviews car:
- il est simple a maintenir;
- il repond directement aux besoins du Lab;
- il reste extensible pour les prochains besoins metier.
