# Lab 1 - Python-only Data Pipeline

Abderrazzak OUTZOULA & Badrdine Saadioui

Dans ce rendu, nous présentons le pipeline que nous avons construit pour le Lab 1, en suivant les trois parties du PDF:
- A. Setup de l'environnement
- B. Pipeline end-to-end (ingestion -> transformation -> serving -> dashboard)
- C. Changements de pipeline et stress testing

## 1) Structure du projet

```text
data/
  raw/                # fichiers bruts (JSONL + CSV de stress test)
  processed/          # tables transformées, KPI, rapports qualité et stress
src/
  ingest.py           # extraction Google Play (apps + reviews) vers JSONL
  transform.py        # normalisation des schémas et nettoyage
  serve.py            # KPI app/jour + KPI contradiction sentiment/note
  dashboard.py        # génération du dashboard
  stress_test.py      # exécution automatique de la partie C
README.md             # documentation principale
Lab1_PythonDataPipeline.pdf
```

## 2) Partie A - Environnement

Nous avons travaillé avec Python 3.7+ dans un environnement virtuel dédié.

```powershell
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install --upgrade pip
python -m pip install google-play-scraper pandas matplotlib
```

## 3) Partie B - Pipeline end-to-end

### 3.1 Ingestion

```powershell
python src/ingest.py
```

Sorties produites:
- `data/raw/apps.jsonl`
- `data/raw/reviews.jsonl`

Dans cette étape, nous gardons les données brutes telles quelles (sans transformation), avec récupération paginée des reviews.

### 3.2 Transformation

```powershell
python src/transform.py
```

Problèmes de données brutes que nous avons traités:
1. Champs imbriqués et non tabulaires.
2. Types incohérents (nombres stockés en texte, tokens null, etc.).
3. Clés manquantes ou noms de colonnes variables selon la source.
4. Formats de date multiples et parfois invalides.
5. Doublons d'identifiants (`appId`, `reviewId`).
6. Reviews qui référencent des applications absentes du catalogue.
7. Évolution du schéma (schema drift), surtout sur les reviews.

Sorties transformées demandées par le PDF:
- `data/processed/apps.csv` avec  
  `appId, title, developer, score, ratings, installs, genre, price`
- `data/processed/reviews.csv` avec  
  `app_id, app_name, reviewId, userName, score, content, thumbsUpCount, at`

Sortie qualité ajoutée:
- `data/processed/quality_report.json`

### 3.3 Serving layer

```powershell
python src/serve.py
```

Sorties de serving demandées:
- `data/processed/app_kpis.csv`
- `data/processed/daily_kpis.csv`

Sortie supplémentaire pour la question métier de la partie C:
- `data/processed/sentiment_mismatch_kpis.csv`

### 3.4 Dashboard

```powershell
python src/dashboard.py
```

Sortie:
- `data/processed/dashboard.png`

Ce que montre le dashboard (en 2-3 phrases):  
Nous y comparons les apps selon le volume d'avis et la note moyenne, ce qui permet d'identifier rapidement les apps qui performent bien ou mal.  
Nous visualisons aussi l'évolution quotidienne des notes pour repérer une amélioration ou une dégradation de la satisfaction utilisateur.  
Enfin, des graphiques complémentaires montrent les écarts d'engagement entre applications.

## 4) Partie C - Stress testing

Nous avons automatisé les scénarios de la partie C avec:

```powershell
python src/stress_test.py
```

Rapport généré:
- `data/processed/stress_test_report.md`

### 4.1 Fichiers CSV de stress test



### 4.2 Résultat global de notre dernière exécution

Les 4 scénarios sont passés avec le statut `ok` dans `data/processed/stress_test_report.md`.

### 4.3 Réponses aux questions de la partie C (comme demandé)

1. New Reviews Batch
- Changements de code: limités et localisés (`src/transform.py`, `src/serve.py`, `src/stress_test.py`).
- Full refresh: oui, explicite (les sorties sont régénérées à chaque run).
- Doublons: détection et suppression par `reviewId` (`5` doublons sur notre run).
- Apps inconnues: reviews conservées, app potentiellement non résolue, cas comptés (`5` sur notre run).

2. Schema Drift in Reviews
- Dépendance aux colonnes: centralisée dans `normalize_review_row` (`src/transform.py`).
- Échec explicite/silencieux: avec notre mapping actuel, le scénario est stable (`ok`).
- Étendue des changements: principalement locale à la transformation.

3. Dirty and Inconsistent Data Records
- Notes/dates invalides: converties en null pour éviter des agrégations incorrectes.
- Gestion des lignes problématiques: on conserve les lignes, on neutralise les champs invalides, on trace l'impact.
- Visibilité qualité: `quality_report.json` remonte les anomalies (`23` scores invalides, `17` timestamps invalides sur notre run).

4. Updated Applications Metadata
- Doublons d'apps: les doublons `appId` sont ignorés (`4` sur notre run).
- Jointure reviews/apps: jointure clé `app_id`; les ids inconnus sont conservés et comptés.
- Impact aval: visible dans les métriques (ex: `1187` app ids inconnus dans ce scénario).

5. New Business Logic Stress Test (consumer-driven change)
- Suffisance des sorties initiales: non, il fallait ajouter une logique dédiée.
- Ajout réalisé: `data/processed/sentiment_mismatch_kpis.csv` via `src/serve.py`.
- Positionnement de la logique: dérivation de feature + agrégation au niveau serving.
- Impact architecture: changements surtout sur `transform` et `serve` (dashboard optionnel).

## 5) Reproductibilité

- Nous ne modifions jamais les fichiers bruts pendant transform/serve/stress.
- Le pipeline fonctionne en full refresh.
- Les indicateurs qualité sont tracés dans `data/processed/quality_report.json`.

## 6) Commandes de reproduction

```powershell
python src/ingest.py
python src/transform.py
python src/serve.py
python src/dashboard.py
python src/stress_test.py
```

## Feedback (addressed)

- Reviews ingestion now paginates and appends in batches for safer collection.
- KPIs are computed via pandas groupby aggregations for simpler, faster code.
- A dashboard screenshot is included in the README.
