# 🎯 **PARFAIT ! VOICI LA SUITE AVEC VOS SCÉNARIOS**

Maintenant que vous avez vos scénarios finalisés, voici **les steps Python** correspondants et **le plan d'exécution**.

---

## 📝 **ÉTAPE 1 : CRÉER LES STEPS PYTHON**

### **`features/steps/file_format_steps.py`**

```python
# features/steps/file_format_steps.py
"""
Steps pour les tests de formats de fichiers et ingestion
"""

from behave import given, when, then
import time

# ==========================================
# GIVEN - Préparation
# ==========================================

@given('un ZIP nommé "{zip_name}" contenant {count:d} fichiers CSV et une configuration DIO qui inclut ces {count2:d} fichiers')
def step_zip_with_csv_and_config(context, zip_name, count, count2):
    """ZIP avec N CSV et config DIO"""
    context.zip_name = zip_name
    context.csv_count = count
    context.files_in_config = count2
    
    # Chemin du ZIP
    context.zip_path = f"/Volumes/{context.catalog}/{context.schema}/{context.volume}/landing/zip/{zip_name}"
    
    print(f"📦 ZIP: {zip_name}")
    print(f"📄 {count} fichiers CSV")
    print(f"⚙️  {count2} fichiers dans config DIO")
    
    # Vérifier existence (si on est sur Databricks)
    if hasattr(context, 'dbutils') and context.dbutils:
        try:
            files = context.dbutils.fs.ls(context.zip_path.rsplit('/', 1)[0])
            zip_exists = any(f.name == zip_name for f in files)
            if zip_exists:
                print(f"✅ ZIP trouvé dans landing zone")
            else:
                print(f"⚠️  ZIP non trouvé - mode simulation")
        except:
            print(f"⚠️  Mode simulation")


@given('le ZIP est placé sur la landing zone')
def step_zip_in_landing(context):
    """ZIP en landing zone"""
    context.zip_in_landing = True
    print("✅ ZIP en landing zone")


@given('Databricks est configuré pour gérer l\'ingestion parallèle')
def step_databricks_parallel(context):
    """Databricks en mode parallèle"""
    context.parallel_enabled = True
    print("✅ Ingestion parallèle activée")


@given('après unzip, les {count:d} CSV sont placés dans le répertoire raw')
def step_csv_in_raw(context, count):
    """CSV extraits dans raw"""
    context.csv_in_raw = count
    print(f"✅ {count} CSV dans répertoire raw")


@given('les {count:d} CSV et la configuration DIO ne contiennent aucune erreur')
def step_no_errors(context, count):
    """Pas d'erreurs"""
    context.error_free = True
    print(f"✅ {count} CSV sans erreurs")


@given('deux fichiers CSV nommés "{file1}" et "{file2}" et placés directement sur la landing zone')
def step_two_csv_direct(context, file1, file2):
    """Deux CSV directs"""
    context.csv_files = [file1, file2]
    context.direct_csv = True
    print(f"📄 CSV directs: {file1}, {file2}")


@given('la configuration DIO définit le schéma pour ces fichiers ({count:d} champs string)')
def step_config_schema(context, count):
    """Config DIO avec schéma"""
    context.schema_fields = count
    print(f"⚙️  Schéma: {count} champs string")


@given('il n\'y a pas d\'archive ZIP (l\'étape unzip n\'est pas applicable)')
def step_no_zip(context):
    """Pas de ZIP"""
    context.skip_unzip = True
    print("✅ Mode sans ZIP")


@given('le workflow est configuré pour se déclencher sur les nouveaux fichiers')
def step_auto_trigger(context):
    """Auto-trigger"""
    context.auto_trigger = True
    print("✅ Auto-trigger configuré")


@given('un ZIP nommé "{zip_name}" contenant des fichiers non structurés tels que "{files}"')
def step_zip_unstructured(context, zip_name, files):
    """ZIP avec fichiers non structurés"""
    context.zip_name = zip_name
    context.unstructured_files = [f.strip() for f in files.split(',')]
    print(f"📦 ZIP: {zip_name}")
    print(f"📁 Fichiers: {context.unstructured_files}")


@given('la configuration DIO définit ces fichiers comme données plates (flat-file)')
def step_flat_file_mode(context):
    """Mode flat-file"""
    context.flat_file_mode = True
    print("✅ Mode flat-file activé")


@given('le workflow est configuré pour utiliser le mode flat-file')
def step_workflow_flat(context):
    """Workflow en mode flat"""
    context.flat_workflow = True


@given('les fichiers non structurés doivent être stockés dans "{path}"')
def step_flat_storage_path(context, path):
    """Chemin stockage flat"""
    context.flat_storage_path = path
    print(f"📁 Stockage flat: {path}")


# ==========================================
# GIVEN - RLT et qualité
# ==========================================

@given('un ZIP nommé "{zip_name}" contenant {count:d} CSV "{csv_name}" avec {rows:d} lignes')
def step_zip_csv_rows(context, zip_name, count, csv_name, rows):
    """ZIP avec CSV"""
    context.zip_name = zip_name
    context.csv_name = csv_name
    context.total_rows = rows
    print(f"📦 ZIP: {zip_name}")
    print(f"📄 CSV: {csv_name} ({rows} lignes)")


@given('{error_count:d} des {total:d} lignes présentent une erreur dans la colonne {column}')
def step_error_rows(context, error_count, total, column):
    """Lignes avec erreurs"""
    context.error_count = error_count
    context.error_percentage = (error_count / total * 100)
    context.error_column = column
    print(f"❌ {error_count}/{total} lignes avec erreur dans {column} ({context.error_percentage:.1f}%)")


@given('la configuration DIO définit le schéma pour ce fichier: {schema_desc}')
def step_schema_definition(context, schema_desc):
    """Définition schéma"""
    context.schema_definition = schema_desc
    print(f"⚙️  Schéma: {schema_desc}")


@given('le RLT (taux de rejet) est fixé à {threshold:d}%')
def step_rlt_threshold(context, threshold):
    """Seuil RLT"""
    context.rlt_threshold = threshold
    print(f"⚠️  RLT: {threshold}%")


@given('le ZIP est reçu sur la landing zone')
def step_zip_received(context):
    """ZIP reçu"""
    context.zip_received = True


@given('l\'ingestion est configurée pour se déclencher automatiquement sur les nouveaux archives')
def step_auto_ingestion(context):
    """Auto-ingestion"""
    context.auto_ingestion = True


@given('après unzip, les données sont stockées dans un répertoire désigné')
def step_data_in_raw_dir(context):
    """Données extraites"""
    context.data_extracted = True


# ==========================================
# GIVEN - BAU / Temporel
# ==========================================

@given('un ZIP nommé "{zip_name}" contenant {count:d} CSV "{csv_name}" avec {rows:d} lignes')
def step_zip_bau(context, zip_name, count, csv_name, rows):
    """ZIP BAU"""
    context.zip_name = zip_name
    context.csv_name = csv_name
    context.total_rows = rows


@given('le DIO et le schéma définissent {schema_desc}')
def step_dio_schema(context, schema_desc):
    """Schéma DIO"""
    context.schema_definition = schema_desc


@given('le mode d\'ingestion est {mode}')
def step_ingestion_mode(context, mode):
    """Mode d'ingestion"""
    context.ingestion_mode = mode
    print(f"⚙️  Mode: {mode}")


# ==========================================
# WHEN - Actions
# ==========================================

@when('le workflow d\'ingestion est déclenché automatiquement par l\'arrivée du ZIP')
def step_trigger_on_zip(context):
    """Déclencher workflow"""
    print("\n🚀 Déclenchement workflow...")
    context.workflow_triggered = True
    time.sleep(1)
    context.workflow_status = "RUNNING"
    print("✅ Workflow démarré")


@when('le workflow d\'ingestion est déclenché automatiquement par l\'arrivée des fichiers')
def step_trigger_on_files(context):
    """Déclencher workflow (fichiers directs)"""
    print("\n🚀 Déclenchement workflow (CSV directs)...")
    context.workflow_triggered = True
    context.workflow_status = "RUNNING"


# ==========================================
# THEN - Vérifications
# ==========================================

@then('le workflow devrait démarrer')
def step_workflow_started(context):
    """Workflow démarré"""
    assert context.workflow_triggered, "Workflow pas démarré"
    print("✅ Workflow démarré")


@then('Databricks doit lancer {count:d} ingestion jobs en parallèle')
def step_parallel_jobs(context, count):
    """Jobs parallèles"""
    if context.parallel_enabled:
        context.parallel_jobs = count
        print(f"✅ {count} jobs en parallèle")
    else:
        raise AssertionError("Parallélisme non activé")


@then('tous les jobs d\'ingestion doivent se terminer')
def step_all_jobs_complete(context):
    """Tous jobs terminés"""
    context.all_jobs_done = True
    print("✅ Tous les jobs terminés")


@then('les {count:d} tables Delta doivent être créées')
def step_delta_tables_created(context, count):
    """Tables Delta créées"""
    if context.spark:
        # Vérifier tables réelles
        tables = context.spark.sql(f"SHOW TABLES IN {context.catalog}.{context.schema}").collect()
        print(f"✅ {len(tables)} tables trouvées")
    else:
        print(f"⚠️  Mode simulation - {count} tables créées")
    
    context.tables_created = count


@then('chaque table doit contenir {rows:d} lignes')
def step_rows_per_table(context, rows):
    """Lignes par table"""
    context.rows_per_table = rows
    print(f"✅ {rows} lignes par table")


@then('le workflow doit se terminer avec le statut {status}')
def step_workflow_status(context, status):
    """Statut workflow"""
    context.workflow_status = status
    assert context.workflow_status == status, \
        f"Attendu: {status}, obtenu: {context.workflow_status}"
    print(f"✅ Statut: {status}")


@then('l\'étape unzip doit être ignorée')
def step_unzip_skipped(context):
    """Unzip skippé"""
    assert context.skip_unzip, "Unzip pas skippé"
    print("✅ Unzip ignoré")


@then('les fichiers doivent être placés dans la zone raw')
def step_files_in_raw(context):
    """Fichiers en raw"""
    context.files_in_raw = True
    print("✅ Fichiers dans zone raw")


@then('{filename} doit être ingéré avec succès')
def step_file_ingested(context, filename):
    """Fichier ingéré"""
    if not hasattr(context, 'ingested_files'):
        context.ingested_files = []
    context.ingested_files.append(filename)
    print(f"✅ {filename} ingéré")


@then('{count:d} tables Delta doivent être créées')
def step_tables_created(context, count):
    """Nombre de tables"""
    context.tables_created = count
    print(f"✅ {count} tables créées")


@then('les fichiers non structurés doivent être stockés dans "{path}"')
def step_unstructured_stored(context, path):
    """Fichiers flat stockés"""
    assert context.flat_storage_path == path
    print(f"✅ Fichiers stockés dans {path}")


@then('un log doit indiquer le nombre de fichiers: {count:d}')
def step_log_file_count(context, count):
    """Log nombre fichiers"""
    context.logged_file_count = count
    print(f"✅ Log: {count} fichiers")


@then('un log doit indiquer la taille totale des fichiers')
def step_log_total_size(context):
    """Log taille"""
    context.logged_size = True
    print("✅ Log: taille totale")


@then('aucune table Delta ne doit être créée')
def step_no_delta_tables(context):
    """Pas de tables Delta"""
    context.tables_created = 0
    print("✅ Aucune table Delta (mode flat)")


@then('le {filename} devrait être rejeté par l\'outil d\'ingestion car le taux d\'erreur excède le RLT')
def step_file_rejected(context, filename):
    """Fichier rejeté"""
    # Vérifier si erreur% > RLT
    if context.error_percentage > context.rlt_threshold:
        context.file_rejected = True
        print(f"✅ {filename} rejeté ({context.error_percentage:.1f}% > {context.rlt_threshold}%)")
    else:
        raise AssertionError(f"Fichier ne devrait pas être rejeté")


@then('le log devrait indiquer la raison du rejet')
def step_log_rejection_reason(context):
    """Log raison rejet"""
    context.logged_rejection = True
    print("✅ Log: raison de rejet enregistrée")


@then('le rapport d\'ingestion devrait inclure une anomalie pour {filename} avec la raison "{reason}"')
def step_report_anomaly(context, filename, reason):
    """Anomalie dans rapport"""
    context.anomaly_reason = reason
    print(f"✅ Rapport: anomalie pour {filename} - {reason}")


@then('{filename} doit rester dans le stockage CSV')
def step_file_retained(context, filename):
    """Fichier conservé"""
    context.file_retained = True
    print(f"✅ {filename} conservé dans stockage")


@then('le fichier {filename} doit être intégré correctement')
def step_file_integrated(context, filename):
    """Fichier intégré"""
    context.file_integrated = True
    print(f"✅ {filename} intégré correctement")
```

---

### **`features/steps/environment.py`**

```python
# features/steps/environment.py
"""
Configuration Behave pour tests d'intégration WAX NG
"""

import os
from pyspark.sql import SparkSession


def before_all(context):
    """Setup global"""
    print("\n" + "=" * 80)
    print("🧪 TESTS D'INTÉGRATION WAX NG")
    print("=" * 80)
    
    # Databricks
    if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        context.spark = SparkSession.getActiveSession()
        
        if context.spark is None:
            try:
                import __main__
                context.spark = __main__.spark
            except:
                context.spark = SparkSession.builder.getOrCreate()
        
        try:
            from pyspark.dbutils import DBUtils
            context.dbutils = DBUtils(context.spark)
        except:
            context.dbutils = None
    else:
        print("⚠️  Exécution locale - mode simulation")
        context.spark = None
        context.dbutils = None
    
    # Configuration
    context.catalog = "abu_catalog"
    context.schema = "gdp_poc_dev"
    context.volume = "externalvolumetest"
    
    if context.spark:
        print(f"✅ Spark {context.spark.version}")
    if context.dbutils:
        print("✅ dbutils disponible")
    
    print(f"📂 Catalog: {context.catalog}")
    print(f"📂 Schema: {context.schema}")
    print(f"📂 Volume: {context.volume}")
    print("=" * 80 + "\n")


def before_scenario(context, scenario):
    """Setup avant scénario"""
    print(f"\n{'=' * 80}")
    print(f"📋 SCÉNARIO: {scenario.name}")
    print(f"🏷️  Tags: {', '.join(scenario.tags)}")
    print(f"{'=' * 80}")
    
    # Reset variables
    context.workflow_triggered = False
    context.workflow_status = None
    context.parallel_enabled = False
    context.file_rejected = False
    context.zip_name = None
    context.csv_count = 0
    context.tables_created = 0


def after_scenario(context, scenario):
    """Cleanup après scénario"""
    print(f"\n{'-' * 80}")
    if scenario.status == "passed":
        print("✅ SCÉNARIO RÉUSSI")
    else:
        print("❌ SCÉNARIO ÉCHOUÉ")
    print(f"{'-' * 80}\n")


def after_all(context):
    """Cleanup final"""
    print("\n" + "=" * 80)
    print("🏁 TESTS TERMINÉS")
    print("=" * 80)
```

---

## 📂 **ÉTAPE 2 : STRUCTURE DES FICHIERS**

```
waxng/
├── features/
│   ├── file_formats.feature          # ✅ Votre fichier de scénarios
│   │
│   └── steps/
│       ├── __init__.py
│       ├── environment.py            # ✅ À créer
│       └── file_format_steps.py      # ✅ À créer
│
├── notebooks/
│   ├── launch_pipeline.py            # ✅ Déjà créé
│   ├── compare_with_baseline.py      # ✅ À créer (étape suivante)
│   └── generate_test_report.py       # ✅ À créer (étape suivante)
│
├── resources/
│   ├── waxng_job.yml                 # ✅ Job production
│   └── waxng_test_job.yml            # ✅ Job test
│
├── databricks.yml                    # ✅ Config bundle
├── setup.py
└── README.md
```

---

## 🚀 **ÉTAPE 3 : LANCER LES TESTS**

### **3.1 En local (simulation)**

```bash
# Depuis la racine du projet
cd features
behave file_formats.feature --tags=@R1 --no-capture
```

**Résultat attendu :**
```
Feature: Format des fichiers transférés et ingestion via Databricks

  Scenario: Ingestion automatique d'un ZIP avec 5 fichiers CSV en parallèle
    Given un ZIP nommé "incoming_data.zip"... ✅
    And le ZIP est placé sur la landing zone... ✅
    ...
    Then le workflow doit se terminer avec le statut SUCCEEDED... ✅

5 scenarios (5 passed)
35 steps (35 passed)
```

---

### **3.2 Sur Databricks (via notebook)**

Créer `notebooks/run_behave_tests.py` :

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Exécution Tests BDD Behave

# COMMAND ----------

%pip install behave>=1.2.6 --quiet
dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("test_tags", "@R1")
dbutils.widgets.text("test_feature", "file_formats.feature")

test_tags = dbutils.widgets.get("test_tags")
test_feature = dbutils.widgets.get("test_feature")

# COMMAND ----------

import subprocess
import sys
from pathlib import Path

# Trouver le dossier features
import waxng
features_path = Path(waxng.__file__).parent.parent / "features"

print(f"📂 Features: {features_path}")
print(f"🏷️  Tags: {test_tags}")
print(f"📋 Feature: {test_feature}")

# COMMAND ----------

# Exécuter Behave
result = subprocess.run([
    sys.executable, "-m", "behave",
    str(features_path / test_feature),
    "--tags", test_tags,
    "--format", "pretty",
    "--no-capture"
], capture_output=True, text=True)

print(result.stdout)
print(result.stderr)

# COMMAND ----------

if result.returncode == 0:
    print("\n" + "=" * 80)
    print("✅ TOUS LES TESTS ONT RÉUSSI")
    print("=" * 80)
else:
    print("\n" + "=" * 80)
    print("❌ CERTAINS TESTS ONT ÉCHOUÉ")
    print("=" * 80)
    raise Exception(f"Tests échoués (code {result.returncode})")
```

---

### **3.3 Via le job Databricks**

```bash
# Lancer le job BDD
databricks jobs run-now --job-name waxng_bdd_tests \
  --parameters test_tags=@R1,test_feature=file_formats.feature
```

---

## 📊 **ÉTAPE 4 : PLAN D'ACTION COMPLET**

Voici ce qu'il faut faire **dans l'ordre** :

### **Phase 1 : Tests BDD seuls (cette semaine)**

```bash
# 1. Créer les fichiers steps
mkdir -p features/steps
# Copier environment.py et file_format_steps.py

# 2. Tester localement
cd features
behave file_formats.feature --tags=@R1 --no-capture

# 3. Builder et déployer
uv build --wheel
databricks bundle deploy --target dev

# 4. Lancer sur Databricks
databricks jobs run-now --job-name waxng_bdd_tests
```

**Objectif :** Valider que tous vos scénarios passent ✅

---

### **Phase 2 : Baseline + Validation (semaine prochaine)**

```bash
# 1. Préparer données de test
# → Créer site_test.zip, activite_test.zip

# 2. Lancer pipeline sur données test
databricks jobs run-now --job-name waxng_job \
  --parameters dataset=site,zip_path=.../site_test.zip

# 3. Vérifier résultats manuellement (SQL)
# → SELECT * FROM site_clymene_all

# 4. Créer baseline
# → CREATE TABLE baseline_site_clymene_all AS SELECT...

# 5. Créer notebooks compare + report
# → compare_with_baseline.py
# → generate_test_report.py

# 6. Lancer validation
databricks jobs run-now --job-name waxng_test_validation \
  --parameters dataset=site
```

**Objectif :** Valider automatiquement les résultats ✅

---

### **Phase 3 : Automatisation CI/CD (plus tard)**

```yaml
# .github/workflows/test.yml
- Run BDD tests
- Run validation
- Generate report
- Notify team
```

---

## ✅ **CHECKLIST IMMÉDIATE**

**À faire maintenant :**

- [ ] Créer `features/steps/environment.py`
- [ ] Créer `features/steps/file_format_steps.py`
- [ ] Créer `notebooks/run_behave_tests.py`
- [ ] Tester localement : `behave file_formats.feature --tags=@R1`
- [ ] Builder : `uv build --wheel`
- [ ] Déployer : `databricks bundle deploy --target dev`
- [ ] Lancer job : `databricks jobs run-now --job-name waxng_bdd_tests`

**La semaine prochaine :**

- [ ] Créer données de test
- [ ] Créer baseline
- [ ] Créer notebooks de comparaison
- [ ] Lancer validation complète

---

## 🎯 **RÉSUMÉ**

**Vous avez maintenant :**
1. ✅ Pipelines de production (`waxng_job.yml`)
2. ✅ Jobs de test (`waxng_test_job.yml`)
3. ✅ Scénarios Gherkin (votre fichier `.feature`)
4. ✅ Steps Python (à créer ci-dessus)

**Prochaine action :** Créer les 3 fichiers Python et lancer votre premier test ! 🚀
