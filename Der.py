# 🎯 **SOLUTION COMPLÈTE : TESTS BDD SUR DATABRICKS**

Je vais vous donner une solution **simple et fonctionnelle** pour déployer vos tests BDD sur Databricks.

---

## 📁 **STRUCTURE FINALE DU PROJET**

```
waxng/
├── features/                           # Tests BDD
│   ├── unzip.feature                   # Scénarios de test
│   └── steps/
│       ├── __init__.py
│       ├── environment.py              # Configuration Behave
│       └── unzip_steps.py              # Implémentation
│
├── notebooks/                          # ✅ NOUVEAU
│   └── run_bdd_tests.py                # Notebook pour exécuter tests
│
├── waxng/                              # Code source
│   ├── __init__.py
│   ├── main.py
│   ├── unzip_module.py
│   └── ... (autres modules)
│
├── resources/
│   └── waxng_job.yml                   # Configuration job
│
├── setup.py
├── requirements.txt
└── README.md
```

---

## 📝 **TOUS LES FICHIERS NÉCESSAIRES**

### **1. `features/unzip.feature` (Scénarios de test)**

```gherkin
# features/unzip.feature

Feature: Tests du module de décompression
  En tant que développeur
  Je veux tester la décompression de fichiers ZIP
  Pour garantir la qualité du pipeline

  @smoke
  Scenario: Décompresser un fichier ZIP valide
    Given je suis sur Databricks
    When je liste les fichiers dans "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/landing/zip/"
    Then je dois trouver au moins 1 fichier
    And les fichiers doivent avoir l'extension ".zip"

  @unzip
  Scenario: Vérifier la présence de dbutils
    Given je suis sur Databricks
    Then dbutils doit être disponible
    And je peux lister des fichiers avec dbutils
```

---

### **2. `features/steps/__init__.py`**

```python
# features/steps/__init__.py

"""Package des step definitions"""
# Fichier vide
```

---

### **3. `features/steps/environment.py`**

```python
# features/steps/environment.py

"""Configuration Behave pour Databricks"""

from pyspark.sql import SparkSession
import sys


def before_all(context):
    """Setup global"""
    print("\n" + "=" * 80)
    print("🚀 INITIALISATION TESTS BDD")
    print("=" * 80)
    
    # Sur Databricks, Spark est déjà disponible
    context.spark = SparkSession.getActiveSession()
    
    if context.spark is None:
        # Fallback : essayer la variable globale
        try:
            import __main__
            context.spark = __main__.spark
        except:
            print("❌ Impossible de trouver Spark")
            sys.exit(1)
    
    print(f"✅ Spark {context.spark.version}")
    
    # dbutils
    try:
        from pyspark.dbutils import DBUtils
        context.dbutils = DBUtils(context.spark)
        print("✅ dbutils disponible")
    except:
        context.dbutils = None
        print("⚠️  dbutils non disponible")
    
    # Configuration
    context.catalog = "abu_catalog"
    context.schema = "gdp_poc_dev"
    
    print("=" * 80 + "\n")


def before_scenario(context, scenario):
    """Setup avant chaque scénario"""
    print(f"\n📋 {scenario.name}")
    print("-" * 80)


def after_scenario(context, scenario):
    """Cleanup après chaque scénario"""
    if scenario.status == "passed":
        print("✅ RÉUSSI")
    else:
        print("❌ ÉCHOUÉ")
    print("-" * 80)


def after_all(context):
    """Cleanup final"""
    print("\n" + "=" * 80)
    print("🏁 TESTS TERMINÉS")
    print("=" * 80)
```

---

### **4. `features/steps/unzip_steps.py`**

```python
# features/steps/unzip_steps.py

"""Steps de test pour le module unzip"""

from behave import given, when, then


# ==========================================
# GIVEN - Configuration
# ==========================================

@given('je suis sur Databricks')
def step_on_databricks(context):
    """Vérifier qu'on est sur Databricks"""
    assert context.spark is not None, "Spark non disponible"
    print("✅ Sur Databricks")


# ==========================================
# WHEN - Actions
# ==========================================

@when('je liste les fichiers dans "{path}"')
def step_list_files(context, path):
    """Lister les fichiers dans un chemin"""
    if context.dbutils is None:
        print("⚠️  dbutils non disponible - skip")
        context.files = []
        return
    
    try:
        context.files = context.dbutils.fs.ls(path)
        print(f"📂 {len(context.files)} fichier(s) trouvé(s)")
    except Exception as e:
        print(f"⚠️  Erreur: {e}")
        context.files = []


# ==========================================
# THEN - Vérifications
# ==========================================

@then('je dois trouver au moins {count:d} fichier')
def step_verify_file_count(context, count):
    """Vérifier le nombre de fichiers"""
    actual = len(context.files)
    assert actual >= count, f"Attendu >= {count}, trouvé {actual}"
    print(f"✅ {actual} fichier(s) >= {count}")


@then('les fichiers doivent avoir l\'extension "{ext}"')
def step_verify_extension(context, ext):
    """Vérifier l'extension des fichiers"""
    if not context.files:
        print("⚠️  Aucun fichier à vérifier")
        return
    
    for f in context.files:
        assert f.name.endswith(ext), f"{f.name} ne se termine pas par {ext}"
    
    print(f"✅ Tous les fichiers ont l'extension {ext}")


@then('dbutils doit être disponible')
def step_verify_dbutils(context):
    """Vérifier que dbutils est disponible"""
    assert context.dbutils is not None, "dbutils non disponible"
    print("✅ dbutils disponible")


@then('je peux lister des fichiers avec dbutils')
def step_can_list_with_dbutils(context):
    """Tester dbutils.fs.ls"""
    if context.dbutils is None:
        raise AssertionError("dbutils non disponible")
    
    # Test sur un chemin qui existe toujours
    path = f"/Volumes/{context.catalog}/{context.schema}"
    
    try:
        files = context.dbutils.fs.ls(path)
        print(f"✅ Listé {len(files)} élément(s) dans {path}")
    except Exception as e:
        raise AssertionError(f"Impossible de lister {path}: {e}")
```

---

### **5. `notebooks/run_bdd_tests.py` (Notebook Databricks)**

```python
# Databricks notebook source

# MAGIC %md
# MAGIC # 🧪 Tests BDD - WAX Pipeline
# MAGIC
# MAGIC Ce notebook exécute les tests BDD avec Behave

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Installation de Behave

# COMMAND ----------

%pip install behave>=1.2.6 --quiet
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Vérification de l'environnement

# COMMAND ----------

print("✅ Spark version:", spark.version)
print("✅ Python version:", __import__('sys').version)

# Vérifier que waxng est installé
try:
    import waxng
    print("✅ Package waxng installé")
except ImportError:
    print("❌ Package waxng non trouvé")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Exécution des tests BDD

# COMMAND ----------

import subprocess
import sys
from pathlib import Path

# Trouver le dossier features
import waxng
pkg_dir = Path(waxng.__file__).parent
features_path = pkg_dir.parent / "features"

if not features_path.exists():
    # Fallback : chercher dans site-packages
    import site
    for site_pkg in site.getsitepackages():
        alt_path = Path(site_pkg) / "features"
        if alt_path.exists():
            features_path = alt_path
            break

print(f"📂 Features path: {features_path}")

# Exécuter Behave
result = subprocess.run(
    [
        sys.executable, "-m", "behave",
        str(features_path),
        "--format", "pretty",
        "--no-capture",
        "--color"
    ],
    capture_output=False
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Résultat

# COMMAND ----------

if result.returncode == 0:
    print("✅ TOUS LES TESTS ONT RÉUSSI")
else:
    print("❌ CERTAINS TESTS ONT ÉCHOUÉ")
    raise Exception(f"Tests échoués (code {result.returncode})")
```

---

### **6. `resources/waxng_job.yml` (Configuration Job)**

```yaml
# resources/waxng_job.yml

resources:
  jobs:
    waxng_job:
      name: waxng_job
      
      tasks:
        # Task 1 : Unzip
        - task_key: unzip
          description: "Extraction ZIP"
          python_wheel_task:
            package_name: waxng
            entry_point: unzip_module
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
          libraries:
            - whl: ../dist/waxng-1.0.0-py3-none-any.whl
        
        # Task 2 : Auto Loader
        - task_key: auto-loader
          depends_on:
            - task_key: unzip
          description: "Auto Loader"
          python_wheel_task:
            package_name: waxng
            entry_point: autoloader_module
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
        
        # Task 3 : Ingestion
        - task_key: waxng-ingestion
          depends_on:
            - task_key: auto-loader
          description: "Ingestion finale"
          python_wheel_task:
            package_name: waxng
            entry_point: main
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
        
        # ✅ Task 4 : Tests BDD (NOTEBOOK)
        - task_key: bdd-tests
          depends_on:
            - task_key: waxng-ingestion
          description: "Tests BDD"
          notebook_task:
            notebook_path: /Workspace/Repos/${workspace.current_user.userName}/waxng/notebooks/run_bdd_tests
            source: WORKSPACE
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
          libraries:
            - whl: ../dist/waxng-1.0.0-py3-none-any.whl
```

---

### **7. `setup.py` (Mise à jour)**

```python
# setup.py

from setuptools import setup, find_packages
import os


def read_requirements():
    req_path = os.path.join(os.path.dirname(__file__), "requirements.txt")
    if os.path.exists(req_path):
        with open(req_path, "r") as f:
            return [line.strip() for line in f if line.strip() and not line.startswith("#")]
    return []


setup(
    name="waxng",
    version="1.0.0",
    packages=find_packages(),
    
    # Inclure les features
    package_data={
        "": ["*.feature", "*.md", "*.txt"],
    },
    include_package_data=True,
    
    # Dépendances
    install_requires=read_requirements(),
    
    # Entry points
    entry_points={
        "console_scripts": [
            "wax-pipeline=waxng.main:main",
        ],
    },
    
    zip_safe=False,
)
```

---

### **8. `MANIFEST.in`**

```
# MANIFEST.in

# Inclure les features
recursive-include features *.feature
recursive-include features/steps *.py

# Inclure les notebooks
recursive-include notebooks *.py

# Documentation
include README.md
include requirements.txt
```

---

## 🚀 **DÉPLOIEMENT COMPLET**

### **Étape 1 : Préparer le projet**

```bash
# 1. Créer les dossiers manquants
mkdir -p features/steps
mkdir -p notebooks

# 2. Créer tous les fichiers ci-dessus

# 3. Vérifier la structure
tree waxng/
```

### **Étape 2 : Builder le wheel**

```bash
# 1. Nettoyer
rm -rf dist/ build/ *.egg-info

# 2. Builder
python setup.py bdist_wheel

# 3. Vérifier
ls -lh dist/
unzip -l dist/waxng-1.0.0-py3-none-any.whl | grep features
```

### **Étape 3 : Créer le notebook dans Databricks**

```bash
# Option A : Via Databricks CLI
databricks workspace import notebooks/run_bdd_tests.py \
  /Workspace/Repos/[votre-username]/waxng/notebooks/run_bdd_tests.py \
  --language PYTHON

# Option B : Via l'interface web
# 1. Aller dans Workspace
# 2. Créer un dossier notebooks/
# 3. Copier le contenu de notebooks/run_bdd_tests.py
```

### **Étape 4 : Déployer le job**

```bash
# Déployer avec Databricks Asset Bundles
databricks bundle deploy --target dev
```

### **Étape 5 : Lancer les tests**

```bash
# Lancer le job complet
databricks jobs run-now --job-name "waxng_job"

# Ou lancer uniquement les tests
databricks jobs run-now --job-name "waxng_job" --task-key bdd-tests
```

---

## 📊 **RÉSULTAT ATTENDU**

Dans le notebook `run_bdd_tests`, vous verrez :

```
✅ Spark version: 14.3.x-scala2.12
✅ Python version: 3.11.x
✅ Package waxng installé

📂 Features path: /local_disk0/.../features

================================================================================
🚀 INITIALISATION TESTS BDD
================================================================================
✅ Spark 14.3.x-scala2.12
✅ dbutils disponible
================================================================================

Feature: Tests du module de décompression

  @smoke
  Scenario: Décompresser un fichier ZIP valide
    ✅ Given je suis sur Databricks
    ✅ When je liste les fichiers dans ...
    ✅ Then je dois trouver au moins 1 fichier
    ✅ And les fichiers doivent avoir l'extension ".zip"

  @unzip
  Scenario: Vérifier la présence de dbutils
    ✅ Given je suis sur Databricks
    ✅ Then dbutils doit être disponible
    ✅ And je peux lister des fichiers avec dbutils

2 scenarios (2 passed)
6 steps (6 passed)

✅ TOUS LES TESTS ONT RÉUSSI
```

---

## 🎓 **PROCHAINES ÉTAPES POUR APPRENDRE GHERKIN**

### **1. Ajouter des tests pour votre pipeline**

Créez `features/ingestion.feature` :

```gherkin
Feature: Tests d'ingestion WAX

  @ingestion
  Scenario: Vérifier les tables Delta
    Given je suis sur Databricks
    When je liste les tables dans "abu_catalog.gdp_poc_dev"
    Then je dois trouver la table "wax_execution_logs"
    And je dois trouver la table "wax_data_quality_errors"

  @quality
  Scenario: Vérifier la qualité des données
    Given je suis sur Databricks
    And la table "abu_catalog.gdp_poc_dev.site_clymene_all" existe
    When je compte les lignes dans cette table
    Then je dois avoir plus de 0 lignes
```

### **2. Pattern Gherkin de base**

```gherkin
Scenario: [Description claire]
  Given [État initial / Contexte]
  When [Action effectuée]
  Then [Résultat attendu]
  And [Vérification supplémentaire]
```

### **3. Tags utiles**

```gherkin
@smoke    # Tests rapides essentiels
@slow     # Tests lents
@wip      # Work In Progress
@skip     # À ignorer
```

---

**Vous avez maintenant une solution complète et fonctionnelle pour vos tests BDD sur Databricks ! 🎯**

**Commencez par déployer ces fichiers, puis ajoutez progressivement vos propres scénarios de test.**
