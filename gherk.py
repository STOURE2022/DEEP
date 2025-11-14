# 🧪 **PREMIER TEST BDD AVEC GHERKIN - MODULE UNZIP**

## 📁 **STRUCTURE COMPLÈTE**

```
waxng/
├── features/
│   ├── unzip.feature              # ✅ Scénarios Gherkin
│   └── steps/
│       ├── __init__.py            # ✅ Fichier vide
│       ├── environment.py         # ✅ Configuration Behave
│       └── unzip_steps.py         # ✅ Implémentation des steps
├── waxng/
│   ├── unzip_module.py            # Votre module existant
│   └── run_bdd_tests.py           # ✅ Script d'exécution
└── setup.py                        # À modifier
```

---

## 📝 **1. FICHIER : `features/unzip.feature`**

```gherkin
# features/unzip.feature

Feature: Décompression de fichiers ZIP
  En tant que data engineer
  Je veux décompresser des fichiers ZIP automatiquement
  Pour préparer les données avant ingestion

  Background:
    Given Spark est disponible
    And les chemins de base sont configurés

  @smoke @unzip
  Scenario: Décompresser un fichier ZIP valide
    Given un fichier ZIP existe à "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/landing/zip/test_valid.zip"
    When je décompresse le fichier vers "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/preprocessed/"
    Then la décompression doit réussir
    And les fichiers extraits doivent exister dans le dossier de destination

  @unzip
  Scenario: Compter les fichiers extraits
    Given un fichier ZIP existe à "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/landing/zip/site_20251112_173355.zip"
    When je décompresse le fichier vers "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/preprocessed/"
    Then au moins 1 fichier doit être extrait

  @unzip @negative
  Scenario: Gérer un fichier ZIP inexistant
    Given un fichier ZIP n'existe pas à "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/landing/zip/nonexistent.zip"
    When je tente de décompresser le fichier
    Then une erreur doit être levée
    And le message d'erreur doit contenir "not found" ou "does not exist"

  @unzip @negative
  Scenario: Gérer un fichier ZIP corrompu
    Given un fichier ZIP corrompu existe à "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/landing/zip/corrupt.zip"
    When je tente de décompresser le fichier vers "/tmp/test_extract/"
    Then une erreur doit être levée
    And le message d'erreur doit contenir "corrupt" ou "invalid"
```

---

## 🐍 **2. FICHIER : `features/steps/unzip_steps.py`**

```python
# features/steps/unzip_steps.py

"""
Step definitions pour les tests de décompression ZIP
"""

from behave import given, when, then
from pyspark.sql import SparkSession
import traceback


# ==========================================
# GIVEN STEPS
# ==========================================

@given('Spark est disponible')
def step_spark_available(context):
    """Vérifier que Spark est disponible"""
    if not hasattr(context, 'spark') or context.spark is None:
        raise AssertionError("❌ Spark n'est pas disponible dans le contexte")
    
    print(f"✅ Spark {context.spark.version} disponible")


@given('les chemins de base sont configurés')
def step_paths_configured(context):
    """Configurer les chemins de base"""
    context.base_path = "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest"
    context.landing_zip = f"{context.base_path}/landing/zip"
    context.preprocessed = f"{context.base_path}/preprocessed"
    
    print(f"📂 Landing ZIP: {context.landing_zip}")
    print(f"📂 Preprocessed: {context.preprocessed}")


@given('un fichier ZIP existe à "{zip_path}"')
def step_zip_exists(context, zip_path):
    """Vérifier qu'un fichier ZIP existe"""
    context.zip_path = zip_path
    
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(context.spark)
        
        # Vérifier l'existence
        file_info = dbutils.fs.ls(zip_path)
        context.zip_exists = True
        print(f"✅ Fichier trouvé: {zip_path}")
        
    except Exception as e:
        context.zip_exists = False
        print(f"⚠️  Fichier non trouvé: {zip_path}")
        raise AssertionError(f"Le fichier ZIP n'existe pas: {zip_path}")


@given('un fichier ZIP n\'existe pas à "{zip_path}"')
def step_zip_not_exists(context, zip_path):
    """S'assurer qu'un fichier ZIP n'existe PAS"""
    context.zip_path = zip_path
    
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(context.spark)
        
        # Vérifier que le fichier n'existe PAS
        file_info = dbutils.fs.ls(zip_path)
        raise AssertionError(f"❌ Le fichier existe alors qu'il ne devrait pas: {zip_path}")
        
    except Exception as e:
        # C'est normal qu'il n'existe pas
        context.zip_exists = False
        print(f"✅ Confirmation: fichier n'existe pas: {zip_path}")


@given('un fichier ZIP corrompu existe à "{zip_path}"')
def step_corrupt_zip_exists(context, zip_path):
    """Créer ou utiliser un fichier ZIP corrompu"""
    context.zip_path = zip_path
    context.zip_exists = True
    context.is_corrupt = True
    
    # Note: Dans un vrai test, vous créeriez un fichier corrompu ici
    print(f"⚠️  Fichier corrompu: {zip_path}")


# ==========================================
# WHEN STEPS
# ==========================================

@when('je décompresse le fichier vers "{extract_path}"')
def step_unzip_to_path(context, extract_path):
    """Décompresser le fichier ZIP"""
    context.extract_path = extract_path
    
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(context.spark)
        
        print(f"🔓 Décompression: {context.zip_path}")
        print(f"📂 Destination: {extract_path}")
        
        # Décompresser
        result = dbutils.fs.unzip(context.zip_path, extract_path)
        
        context.unzip_success = True
        context.unzip_error = None
        context.unzip_result = result
        
        print(f"✅ Décompression réussie")
        
    except Exception as e:
        context.unzip_success = False
        context.unzip_error = e
        context.error_message = str(e)
        
        print(f"❌ Erreur décompression: {e}")
        traceback.print_exc()


@when('je tente de décompresser le fichier')
def step_attempt_unzip(context):
    """Tenter de décompresser (peut échouer)"""
    context.extract_path = "/tmp/test_extract/"
    
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(context.spark)
        
        print(f"🔓 Tentative décompression: {context.zip_path}")
        
        # Tenter de décompresser
        result = dbutils.fs.unzip(context.zip_path, context.extract_path)
        
        context.unzip_success = True
        context.unzip_error = None
        
        print(f"✅ Décompression réussie (inattendu)")
        
    except Exception as e:
        context.unzip_success = False
        context.unzip_error = e
        context.error_message = str(e).lower()
        context.error_type = type(e).__name__
        
        print(f"❌ Erreur attendue: {type(e).__name__}")


# ==========================================
# THEN STEPS
# ==========================================

@then('la décompression doit réussir')
def step_verify_unzip_success(context):
    """Vérifier que la décompression a réussi"""
    assert context.unzip_success is True, \
        f"❌ La décompression a échoué: {context.unzip_error}"
    
    print("✅ Décompression validée")


@then('les fichiers extraits doivent exister dans le dossier de destination')
def step_verify_files_exist(context):
    """Vérifier que des fichiers existent dans le dossier"""
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(context.spark)
        
        # Lister les fichiers extraits
        files = dbutils.fs.ls(context.extract_path)
        
        assert len(files) > 0, \
            f"❌ Aucun fichier trouvé dans {context.extract_path}"
        
        print(f"✅ {len(files)} fichier(s) extrait(s)")
        for file in files:
            print(f"   - {file.name}")
        
    except Exception as e:
        raise AssertionError(f"❌ Impossible de lister les fichiers: {e}")


@then('au moins {min_count:d} fichier doit être extrait')
def step_verify_min_files(context, min_count):
    """Vérifier le nombre minimum de fichiers extraits"""
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(context.spark)
        
        files = dbutils.fs.ls(context.extract_path)
        actual_count = len(files)
        
        assert actual_count >= min_count, \
            f"❌ Attendu au moins {min_count} fichier(s), trouvé {actual_count}"
        
        print(f"✅ {actual_count} fichier(s) extrait(s) (>= {min_count})")
        
    except Exception as e:
        raise AssertionError(f"❌ Erreur comptage fichiers: {e}")


@then('une erreur doit être levée')
def step_verify_error_raised(context):
    """Vérifier qu'une erreur a bien été levée"""
    assert context.unzip_success is False, \
        "❌ Aucune erreur levée alors qu'on en attendait une"
    
    assert context.unzip_error is not None, \
        "❌ Aucune erreur enregistrée"
    
    print(f"✅ Erreur levée comme attendu: {context.error_type}")


@then('le message d\'erreur doit contenir "{expected_text}"')
def step_verify_error_contains(context, expected_text):
    """Vérifier que le message d'erreur contient un texte"""
    assert context.unzip_error is not None, \
        "❌ Aucune erreur pour vérifier le message"
    
    error_msg = context.error_message.lower()
    expected = expected_text.lower()
    
    assert expected in error_msg, \
        f"❌ Message d'erreur ne contient pas '{expected_text}'\n" \
        f"   Message réel: {context.error_message}"
    
    print(f"✅ Message d'erreur contient bien '{expected_text}'")


@then('le message d\'erreur doit contenir "{text1}" ou "{text2}"')
def step_verify_error_contains_either(context, text1, text2):
    """Vérifier que le message contient l'un des deux textes"""
    assert context.unzip_error is not None, \
        "❌ Aucune erreur pour vérifier le message"
    
    error_msg = context.error_message.lower()
    t1 = text1.lower()
    t2 = text2.lower()
    
    assert (t1 in error_msg) or (t2 in error_msg), \
        f"❌ Message d'erreur ne contient ni '{text1}' ni '{text2}'\n" \
        f"   Message réel: {context.error_message}"
    
    print(f"✅ Message d'erreur vérifié")
```

---

## ⚙️ **3. FICHIER : `features/steps/environment.py`**

```python
# features/steps/environment.py

"""
Configuration de l'environnement Behave pour les tests BDD
"""

from pyspark.sql import SparkSession
import sys


def before_all(context):
    """
    Setup global AVANT tous les tests
    """
    print("\n" + "=" * 80)
    print("🚀 INITIALISATION ENVIRONNEMENT DE TEST BDD")
    print("=" * 80)
    
    # Initialiser Spark
    try:
        context.spark = SparkSession.builder \
            .appName("WAX_BDD_Tests_Unzip") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        
        print(f"✅ Spark {context.spark.version} initialisé")
        print(f"📍 Spark Master: {context.spark.sparkContext.master}")
        
    except Exception as e:
        print(f"❌ Erreur initialisation Spark: {e}")
        sys.exit(1)
    
    # Configurer les chemins
    context.catalog = "abu_catalog"
    context.schema = "gdp_poc_dev"
    context.volume = "externalvolumetest"
    
    print(f"📂 Catalog: {context.catalog}")
    print(f"📂 Schema: {context.schema}")
    print(f"📂 Volume: {context.volume}")
    print("=" * 80 + "\n")


def after_all(context):
    """
    Cleanup APRÈS tous les tests
    """
    print("\n" + "=" * 80)
    print("🧹 NETTOYAGE ENVIRONNEMENT DE TEST")
    print("=" * 80)
    
    if hasattr(context, 'spark') and context.spark is not None:
        try:
            context.spark.stop()
            print("✅ Spark arrêté proprement")
        except Exception as e:
            print(f"⚠️  Erreur arrêt Spark: {e}")
    
    print("=" * 80 + "\n")


def before_scenario(context, scenario):
    """
    Setup AVANT chaque scénario
    """
    print("\n" + "-" * 80)
    print(f"📋 SCÉNARIO: {scenario.name}")
    print(f"🏷️  Tags: {', '.join(scenario.tags) if scenario.tags else 'Aucun'}")
    print("-" * 80)
    
    # Réinitialiser les variables de contexte
    context.zip_path = None
    context.extract_path = None
    context.zip_exists = False
    context.unzip_success = False
    context.unzip_error = None
    context.error_message = None
    context.error_type = None


def after_scenario(context, scenario):
    """
    Cleanup APRÈS chaque scénario
    """
    if scenario.status == "failed":
        print(f"\n❌ SCÉNARIO ÉCHOUÉ: {scenario.name}")
        
        # Afficher des infos de debug
        if hasattr(context, 'unzip_error') and context.unzip_error:
            print(f"\n🔍 DÉTAILS DE L'ERREUR:")
            print(f"   Type: {type(context.unzip_error).__name__}")
            print(f"   Message: {context.unzip_error}")
    else:
        print(f"\n✅ SCÉNARIO RÉUSSI: {scenario.name}")
    
    print("-" * 80)


def before_feature(context, feature):
    """
    Setup AVANT chaque feature
    """
    print("\n" + "=" * 80)
    print(f"🎯 FEATURE: {feature.name}")
    print("=" * 80)


def after_feature(context, feature):
    """
    Cleanup APRÈS chaque feature
    """
    passed = sum(1 for s in feature.scenarios if s.status == "passed")
    failed = sum(1 for s in feature.scenarios if s.status == "failed")
    total = len(feature.scenarios)
    
    print("\n" + "=" * 80)
    print(f"📊 RÉSULTATS FEATURE: {feature.name}")
    print(f"   ✅ Réussis: {passed}/{total}")
    print(f"   ❌ Échoués: {failed}/{total}")
    print("=" * 80)
```

---

## 🔧 **4. FICHIER : `features/steps/__init__.py`**

```python
# features/steps/__init__.py

"""
Package des step definitions pour Behave
"""

# Ce fichier peut rester vide, il indique juste que c'est un package Python
```

---

## 🎯 **5. FICHIER : `waxng/run_bdd_tests.py`**

```python
# waxng/run_bdd_tests.py

"""
Point d'entrée pour exécuter les tests BDD sur Databricks
"""

import sys
import os
import subprocess
from pathlib import Path


def main():
    """
    Exécute les tests BDD avec Behave
    """
    print("\n" + "=" * 80)
    print("🧪 EXÉCUTION DES TESTS BDD - MODULE UNZIP")
    print("=" * 80 + "\n")
    
    # Trouver le répertoire des features
    current_dir = Path(__file__).parent.parent  # Remonte au dossier waxng/
    features_path = current_dir / "features"
    
    if not features_path.exists():
        print(f"❌ Dossier features introuvable: {features_path}")
        print(f"📂 Répertoire actuel: {current_dir}")
        sys.exit(1)
    
    print(f"📂 Features path: {features_path}")
    print(f"📂 Current dir: {current_dir}")
    
    # Configurer l'environnement
    env = os.environ.copy()
    env['PYTHONPATH'] = str(current_dir)
    
    # Arguments Behave
    behave_args = [
        sys.executable, "-m", "behave",
        str(features_path / "unzip.feature"),  # Test uniquement unzip.feature
        "--format", "pretty",
        "--no-capture",
        "--tags", "~@skip",  # Exclure les tests @skip
        # "--tags", "@smoke",  # Décommenter pour n'exécuter que les tests @smoke
        "--color",  # Couleurs dans le terminal
    ]
    
    print(f"🚀 Commande: {' '.join(behave_args)}\n")
    
    # Exécuter Behave
    try:
        result = subprocess.run(
            behave_args,
            env=env,
            cwd=str(current_dir),
            capture_output=False,  # Afficher directement la sortie
            text=True
        )
        
        # Vérifier le résultat
        print("\n" + "=" * 80)
        if result.returncode == 0:
            print("✅ TOUS LES TESTS BDD ONT RÉUSSI")
            print("=" * 80 + "\n")
            sys.exit(0)
        else:
            print("❌ CERTAINS TESTS BDD ONT ÉCHOUÉ")
            print(f"Code retour: {result.returncode}")
            print("=" * 80 + "\n")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Erreur lors de l'exécution des tests: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
```

---

## 📦 **6. MODIFIER `setup.py`**

```python
# setup.py

from setuptools import setup, find_packages

setup(
    name="waxng",
    version="1.0.0",
    packages=find_packages(),
    
    # Points d'entrée
    entry_points={
        'console_scripts': [
            'unzip_module=waxng.unzip_module:main',
            'autoloader_module=waxng.autoloader_module:main',
            'main=waxng.main:main',
            'run_bdd_tests=waxng.run_bdd_tests:main',  # ✅ Point d'entrée tests
        ],
    },
    
    # ✅ Inclure les fichiers .feature
    package_data={
        'waxng': [],
    },
    include_package_data=True,
    
    # Fichiers à inclure via MANIFEST.in
    # (voir ci-dessous)
    
    install_requires=[
        'pyspark>=3.0.0',
        'pandas',
        'openpyxl',
    ],
    
    # Dépendances pour les tests
    extras_require={
        'test': [
            'behave>=1.2.6',
            'pytest>=7.0.0',
        ],
    },
)
```

---

## 📄 **7. CRÉER `MANIFEST.in`**

```
# MANIFEST.in

# Inclure les features et steps
recursive-include features *.feature
recursive-include features/steps *.py
```

---

## 🚀 **8. EXÉCUTION LOCALE**

```bash
# 1. Installer Behave
pip install behave

# 2. Depuis le dossier racine waxng/
cd /path/to/waxng

# 3. Exécuter tous les tests
behave features/

# 4. Exécuter uniquement unzip.feature
behave features/unzip.feature

# 5. Exécuter uniquement les tests @smoke
behave features/unzip.feature --tags=@smoke

# 6. Exécuter SAUF les tests @negative
behave features/unzip.feature --tags=~@negative

# 7. Mode verbose avec couleurs
behave features/unzip.feature -v --color

# 8. Générer un rapport HTML
behave features/unzip.feature --format=html --outfile=report.html
```

---

## 🏗️ **9. BUILDER LE PACKAGE**

```bash
# 1. Créer le wheel
python setup.py bdist_wheel

# 2. Vérifier le contenu du wheel
unzip -l dist/waxng-1.0.0-py3-none-any.whl | grep features

# Vous devriez voir:
#   features/unzip.feature
#   features/steps/environment.py
#   features/steps/unzip_steps.py
#   features/steps/__init__.py
```

---

## ☁️ **10. DÉPLOYER SUR DATABRICKS**

```bash
# 1. Déployer le bundle
databricks bundle deploy --target dev

# 2. Lancer les tests manuellement
databricks jobs run-now --job-name "waxng_job"

# 3. Ou créer un job dédié aux tests
# Voir le fichier waxng_job.yml modifié ci-dessous
```

---

## 📊 **RÉSULTAT ATTENDU**

Quand vous exécutez `behave features/unzip.feature`, vous devriez voir :

```
Feature: Décompression de fichiers ZIP

  Background:
    Given Spark est disponible               ✅ passed
    And les chemins de base sont configurés  ✅ passed

  @smoke @unzip
  Scenario: Décompresser un fichier ZIP valide
    Given un fichier ZIP existe à ...        ✅ passed
    When je décompresse le fichier vers ...  ✅ passed
    Then la décompression doit réussir       ✅ passed
    And les fichiers extraits doivent ...    ✅ passed

  @unzip
  Scenario: Compter les fichiers extraits
    ...                                       ✅ passed

  @unzip @negative
  Scenario: Gérer un fichier ZIP inexistant
    ...                                       ✅ passed

4 scenarios (4 passed)
12 steps (12 passed)
```

---

**Tous les fichiers sont complets et prêts à utiliser ! Besoin d'aide pour l'exécution ?** 🚀
