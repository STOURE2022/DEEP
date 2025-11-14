# 🔴 **ERREUR : Initialisation Spark sur Databricks**

## **PROBLÈME**

Sur Databricks, vous ne pouvez PAS créer une nouvelle SparkSession avec `SparkSession.builder`. Spark est **déjà disponible** !

---

## ✅ **SOLUTION : Modifier `environment.py`**

### **Remplacer COMPLÈTEMENT le fichier `features/steps/environment.py` :**

```python
# features/steps/environment.py

"""
Configuration de l'environnement Behave pour les tests BDD
Compatible Databricks et exécution locale
"""

from pyspark.sql import SparkSession
import sys
import os


def _get_spark_session():
    """
    Obtenir la session Spark selon l'environnement
    
    Returns:
        SparkSession: Session Spark active
    """
    # ✅ Sur Databricks : Utiliser la session existante
    try:
        # Vérifier si on est sur Databricks
        if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
            print("🎯 Environnement détecté: Databricks")
            
            # Récupérer la session Spark existante
            spark = SparkSession.getActiveSession()
            
            if spark is None:
                # Si pas de session active, en créer une (ne devrait pas arriver sur Databricks)
                spark = SparkSession.builder.getOrCreate()
            
            return spark
        
        else:
            # ✅ Exécution locale : Créer une session
            print("🎯 Environnement détecté: Local")
            
            spark = SparkSession.builder \
                .appName("WAX_BDD_Tests_Local") \
                .config("spark.sql.shuffle.partitions", "2") \
                .config("spark.driver.memory", "2g") \
                .master("local[*]") \
                .getOrCreate()
            
            return spark
            
    except Exception as e:
        print(f"⚠️  Erreur détection environnement: {e}")
        
        # Fallback : essayer de récupérer session active
        spark = SparkSession.getActiveSession()
        if spark:
            return spark
        
        # Dernier recours : créer une session locale
        return SparkSession.builder \
            .appName("WAX_BDD_Tests") \
            .getOrCreate()


def before_all(context):
    """
    Setup global AVANT tous les tests
    """
    print("\n" + "=" * 80)
    print("🚀 INITIALISATION ENVIRONNEMENT DE TEST BDD")
    print("=" * 80)
    
    # Obtenir Spark selon l'environnement
    try:
        context.spark = _get_spark_session()
        
        if context.spark is None:
            raise RuntimeError("Impossible d'obtenir une session Spark")
        
        print(f"✅ Spark {context.spark.version} disponible")
        print(f"📍 Spark Master: {context.spark.sparkContext.master}")
        
        # Vérifier si dbutils est disponible (Databricks)
        try:
            from pyspark.dbutils import DBUtils
            context.dbutils = DBUtils(context.spark)
            print("✅ dbutils disponible")
        except Exception:
            context.dbutils = None
            print("⚠️  dbutils non disponible (mode local)")
        
    except Exception as e:
        print(f"❌ Erreur initialisation Spark: {e}")
        import traceback
        traceback.print_exc()
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
    
    # Sur Databricks, NE PAS arrêter Spark !
    if 'DATABRICKS_RUNTIME_VERSION' not in os.environ:
        if hasattr(context, 'spark') and context.spark is not None:
            try:
                context.spark.stop()
                print("✅ Spark arrêté proprement (mode local)")
            except Exception as e:
                print(f"⚠️  Erreur arrêt Spark: {e}")
    else:
        print("ℹ️  Session Spark conservée (Databricks)")
    
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
    skipped = sum(1 for s in feature.scenarios if s.status == "skipped")
    total = len(feature.scenarios)
    
    print("\n" + "=" * 80)
    print(f"📊 RÉSULTATS FEATURE: {feature.name}")
    print(f"   ✅ Réussis: {passed}/{total}")
    
    if failed > 0:
        print(f"   ❌ Échoués: {failed}/{total}")
    
    if skipped > 0:
        print(f"   ⏭️  Ignorés: {skipped}/{total}")
    
    print("=" * 80)
```

---

## 🔧 **AUSSI : Vérifier `run_bdd_tests.py`**

Assurez-vous que le script n'essaie pas de créer Spark non plus :

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
    # Sur Databricks avec wheel, le package est dans site-packages
    try:
        import waxng
        waxng_path = Path(waxng.__file__).parent
        features_path = waxng_path.parent / "features"
        
        # Si features n'est pas trouvé, essayer autre chemin
        if not features_path.exists():
            features_path = waxng_path / "features"
        
    except ImportError:
        # Fallback : utiliser le répertoire courant
        current_dir = Path(__file__).parent.parent
        features_path = current_dir / "features"
    
    if not features_path.exists():
        print(f"❌ Dossier features introuvable: {features_path}")
        print(f"📂 Chemins essayés:")
        print(f"   - {waxng_path.parent / 'features'}")
        print(f"   - {waxng_path / 'features'}")
        sys.exit(1)
    
    print(f"📂 Features path: {features_path}")
    
    # Configurer l'environnement
    env = os.environ.copy()
    
    # Arguments Behave
    behave_args = [
        sys.executable, "-m", "behave",
        str(features_path),  # Exécuter tous les .feature
        "--format", "pretty",
        "--no-capture",
        "--tags", "~@skip",
        "--color",
    ]
    
    print(f"🚀 Commande: {' '.join(behave_args)}\n")
    
    # Exécuter Behave
    try:
        result = subprocess.run(
            behave_args,
            env=env,
            capture_output=False,
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

## 📦 **RE-BUILDER LE PACKAGE**

```bash
# 1. Nettoyer les anciens builds
rm -rf dist/ build/ *.egg-info

# 2. Re-builder le wheel
python setup.py bdist_wheel

# 3. Vérifier le contenu
unzip -l dist/waxng-1.0.0-py3-none-any.whl | grep -E "(features|environment)"
```

---

## ☁️ **RE-DÉPLOYER SUR DATABRICKS**

```bash
# 1. Re-déployer
databricks bundle deploy --target dev

# 2. Relancer les tests
databricks jobs run-now --job-name "waxng_job"
```

---

## 📊 **RÉSULTAT ATTENDU**

Maintenant vous devriez voir :

```
🎯 Environnement détecté: Databricks
✅ Spark 14.3.x disponible
✅ dbutils disponible
📂 Catalog: abu_catalog
📂 Schema: gdp_poc_dev
📂 Volume: externalvolumetest

Feature: Décompression de fichiers ZIP
  ...
  ✅ 0 features passed, 0 failed, 0 skipped
```

---

## 🔍 **ALTERNATIVE : Tester SANS Spark**

Si vous voulez tester la logique sans Databricks, créez un fichier `test_mock.py` :

```python
# tests/test_unzip_mock.py

"""Tests unitaires sans Spark pour la logique métier"""

import pytest
from unittest.mock import Mock, patch

def test_parse_tolerance():
    """Tester parse_tolerance sans Spark"""
    from waxng.utils import parse_tolerance
    
    assert parse_tolerance("10%", 100) == 10
    assert parse_tolerance("5", 100) == 0.05
    assert parse_tolerance("20%", 1000) == 200

def test_unzip_logic_mock():
    """Tester la logique unzip avec des mocks"""
    # Mock dbutils
    mock_dbutils = Mock()
    mock_dbutils.fs.unzip.return_value = True
    
    # Votre logique métier ici
    result = mock_dbutils.fs.unzip("/path/to/file.zip", "/extract/")
    
    assert result is True
    mock_dbutils.fs.unzip.assert_called_once()
```

---

**Le problème principal était l'initialisation de Spark. Sur Databricks, il faut TOUJOURS utiliser la session existante ! 🎯**
