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
    # ✅ SUR DATABRICKS : TOUJOURS utiliser la session active
    if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        print("🎯 Environnement: Databricks")
        
        # Récupérer la session EXISTANTE (NE PAS EN CRÉER UNE NOUVELLE)
        spark = SparkSession.getActiveSession()
        
        if spark is None:
            raise RuntimeError(
                "❌ Aucune session Spark active trouvée sur Databricks.\n"
                "   Cela ne devrait jamais arriver dans un cluster Databricks."
            )
        
        return spark
    
    # ✅ EXÉCUTION LOCALE : Créer une session
    else:
        print("🎯 Environnement: Local")
        
        return SparkSession.builder \
            .appName("WAX_BDD_Tests_Local") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.driver.memory", "2g") \
            .master("local[*]") \
            .getOrCreate()


def before_all(context):
    """Setup global AVANT tous les tests"""
    print("\n" + "=" * 80)
    print("🚀 INITIALISATION ENVIRONNEMENT DE TEST BDD")
    print("=" * 80)
    
    try:
        # Obtenir Spark
        context.spark = _get_spark_session()
        
        if context.spark is None:
            raise RuntimeError("❌ Impossible d'obtenir une session Spark")
        
        print(f"✅ Spark {context.spark.version} disponible")
        print(f"📍 Spark Master: {context.spark.sparkContext.master}")
        
        # Vérifier dbutils (Databricks uniquement)
        try:
            from pyspark.dbutils import DBUtils
            context.dbutils = DBUtils(context.spark)
            print("✅ dbutils disponible")
        except Exception as e:
            context.dbutils = None
            print("⚠️  dbutils non disponible (mode local)")
            
    except Exception as e:
        print(f"❌ Erreur initialisation Spark: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    # Configuration
    context.catalog = "abu_catalog"
    context.schema = "gdp_poc_dev"
    context.volume = "externalvolumetest"
    
    print(f"📂 Catalog: {context.catalog}")
    print(f"📂 Schema: {context.schema}")
    print("=" * 80 + "\n")


def after_all(context):
    """Cleanup APRÈS tous les tests"""
    print("\n" + "=" * 80)
    print("🧹 NETTOYAGE ENVIRONNEMENT DE TEST")
    print("=" * 80)
    
    # ✅ NE JAMAIS arrêter Spark sur Databricks
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
    """Setup AVANT chaque scénario"""
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
    """Cleanup APRÈS chaque scénario"""
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
    """Setup AVANT chaque feature"""
    print("\n" + "=" * 80)
    print(f"🎯 FEATURE: {feature.name}")
    print("=" * 80)


def after_feature(context, feature):
    """Cleanup APRÈS chaque feature"""
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
