# features/steps/unzip_steps.py

"""
Step definitions pour les tests de décompression ZIP
"""

from behave import given, when, then
from pyspark.sql import SparkSession
import traceback


# ==========================================
# ÉTANT DONNÉ (GIVEN)
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
        file_info = dbutils.fs.ls(zip_path)
        raise AssertionError(f"❌ Le fichier existe alors qu'il ne devrait pas: {zip_path}")
    except Exception as e:
        context.zip_exists = False
        print(f"✅ Confirmation: fichier n'existe pas: {zip_path}")


@given('un fichier ZIP corrompu existe à "{zip_path}"')
def step_corrupt_zip_exists(context, zip_path):
    """Créer ou utiliser un fichier ZIP corrompu"""
    context.zip_path = zip_path
    context.zip_exists = True
    context.is_corrupt = True
    print(f"⚠️  Fichier corrompu: {zip_path}")


# ==========================================
# QUAND (WHEN)
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
# ALORS (THEN)
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


# ✅ STEP UNIQUE POUR GÉRER TOUS LES CAS
@then('le message d\'erreur doit contenir "{texts}"')
def step_verify_error_contains_any(context, texts):
    """
    Vérifier que le message d'erreur contient un ou plusieurs textes.
    
    Usage:
      - Then le message d'erreur doit contenir "not found"
      - Then le message d'erreur doit contenir "not found" ou "does not exist"
      - Then le message d'erreur doit contenir "corrupt" ou "invalid" ou "broken"
    """
    assert context.unzip_error is not None, \
        "❌ Aucune erreur pour vérifier le message"
    
    error_msg = context.error_message.lower()
    
    # Séparer les textes par " ou "
    possible_texts = [t.strip().strip('"').lower() for t in texts.split(' ou ')]
    
    # Vérifier qu'au moins un des textes est présent
    found = False
    matched_text = None
    
    for text in possible_texts:
        if text in error_msg:
            found = True
            matched_text = text
            break
    
    if found:
        print(f"✅ Message d'erreur contient bien '{matched_text}'")
    else:
        texts_str = "', '".join(possible_texts)
        raise AssertionError(
            f"❌ Message d'erreur ne contient aucun de: '{texts_str}'\n"
            f"   Message réel: {context.error_message}"
        )
