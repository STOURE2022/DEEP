"""
WAX Pipeline
"""

import sys
import os, subprocess
import time, datetime
import traceback
import re
import pandas as pd
from functools import reduce

# Import modules WAX
from waxng.config import Config
from waxng.validator import DataValidator
from waxng.file_processor import FileProcessor
from waxng.column_processor import ColumnProcessor
from waxng.delta_manager import DeltaManager
from waxng.logger_manager import LoggerManager
from waxng.ingestion import IngestionManager
from waxng.dashboard_manager import DashboardManager
from waxng.maintenance_manager import MaintenanceManager
from waxng.simple_report_manager import SimpleReportManager
from waxng.utils import (
    parse_bool, parse_tolerance, normalize_error_dataframe
)

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

try:
    import openpyxl
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl", "--quiet"])
    import openpyxl


def main():
    """Point d'entrée pipeline - Unity Catalog avec traitement séparé des fichiers"""

    # Temps global pour le rapport
    start_total_time = time.time()

    print("*" * 80)
    print("🚀 WAX PIPELINE - MODULE 3 : INGESTION")
    print("*" * 80)

    # ========== CONFIGURATION ==========
    print("\n⚙️  Configuration Unity Catalog...")

    config = Config({
        "catalog": "abu_catalog",
        "schema_files": "databricksassetbundletest",
        "volume": "externalvolumetest",
        "schema_tables": "gdp_poc_dev",
        "env": "dev",
        "version": "v1"
    })

    config.print_config()

    # Validation
    validation = config.validate_paths()
    if not validation["valid"]:
        print("\n⚠️  Avertissements :")
        for issue in validation["issues"]:
            print(f"{issue}")
    else:
        print(f"\n✅ Configuration validée - {validation['mode']}")
        for issue in validation["issues"]:
            if issue.startswith("ℹ️"):
                print(f"{issue}")

    # ========== INITIALISATION SPARK ==========
    print("\n💡 Initialisation Spark...")
    spark = SparkSession.builder.getOrCreate()
    print(f"✅ Spark version : {spark.version}")

    # ========== MANAGERS ==========
    validator = DataValidator(spark)
    file_processor = FileProcessor(spark)
    column_processor = ColumnProcessor(spark, config)
    delta_manager = DeltaManager(spark, config)
    logger_manager = LoggerManager(spark, config)
    ingestion_manager = IngestionManager(spark, config, delta_manager)
    # dashboard_manager = DashboardManager(spark, config)
    maintenance_manager = MaintenanceManager(spark, config)

    print("✅ Managers initialisés\n")

    # ========== LECTURE EXCEL ==========
    print("\n" + "-" * 80)
    print("📘 LECTURE CONFIGURATION EXCEL")
    print("-" * 80)

    excel_path = f"{config.volume_base}/landing/config/wax_config.xlsx"
    print(f"📂 Fichier : {excel_path}")

    try:
        file_columns_df = pd.read_excel(excel_path, sheet_name="Field-Column")
        file_tables_df = pd.read_excel(excel_path, sheet_name="File-Table")
        print(f"✅ Config chargée : {len(file_tables_df)} tables, {len(file_columns_df)} colonnes")
        
        # ✅✅✅ NOUVEAU : EXTRAIRE TOLÉRANCES GLOBALES ✅✅✅
        if len(file_tables_df) > 0:
            # Lire depuis première ligne (même valeur pour toutes les tables)
            first_table = file_tables_df.iloc[0]
            tolerance_rlk_global_str = str(first_table.get("Rejected line per file tolerance", "10%")).strip()
            tolerance_invalid_col_str = str(first_table.get("Invalid column per line tolerance", "20%")).strip()
            
            print(f"\n📊 TOLÉRANCES GLOBALES:")
            print(f"   Rejected line per file:    {tolerance_rlk_global_str}")
            print(f"   Invalid column per line:   {tolerance_invalid_col_str}")
        else:
            tolerance_rlk_global_str = "10%"
            tolerance_invalid_col_str = "20%"
            print(f"⚠️  Utilisation tolérances par défaut: 10% et 20%")
            
    except Exception as e:
        print(f"❌ Erreur lecture Excel : {e}")
        traceback.print_exc()
        return

    # ========== TRAITEMENT TABLES RAW ==========
    print("\n" + "-" * 80)
    print("📗 TRAITEMENT DES TABLES RAW")
    print("-" * 80)

    total_success = 0
    total_rejected = 0  # ✅ NOUVEAU : Compteur pour rejets Fail Fast
    total_failed = 0
    total_files_processed = 0

    for table_idx, trow in file_tables_df.iterrows():
        start_table_time = time.time()
        source_table = trow["Delta Table Name"]
        input_format = str(trow.get("Input Format", "csv")).strip().lower()
        output_zone = str(trow.get("Output Zone", "internal")).strip().lower()
        ingestion_mode = str(trow.get("Ingestion Mode", "")).strip().lower()
        last_table_name = str(trow.get("Last Table Name", "")).strip().lower()

        print("\n" + "-" * 80)
        print(f"🧩 Table {table_idx + 1}/{len(file_tables_df)} : {source_table}")
        print(f"🌍 Zone : {output_zone}, Mode : {ingestion_mode}")
        print("-" * 80)

        trim_flag = parse_bool(trow.get("Trim", True), True)
        staging_table = f"{config.catalog}.{config.schema_tables}.{source_table}_raw"

        # Étape 1 : vérifier si la table existe
        try:
            table_exists = spark.catalog.tableExists(staging_table)
        except Exception as e:
            print(f"⚠️  Erreur vérification table raw : {e}")
            table_exists = False

        if not table_exists:
            print(f"⚠️  Table raw {staging_table} n'existe pas")
            continue

        # Étape 2 : lire la table raw
        try:
            df_staging = spark.table(staging_table)
            staging_count = df_staging.count()
            if staging_count == 0:
                print(f"⚠️  Table staging {staging_table} vide (aucun fichier Auto Loader)")
                continue
            print(f"✅ {staging_count} ligne(s) trouvée(s) dans RAW")
        except Exception as e:
            print(f"❌ Erreur lecture table RAW : {e}")
            logger_manager.log_execution(source_table, "N/A", "staging", ingestion_mode,
                                         output_zone, error_msg=f"RAW table read error: {e}",
                                         status="FAILED", start_time=start_table_time)
            total_failed += 1
            continue

        # ========== RÉCUPÉRER LES FICHIERS SOURCES ==========
        if "FILE_NAME_RECEIVED" in df_staging.columns:
            source_files = [
                row["FILE_NAME_RECEIVED"] for row in df_staging.select("FILE_NAME_RECEIVED").distinct().collect()
            ]
            print(f"📁 {len(source_files)} fichier(s) source détecté(s) : {', '.join(source_files[:3])}")
            if len(source_files) > 3:
                print(f"  ... et {len(source_files) - 3} autres")
        else:
            source_files = ["AUTO_LOADER_BATCH"]
            print(f"⚠️ Colonne FILE_NAME_RECEIVED manquante - traitement en lot")

        # ========== CONFIGURATION COLONNES ==========
        expected_cols = file_columns_df[file_columns_df["Delta Table Name"] == source_table]["Column Name"].tolist()
        column_defs_for_table = file_columns_df[file_columns_df["Delta Table Name"] == source_table].sort_values(
            by=["Field Order"]
        ) if "Field Order" in file_columns_df.columns else None

        specials = file_columns_df[file_columns_df["Delta Table Name"] == source_table].copy()
        if "Is Special" in specials.columns:
            specials["Is Special lower"] = specials["Is Special"].astype(str).str.lower()
            merge_keys = specials[specials["Is Special lower"] == "ismergekey"]["Column Name"].tolist()
        else:
            merge_keys = []

        # ✅✅✅ NOUVEAU : PARSER TOLÉRANCE POUR CETTE TABLE ✅✅✅
        tolerance_rlk_str = str(trow.get("Rejected line per file tolerance", tolerance_rlk_global_str)).strip()
        tolerance_rlk = parse_tolerance(tolerance_rlk_str, 100)  # Parser en ratio (0.0-1.0)
        print(f"\n📊 Tolérance RLK pour {source_table}: {tolerance_rlk_str} = {tolerance_rlk * 100:.1f}%")

        # ========== TRAITEMENT DE CHAQUE FICHIER ==========
        print(f"\n🧮 Traitement séparé de {len(source_files)} fichier(s)")

        for file_idx, filename_current in enumerate(source_files, 1):
            print("\n" + "-" * 80)
            print(f"📄 Fichier {file_idx}/{len(source_files)} : {filename_current}")
            print("-" * 80)

            start_file_time = time.time()

            # ========== FILTRER LES DONNÉES ==========
            if "FILE_NAME_RECEIVED" in df_staging.columns and len(source_files) > 1:
                df_raw = df_staging.filter(F.col("FILE_NAME_RECEIVED") == filename_current)
                print(f"🧾 Filtrage sur FILE_NAME_RECEIVED = {filename_current}")
            else:
                df_raw = df_staging

            total_rows_initial = df_raw.count()
            print(f"📊 {total_rows_initial} ligne(s) pour ce fichier")

            # ✅ NOUVEAU : Parser tolérance pour check_corrupt_records
            rej_tol = parse_tolerance(tolerance_rlk_str, total_rows_initial)

            # VALIDATION COLONNES
            is_valid, err_df = validator.validate_columns_presence(df_raw, expected_cols, source_table, filename_current)
            if not is_valid:
                if err_df.get("df_errors") is not None:
                    err_df_normalized = normalize_error_dataframe(err_df["df_errors"], source_table, filename_current)
                    logger_manager.write_quality_errors(err_df_normalized, source_table, zone=output_zone)
                logger_manager.log_execution(source_table, filename_current, "staging", ingestion_mode, output_zone,
                                             error_msg="Missing columns", status="FAILED", start_time=start_file_time)
                total_failed += 1
                continue

            # Ajouter colonnes manquantes
            if expected_cols:
                for c in expected_cols:
                    if c not in df_raw.columns:
                        df_raw = df_raw.withColumn(c, F.lit(None).cast(StringType()))
                    else:
                        df_raw = df_raw.withColumn(c, df_raw[c].cast(StringType()))

            # CHECK CORRUPT RECORDS
            df_raw, corrupt_rows, should_abort = file_processor.check_corrupt_records(
                df_raw, total_rows_initial, rej_tol, source_table, filename_current
            )
            if should_abort:
                logger_manager.log_execution(
                    source_table, filename_current, "staging", ingestion_mode, output_zone,
                    row_count=total_rows_initial, column_count=len(df_raw.columns),
                    error_msg="Too many corrupt lines", status="FAILED", start_time=start_file_time
                )
                total_failed += 1
                continue

            # TRIM
            if trim_flag:
                for c in df_raw.columns:
                    if c not in ["FILE_NAME_RECEIVED", "FILE_PROCESS_DATE", "yyyy", "mm", "dd"]:
                        df_raw = df_raw.withColumn(c, F.trim(F.col(c)))

            # ✅✅✅ NOUVEAU : TYPAGE COLONNES AVEC TOLÉRANCE ET 4 RETOURS ✅✅✅
            print(f"\n🧠 Typage colonnes pour {source_table}...")
            df_raw, col_errors, invalid_flags, tolerance_exceeded = column_processor.process_columns(
                df=df_raw,
                column_defs=column_defs_for_table,
                table_name=source_table,
                filename=filename_current,
                total_rows=total_rows_initial,
                tolerance_rlk=tolerance_rlk  # ✅ PASSER LA TOLÉRANCE
            )

            # ✅✅✅ VÉRIFIER SI FICHIER REJETÉ (FAIL FAST) ✅✅✅
            if tolerance_exceeded is not None:
                print(f"\n{'='*70}")
                print(f"🚫 FICHIER REJETÉ - FAIL FAST")
                print(f"{'='*70}")
                print(f"Error No:  {tolerance_exceeded.error_no}")
                print(f"Message:   {tolerance_exceeded.message}")
                print(f"Colonne:   {tolerance_exceeded.details.get('column_name', 'N/A')}")
                print(f"Ratio:     {tolerance_exceeded.details['actual_ratio']:.2%}")
                print(f"Seuil:     {tolerance_exceeded.details['threshold']:.2%}")
                print(f"{'='*70}\n")
                
                # Logger le rejet
                try:
                    logger_manager.log_file_rejection(
                        table_name=source_table,
                        filename=filename_current,
                        tolerance_exceeded=tolerance_exceeded,
                        total_rows=total_rows_initial,
                        input_format=input_format,
                        ingestion_mode=ingestion_mode,
                        output_zone=output_zone
                    )
                except Exception as log_err:
                    print(f"⚠️  Erreur logging rejet: {log_err}")
                
                # NE PAS SAUVEGARDER - Passer au fichier suivant
                total_rejected += 1
                duration = round(time.time() - start_file_time, 2)
                print(f"🚫 Fichier {filename_current} rejeté en {duration}s")
                continue  # ← PASSE AU FICHIER SUIVANT

            # ✅ Si on arrive ici, le fichier est OK, continuer le traitement
            print(f"✅ Validation réussie, traitement continue...")

            # REJETER LIGNES INVALIDES
            df_raw, line_errors = column_processor.reject_invalid_lines(
                df_raw, invalid_flags, table_name=source_table, filename=filename_current
            )

            # FUSIONNER LES ERREURS
            all_column_errors = []
            if col_errors:
                all_column_errors.extend(col_errors)
            if line_errors:
                all_column_errors.extend(line_errors)

            df_col_err = None
            if all_column_errors:
                normalized_errors = [
                    normalize_error_dataframe(err_df, table_name=source_table, filename=filename_current)
                    for err_df in all_column_errors
                ]
                df_col_err = reduce(lambda a, b: a.union(b), normalized_errors)

            # VALIDATION QUALITÉ
            df_err_quality = validator.check_data_quality(
                df_raw, source_table, merge_keys, filename=filename_current, column_defs=file_columns_df
            )
            if df_err_quality and df_err_quality.count() > 0:
                df_err_quality = normalize_error_dataframe(df_err_quality, source_table, filename_current)

            # VALIDATION FINALE TYPES
            df_raw, errors_list, column_errors = validator.validate_and_rebuild_dataframe(
                df_raw, column_defs_for_table, source_table, filename_current
            )

            # FUSIONNER TOUTES LES ERREURS
            df_err_global = None
            if df_col_err is not None:
                df_err_global = df_col_err
            if df_err_quality is not None:
                if df_err_global is not None:
                    df_err_global = df_err_global.union(df_err_quality)
                else:
                    df_err_global = df_err_quality
            if errors_list:
                normalized_final_errors = [
                    normalize_error_dataframe(err_df, source_table, filename_current)
                    for err_df in errors_list
                ]
                df_final_err = reduce(lambda a, b: a.union(b), normalized_final_errors)
                df_err_global = df_final_err if df_err_global is None else df_err_global.union(df_final_err)

            # INGESTION
            print(f"\n📥 Ingestion fichier : {filename_current}")
            print(f"🧭 Mode : {ingestion_mode}")
            print(f"📦 Destination : {config.catalog}.{config.schema_tables}.{source_table}_{{all,last}}")

            parts = {}
            if "yyyy" in df_raw.columns:
                yyyy_val = df_raw.select("yyyy").first()
                if yyyy_val and yyyy_val.yyyy:
                    parts["yyyy"] = yyyy_val.yyyy
            if "mm" in df_raw.columns:
                mm_val = df_raw.select("mm").first()
                if mm_val and mm_val.mm:
                    parts["mm"] = mm_val.mm
            if "dd" in df_raw.columns:
                dd_val = df_raw.select("dd").first()
                if dd_val and dd_val.dd:
                    parts["dd"] = dd_val.dd

            try:
                ingestion_manager.apply_ingestion_mode(
                    df_raw,
                    column_defs=column_defs_for_table,
                    table_name=source_table,
                    ingestion_mode=ingestion_mode,
                    zone=output_zone,
                    parts=parts,
                    file_name_received=filename_current,
                    table_config=last_table_name
                )
            except Exception as e:
                print(f"❌ Erreur ingestion : {e}")
                traceback.print_exc()
                logger_manager.log_execution(
                    source_table, filename_current, "staging", ingestion_mode,
                    output_zone, row_count=0, column_count=len(df_raw.columns),
                    error_msg=f"Ingestion error: {e}", status="FAILED",
                    start_time=start_file_time
                )
                total_failed += 1
                continue

            # MÉTRIQUES & LOGS
            metrics = logger_manager.calculate_final_metrics(df_raw, df_err_global)

            logger_manager.print_summary(
                table_name=source_table,
                filename=filename_current,
                total_rows=total_rows_initial,
                corrupt_rows=metrics["corrupt_rows"],
                anomalies_total=metrics["anomalies_total"],
                cleaned_rows=metrics["cleaned_rows"],
                errors_df=df_err_global
            )

            logger_manager.write_quality_errors(df_err_global, source_table, zone=output_zone)

            logger_manager.log_execution(
                source_table, filename_current, "staging",
                ingestion_mode, output_zone,
                row_count=total_rows_initial,
                column_count=len(df_raw.columns),
                error_msg=f"{metrics['anomalies_total']} errors" if metrics["anomalies_total"] > 0 else None,
                status="SUCCESS", error_count=metrics["anomalies_total"],
                start_time=start_file_time
            )

            duration = round(time.time() - start_file_time, 2)
            print(f"✅ Fichier {filename_current} traité en {duration}s")
            total_success += 1
            total_files_processed += 1

    # ✅✅✅ NOUVEAU : RÉSUMÉ FINAL AVEC REJETS ✅✅✅
    execution_time = time.time() - start_total_time
    total_files = total_files_processed + total_rejected + total_failed
    
    print("\n" + "=" * 80)
    print("🏁 TRAITEMENT TERMINÉ")
    print("=" * 80 + "\n")

    print(f"📊 STATISTIQUES:")
    print(f"   Total fichiers traités:    {total_files}")
    print(f"   ✅ Succès:                 {total_files_processed}")
    print(f"   🚫 Rejetés (Fail Fast):    {total_rejected}")
    print(f"   ❌ Erreurs:                {total_failed}")
    print(f"   ⏱️  Durée totale:           {execution_time:.1f}s")
    print(f"")

    if total_rejected > 0:
        print(f"⚠️  {total_rejected} fichier(s) rejeté(s) suite à dépassement de seuil")
        print(f"   → Consultez la table {config.catalog}.{config.schema_tables}.wax_execution_logs")
        print(f"   → Filtrez sur status='REJECTED' pour voir les détails")
        print(f"")
    
    if total_failed > 0:
        print(f"⚠️  {total_failed} fichier(s) en erreur")
        print(f"")

    print("=" * 80 + "\n")

    # RAPPORT SIMPLIFIÉ
    if total_files_processed > 0 or total_failed > 0 or total_rejected > 0:
        try:
            report_manager = SimpleReportManager(spark, config)
            report_manager.generate_simple_report(
                total_files_processed=total_files_processed,
                total_failed=total_failed + total_rejected,  # Rejets comptés comme failed pour le rapport
                execution_time=execution_time
            )
        except Exception as e:
            print(f"⚠️  Erreur génération rapport : {e}")
            traceback.print_exc()

    # DASHBOARDS
    # if total_files_processed > 0:
    #     try:
    #         print("\n📊 Dashboards disponibles via dashboard_manager.display_all_dashboards()")
    #         dashboard_manager.display_all_dashboards()
    #     except Exception as e:
    #         print(f"⚠️ Info dashboards : {e}")

    # print("\n🎯 Pipeline terminé !")

    # Retourne le code de sortie
    if total_failed > 0:
        return 1  # Erreurs critiques
    elif total_rejected > 0 and total_files_processed == 0:
        return 2  # Tous rejetés
    else:
        return 0  # Succès


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
