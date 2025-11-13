# Après process_columns
if tolerance_exceeded is not None:
    print(f"\n🚫 FICHIER REJETÉ - FAIL FAST")
    
    # ✅ LOGGER avec LoggerManager
    logger_manager.log_file_rejection(
        table_name=source_table,
        filename=filename_current,
        tolerance_exceeded=tolerance_exceeded,
        total_rows=total_rows_initial,
        input_format="csv",  # Adapter selon votre config
        ingestion_mode=ingestion_mode,
        output_zone=zone_output
    )
    
    # Ne pas sauvegarder
    continue
