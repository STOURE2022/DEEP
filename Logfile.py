def log_file_rejection(
    self,
    table_name: str,
    filename: str,
    tolerance_exceeded,  # ToleranceExceededError
    total_rows: int,
    input_format: str = "csv",
    ingestion_mode: str = "FULL_SNAPSHOT",
    output_zone: str = "internal",
):
    """
    Log le rejet d'un fichier (Fail Fast) dans wax_execution_logs.
    
    Args:
        tolerance_exceeded: Instance de ToleranceExceededError
    """
    self._ensure_execution_table_exists()
    
    today = datetime.today()
    
    # Extraire détails de l'exception
    details = tolerance_exceeded.details
    error_message = f"[{tolerance_exceeded.error_no}] REJECTED - {tolerance_exceeded.message}"
    
    # ✅ CRÉER LE ROW AVEC EXACTEMENT 18 COLONNES
    row_data = [(
        str(table_name),                    # 1. table_name
        str(filename),                      # 2. filename
        str(input_format),                  # 3. input_format
        str(ingestion_mode),                # 4. ingestion_mode
        str(output_zone),                   # 5. output_zone
        int(total_rows),                    # 6. row_count
        0,                                  # 7. column_count (pas encore traité)
        False,                              # 8. masking_applied
        0,                                  # 9. error_count (fichier entier rejeté)
        error_message,                      # 10. error_message
        str(tolerance_exceeded.error_no),   # 11. error_no ← AJOUTER
        "REJECTED",                         # 12. status
        0.0,                                # 13. duration
        str(self.config.env),               # 14. env ← AJOUTER
        datetime.now(),                     # 15. log_ts
        today.year,                         # 16. yyyy
        today.month,                        # 17. mm
        today.day,                          # 18. dd
    )]
    
    # Utiliser le schéma défini
    schema = self._get_execution_schema()
    df_log = self.spark.createDataFrame(row_data, schema=schema)
    
    try:
        df_log.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).partitionBy("yyyy", "mm", "dd").saveAsTable(self.exec_table)
        
        print(f"✅ Rejet loggé dans {self.exec_table}")
        
    except Exception as e:
        print(f"⚠️ Erreur logging rejet : {e}")
        # Afficher le détail pour debug
        import traceback
        traceback.print_exc()
