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
    Log le rejet d'un fichier (Fail Fast).
    
    Args:
        tolerance_exceeded: Instance de ToleranceExceededError
    """
    self._ensure_execution_table_exists()
    
    today = datetime.today()
    
    # Extraire détails de l'exception
    details = tolerance_exceeded.details
    error_message = (
        f"[{tolerance_exceeded.error_no}] REJECTED - {tolerance_exceeded.message} | "
        f"Column: {details.get('column_name', 'N/A')}, "
        f"Tolerance: {details['threshold']:.2%}, "
        f"Actual: {details['actual_ratio']:.2%}"
    )
    
    row_data = [(
        str(table_name),
        str(filename),
        str(input_format),
        str(ingestion_mode),
        str(output_zone),
        int(total_rows),      # Lignes dans le fichier
        0,                     # column_count (pas encore traité)
        False,                 # masking_applied
        0,                     # error_count (fichier entier rejeté)
        error_message,         # Message détaillé
        "REJECTED",            # ✅ STATUS
        0.0,                   # duration
        datetime.now(),        # log_ts
        today.year,
        today.month,
        today.day,
    )]
    
    schema = self._get_execution_schema()
    df_log = self.spark.createDataFrame(row_data, schema=schema)
    
    try:
        df_log.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).partitionBy("yyyy", "mm", "dd").saveAsTable(self.exec_table)
        
        print(f"✅ Rejet loggé : {filename} | Error {tolerance_exceeded.error_no}")
        
    except Exception as e:
        print(f"⚠️ Erreur logging rejet : {e}")
