def _add_metadata(self, df_stream):
    """
    Ajoute les colonnes de métadonnées au DataFrame stream.
    
    GÈRE TOUS LES FORMATS DE NOMS DE FICHIERS:
    - Avec date: data_20251108.csv
    - Avec date et heure: data_20251108_143022.csv ou data_20251108143022.csv
    - Sans date: data.csv (utilise date du jour)
    - Formats alternatifs: data-20251108.csv, data20251108.csv, etc.
    
    Args:
        df_stream: DataFrame stream avec métadonnées _metadata
        
    Returns:
        DataFrame avec colonnes de métadonnées ajoutées
    """
    from pyspark.sql.functions import (
        col, regexp_extract, to_date, 
        year, month, dayofmonth, current_timestamp,
        when, lit, coalesce, expr
    )
    
    # 1. Extraire le nom du fichier depuis _metadata.file_path
    # Exemple: dbfs:/mnt/path/data_20251108.csv -> data_20251108
    df_stream = df_stream.withColumn(
        "FILE_NAME_RECEIVED",
        regexp_extract(col("_metadata.file_path"), r"([^/]+)\.csv$", 1)
    )
    
    # 2. Sauvegarder FILE_NAME_RECEIVED dans une colonne temporaire
    df_stream = df_stream.withColumn(
        "_temp_filename",
        col("FILE_NAME_RECEIVED")
    )
    
    # 3. TENTATIVE 1: Extraire date format YYYYMMDD (avec séparateurs _ ou -)
    # Patterns supportés:
    # - data_20251108
    # - data-20251108
    # - data20251108
    df_stream = df_stream.withColumn(
        "_temp_date_str_v1",
        regexp_extract(col("_temp_filename"), r"[_-]?(\d{8})[_-]?", 1)
    )
    
    # 4. TENTATIVE 2: Extraire date+heure format YYYYMMDD_HHMMSS ou YYYYMMDDHHMMSS
    # Patterns supportés:
    # - data_20251108_143022
    # - data_20251108143022
    # On extrait juste la partie date (les 8 premiers chiffres)
    df_stream = df_stream.withColumn(
        "_temp_date_str_v2",
        regexp_extract(col("_temp_filename"), r"[_-]?(\d{8})\d{6}[_-]?", 1)
    )
    
    # 5. TENTATIVE 3: Extraire date format YYYYMMDD suivi de _HHMMSS
    df_stream = df_stream.withColumn(
        "_temp_date_str_v3",
        regexp_extract(col("_temp_filename"), r"[_-]?(\d{8})[_-]\d{6}", 1)
    )
    
    # 6. Choisir la meilleure extraction (la première non-vide)
    df_stream = df_stream.withColumn(
        "_temp_date_str",
        when(
            col("_temp_date_str_v1") != "", col("_temp_date_str_v1")
        ).when(
            col("_temp_date_str_v2") != "", col("_temp_date_str_v2")
        ).when(
            col("_temp_date_str_v3") != "", col("_temp_date_str_v3")
        ).otherwise(
            lit("")  # Aucune date trouvée
        )
    )
    
    # 7. Créer FILE_EXTRACTION_DATE
    # Si date trouvée: la convertir
    # Sinon: NULL (sera géré plus tard avec fallback)
    df_stream = df_stream.withColumn(
        "FILE_EXTRACTION_DATE",
        when(
            (col("_temp_date_str") == "") | (col("_temp_date_str").isNull()),
            lit(None).cast("date")  # Pas de date trouvée
        ).otherwise(
            # Utiliser try_cast pour éviter les erreurs si le format est invalide
            expr("try_cast(to_date(_temp_date_str, 'yyyyMMdd') as date)")
        )
    )
    
    # 8. Créer une date de fallback pour les colonnes de partition
    # Si FILE_EXTRACTION_DATE est NULL, utiliser la date du jour
    df_stream = df_stream.withColumn(
        "_partition_date",
        coalesce(
            col("FILE_EXTRACTION_DATE"),
            current_timestamp().cast("date")  # Fallback: date du jour
        )
    )
    
    # 9. Créer les colonnes de partitionnement yyyy, mm, dd
    # Utilise _partition_date qui ne peut jamais être NULL
    df_stream = df_stream.withColumn(
        "yyyy",
        year(col("_partition_date"))
    )
    
    df_stream = df_stream.withColumn(
        "mm",
        month(col("_partition_date"))
    )
    
    df_stream = df_stream.withColumn(
        "dd",
        dayofmonth(col("_partition_date"))
    )
    
    # 10. Ajouter FILE_PROCESS_DATE (timestamp actuel)
    df_stream = df_stream.withColumn(
        "FILE_PROCESS_DATE",
        current_timestamp()
    )
    
    # 11. Nettoyer TOUTES les colonnes temporaires
    df_stream = df_stream.drop(
        "_temp_filename",
        "_temp_date_str_v1",
        "_temp_date_str_v2",
        "_temp_date_str_v3",
        "_temp_date_str",
        "_partition_date",
        "_metadata"
    )
    
    return df_stream
