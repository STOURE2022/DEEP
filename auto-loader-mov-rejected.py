def _move_rejected_files(self, rejected_files: list):
    """
    Déplace les fichiers rejetés vers rejected/
    
    ✅ NOUVEAU: Logger avec error_no dans les logs qualité
    """
    import shutil
    from logger_manager import LoggerManager
    
    if not rejected_files:
        return
    
    print(f"\n🗑️  Déplacement de {len(rejected_files)} fichier(s) rejeté(s)...")
    
    # Créer répertoire rejected si nécessaire
    os.makedirs(self.rejected_base, exist_ok=True)
    
    # ✅ NOUVEAU: Préparer données pour logging qualité
    error_logs = []
    
    for rejected in rejected_files:
        file_path = rejected["file_path"]
        filename = rejected["filename"]
        reason = rejected["reason"]
        error_no = rejected.get("error_no", "000000008")  # Default: FileRejected
        table_name = rejected.get("table_name", "UNKNOWN")
        
        # Déplacer fichier
        try:
            dest_path = os.path.join(self.rejected_base, filename)
            
            # Si fichier existe déjà, ajouter timestamp
            if os.path.exists(dest_path):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                base, ext = os.path.splitext(filename)
                dest_path = os.path.join(
                    self.rejected_base, 
                    f"{base}_{timestamp}{ext}"
                )
            
            shutil.move(file_path, dest_path)
            print(f"   ✓ {filename} → rejected/")
            
            # ✅ NOUVEAU: Préparer log d'erreur
            error_logs.append({
                "table_name": table_name,
                "filename": filename,
                "column_name": "filename",
                "error_message": reason,
                "raw_value": filename,
                "error_no": error_no,  # ✅ NOUVEAU
                "error_count": 1
            })
            
        except Exception as e:
            print(f"   ✗ Erreur déplacement {filename}: {e}")
    
    # ✅ NOUVEAU: Logger dans wax_data_quality_errors avec error_no
    if error_logs and hasattr(self, 'config'):
        try:
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType
            
            schema = StructType([
                StructField("table_name", StringType(), True),
                StructField("filename", StringType(), True),
                StructField("column_name", StringType(), True),
                StructField("error_message", StringType(), True),
                StructField("raw_value", StringType(), True),
                StructField("error_no", StringType(), True),  # ✅ NOUVEAU
                StructField("error_count", IntegerType(), True),
            ])
            
            df_errors = self.spark.createDataFrame(
                [(
                    log["table_name"],
                    log["filename"],
                    log["column_name"],
                    log["error_message"],
                    log["raw_value"],
                    log["error_no"],
                    log["error_count"]
                ) for log in error_logs],
                schema=schema
            )
            
            # Logger dans logger_manager si disponible
            logger = LoggerManager(self.spark, self.config)
            logger.write_quality_errors(df_errors, "AUTOLOADER", zone="raw")
            
            print(f"✅ {len(error_logs)} erreur(s) loggée(s) avec error_no")
            
        except Exception as e:
            print(f"⚠️  Impossible de logger les erreurs: {e}")
