# 🔴 **PROBLÈME : Erreur de Logging**

```
⚠️ Erreur logging rejet: [AXIS_LENGTH_MISMATCH] Length mismatch: Expected axis has 18 elements...
```

**Cause :** Le schéma de votre table `wax_execution_logs` a **18 colonnes** mais vous n'en fournissez que **16**.

---

## ✅ **SOLUTION - Corriger `log_file_rejection` dans logger_manager.py**

### **REMPLACER la méthode complète :**

```python
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
    """
    self._ensure_execution_table_exists()
    
    today = datetime.today()
    
    # Extraire détails de l'exception
    details = tolerance_exceeded.details
    error_message = (
        f"[{tolerance_exceeded.error_no}] REJECTED - {tolerance_exceeded.message}"
    )
    
    # ✅ CRÉER LE ROW AVEC EXACTEMENT 16 COLONNES (comme le schéma)
    row_data = [(
        str(table_name),           # 1. table_name
        str(filename),             # 2. filename
        str(input_format),         # 3. input_format
        str(ingestion_mode),       # 4. ingestion_mode
        str(output_zone),          # 5. output_zone
        int(total_rows),           # 6. row_count
        0,                         # 7. column_count (pas encore traité)
        False,                     # 8. masking_applied
        0,                         # 9. error_count (fichier entier rejeté)
        error_message,             # 10. error_message
        "REJECTED",                # 11. status
        0.0,                       # 12. duration
        datetime.now(),            # 13. log_ts
        today.year,                # 14. yyyy
        today.month,               # 15. mm
        today.day,                 # 16. dd
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
```

---

## 📊 **Pour logger les ERREURS DE QUALITÉ**

Si vous voulez **aussi** logger les erreurs de colonnes dans `wax_data_quality_errors`, ajoutez ceci **dans main.py** AVANT le `continue` :

```python
# Dans main.py, après la détection du Fail Fast (ligne ~305)

if tolerance_exceeded is not None:
    print(f"\n{'='*70}")
    print(f"🚫 FICHIER REJETÉ - FAIL FAST")
    print(f"{'='*70}")
    # ... affichage ...
    
    # ✅ Logger dans wax_execution_logs (rejet du fichier)
    logger_manager.log_file_rejection(
        table_name=source_table,
        filename=filename_current,
        tolerance_exceeded=tolerance_exceeded,
        total_rows=total_rows_initial,
        input_format=input_format,
        ingestion_mode=ingestion_mode,
        output_zone=output_zone
    )
    
    # ✅✅✅ NOUVEAU : Logger aussi dans wax_data_quality_errors (détail de l'erreur)
    # Créer un DataFrame d'erreur pour la colonne problématique
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    
    error_schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("raw_value", StringType(), True),
        StructField("error_count", IntegerType(), True),
    ])
    
    error_data = [(
        source_table,
        filename_current,
        tolerance_exceeded.details.get('column_name', 'N/A'),
        f"FAIL FAST - {tolerance_exceeded.message}",
        None,  # raw_value
        int(total_rows_initial * tolerance_exceeded.details['actual_ratio'])
    )]
    
    df_quality_error = spark.createDataFrame(error_data, error_schema)
    
    # Logger dans la table de qualité
    logger_manager.write_quality_errors(
        df_quality_error, 
        table_name=source_table, 
        zone=output_zone
    )
    
    # Passer au fichier suivant
    total_rejected += 1
    duration = round(time.time() - start_file_time, 2)
    print(f"🚫 Fichier {filename_current} rejeté en {duration}s")
    continue
```

---

## 🎯 **RÉSULTAT ATTENDU**

### **Table `wax_execution_logs` :**
| status | error_message | filename | row_count |
|--------|---------------|----------|-----------|
| **REJECTED** | [000000009] REJECTED - Column 'contractid': RLK tolerance exceeded... | site_20251112_173355 | 3 |

### **Table `wax_data_quality_errors` :**
| table_name | filename | column_name | error_message | error_count |
|------------|----------|-------------|---------------|-------------|
| site_clymene | site_20251112_173355 | contractid | FAIL FAST - Column 'contractid': RLK tolerance exceeded: 100.00% > 0.10% | 3 |

---

## 📝 **VÉRIFIER VOS LOGS**

```sql
-- Vérifier les rejets
SELECT * 
FROM abu_catalog.gdp_poc_dev.wax_execution_logs 
WHERE status = 'REJECTED' 
ORDER BY log_ts DESC;

-- Vérifier les erreurs de qualité
SELECT * 
FROM abu_catalog.gdp_poc_dev.wax_data_quality_errors 
WHERE error_message LIKE '%FAIL FAST%'
ORDER BY log_ts DESC;
```

---

**Le problème était le mismatch entre le nombre de colonnes dans le schéma (16) et ce que la table attend (18). La solution : utiliser exactement le schéma défini avec les 16 colonnes.** 🎯
