# Appel à process_columns
df_raw, col_errors, invalid_flags, tolerance_exceeded = column_processor.process_columns(
    df=df_raw,
    column_defs=column_defs_for_table,
    table_name=source_table,
    filename=filename_current,
    total_rows=total_rows_initial,
    tolerance_rlk=tolerance_rejected_line
)

# ✅ VÉRIFIER SI FICHIER REJETÉ
if tolerance_exceeded is not None:
    print(f"\n{'='*70}")
    print(f"🚫 FICHIER REJETÉ - FAIL FAST")
    print(f"{'='*70}")
    print(f"Error No: {tolerance_exceeded.error_no}")
    print(f"Colonne:  {tolerance_exceeded.details.get('column_name')}")
    print(f"Ratio:    {tolerance_exceeded.details['actual_ratio']:.2%}")
    print(f"Seuil:    {tolerance_exceeded.details['threshold']:.2%}")
    print(f"{'='*70}\n")
    
    # Logger le rejet (dans votre table de logs)
    # log_file_rejection(spark, source_table, filename_current, tolerance_exceeded)
    
    # ✅ NE PAS SAUVEGARDER - Passer au fichier suivant
    continue  # Si dans une boucle sur fichiers
    # OU
    # return  # Si traitement d'un seul fichier
