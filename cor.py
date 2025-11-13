try:
    file_columns_df = pd.read_excel(excel_path, sheet_name="Field-Column")
    file_tables_df = pd.read_excel(excel_path, sheet_name="File-Table")
    print(f"✅ Config chargée : {len(file_tables_df)} tables, {len(file_columns_df)} colonnes")
    
    # ✅✅✅ EXTRAIRE TOLÉRANCES GLOBALES ✅✅✅
    if len(file_tables_df) > 0:
        # Lire depuis première ligne (même valeur pour toutes les tables)
        first_table = file_tables_df.iloc[0]
        tolerance_rlk_str = str(first_table.get("Rejected line per file tolerance", "10%")).strip()
        tolerance_invalid_col_str = str(first_table.get("Invalid column per line tolerance", "20%")).strip()
        
        print(f"\n📊 TOLÉRANCES GLOBALES:")
        print(f"   Rejected line per file:    {tolerance_rlk_str}")
        print(f"   Invalid column per line:   {tolerance_invalid_col_str}")
    else:
        tolerance_rlk_str = "10%"
        tolerance_invalid_col_str = "20%"
        print(f"⚠️  Utilisation tolérances par défaut: 10% et 20%")
        
except Exception as e:
    print(f"❌ Erreur lecture Excel : {e}")
    traceback.print_exc()
    return


# ✅ NOUVEAU CODE - Parser tolérance
tolerance_rlk_str = str(trow.get("Rejected line per file tolerance", "10%")).strip()
tolerance_rlk = parse_tolerance(tolerance_rlk_str, 100)  # Parser en ratio
print(f"📊 Tolérance RLK pour cette table: {tolerance_rlk_str} = {tolerance_rlk * 100:.1f}%")

# Garder aussi rej_tol pour check_corrupt_records
rej_tol = parse_tolerance(tolerance_rlk_str, total_rows_initial)


# ✅ NOUVEAU - Avec tolérance et 4 valeurs de retour
# TYPAGE COLONNES
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
    logger_manager.log_file_rejection(
        table_name=source_table,
        filename=filename_current,
        tolerance_exceeded=tolerance_exceeded,
        total_rows=total_rows_initial,
        input_format=input_format,
        ingestion_mode=ingestion_mode,
        output_zone=output_zone
    )
    
    # NE PAS SAUVEGARDER - Passer au fichier suivant
    total_failed += 1
    continue  # ← Passe au fichier suivant dans la boucle


# RÉSUMÉ FINAL
execution_time = time.time() - start_total_time
print("\n" + "=" * 80)
print("🏁 TRAITEMENT TERMINÉ")
print("=" * 80 + "\n")

print(f"📊 STATISTIQUES:")
print(f"   Total fichiers traités:  {total_files_processed + total_failed}")
print(f"   ✅ Succès:               {total_files_processed}")
print(f"   🚫 Rejetés (Fail Fast):  {total_failed}")  # ← Maintenant ce sont les rejets
print(f"   ⏱️  Durée totale:         {execution_time:.1f}s")
print(f"")

if total_failed > 0:
    print(f"⚠️  {total_failed} fichier(s) rejeté(s) - Voir table wax_execution_logs pour détails")
    print(f"")

print("=" * 80 + "\n")
```

---

## 📝 **RÉSUMÉ DES CHANGEMENTS**

| Ligne | Action | Code |
|-------|--------|------|
| **~132** | Ajouter | Extraction tolérances globales |
| **~196** | Modifier | Parser `tolerance_rlk` avant la boucle fichiers |
| **~230** | Remplacer | Appel `process_columns` avec 4 retours + `tolerance_rlk` |
| **~232** | Ajouter | Vérification `if tolerance_exceeded is not None:` + continue |
| **~325** | Modifier | Récapitulatif avec stats "Rejetés" |

---

## ✅ **RÉSULTAT ATTENDU**
```
📊 TOLÉRANCES GLOBALES:
   Rejected line per file:    20%
   Invalid column per line:   20%

📄 Fichier 1/3 : site_20251112_173355
📊 Tolérance RLK pour cette table: 20% = 20.0%
⛔ ICT_DRIVEN FAIL FAST: 3/3 (100.0%) > seuil (20.0%)

🚫 FICHIER REJETÉ - FAIL FAST
Error No:  000000009
✅ Rejet loggé

📄 Fichier 2/3 : site_20251113_101200
✅ Validation réussie
✅ Fichier traité

🏁 TRAITEMENT TERMINÉ
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 STATISTIQUES:
   Total fichiers traités:  2
   ✅ Succès:               1
   🚫 Rejetés (Fail Fast):  1
   ⏱️  Durée totale:         45.3s
