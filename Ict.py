elif err_action == "ICT_DRIVEN":
    # Calculer le ratio d'erreurs
    error_ratio = invalid_count / float(total_rows) if total_rows > 0 else 0
    error_percentage = error_ratio * 100
    
    # VÉRIFIER si le seuil est dépassé
    if error_percentage > tolerance:
        # ✅ LEVER UNE EXCEPTION
        from exceptions import ToleranceExceededError
        
        print(
            f"⛔ ICT_DRIVEN FAIL FAST: {invalid_count}/{total_rows} "
            f"({error_percentage:.1f}%) d'erreurs > seuil ({tolerance:.1f}%)"
        )
        
        # ✅ LEVER L'EXCEPTION avec error_no
        raise ToleranceExceededError(
            tolerance_type="RLK",  # Rejected Lines Keys
            actual=error_ratio,
            threshold=tolerance / 100.0,  # Convertir en décimal
            table_name=table_name,
            filename=filename
        )
    else:
        # Sous le seuil : continuer avec flag
        print(
            f"⚠️ ICT_DRIVEN: {invalid_count} valeur(s) invalide(s) pour {cname} "
            f"({error_percentage:.1f}% < {tolerance:.1f}%)"
        )
        flag_col = f"{cname}_invalid"
        df = df.withColumn(flag_col, invalid_cond.cast("int"))
        
        # Logger l'erreur
        errs_summary = self.spark.createDataFrame(
            [
                (
                    table_name,
                    filename,
                    cname,
                    f"ICT_DRIVEN: {invalid_count}/{total_rows} invalid ({error_percentage:.1f}%)",
                    ", ".join(sample_values[:3]),
                    invalid_count,
                )
            ],
            self.ERROR_SCHEMA,
        )
        errors.append(errs_summary)
