"""
column_processor.py
Traitement et typage des colonnes avec gestion des erreurs
ICT_DRIVEN, REJECT, LOG_ONLY
"""

from functools import reduce
import operator
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    TimestampType,
    StructType,
    StructField,
    StringType,
    IntegerType,
)
from .utils import parse_bool, parse_tolerance, TYPE_MAPPING
from .exceptions import ToleranceExceededError  # ✅ NOUVEAU IMPORT


class ColumnProcessor:
    """Processeur de colonnes avec typage et validation"""

    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.ERROR_SCHEMA = self._build_error_schema()

    def _build_error_schema(self):
        """Schéma unifié pour erreurs"""
        return StructType(
            [
                StructField("table_name", StringType(), True),
                StructField("filename", StringType(), True),
                StructField("column_name", StringType(), True),
                StructField("error_message", StringType(), True),
                StructField("raw_value", StringType(), True),
                StructField("error_count", IntegerType(), True),
            ]
        )

    def parse_date_with_logs(
        self,
        df: DataFrame,
        cname: str,
        pattern: list,
        table_name: str,
        filename: str,
        error_action: str,
    ) -> tuple:
        """
        Parse dates avec validation STRICTE et logging.
        Si input ne match pas le pattern défini -> Error Action Rule + NULL
        Format sortie : timestamp (yyyy-MM-dd HH:mm:ss)

        Args:
            df: DataFrame
            cname: Nom de la colonne
            pattern: Pattern de transformation (UN SEUL, pas de fallback)
            table_name: Nom de la table
            filename: Nom du fichier
            error_action: Action en cas d'erreur (ICT_DRIVEN, REJECT, LOG_ONLY)

        Returns:
            (df_parsed, errors_df)
        """

        raw_col = F.col(cname)

        # Nettoyer : vides -> NULL
        col_expr = F.when(F.length(F.trim(raw_col)) == 0, F.lit(None)).otherwise(
            raw_col
        )

        # Essayer UNIQUEMENT le pattern défini dans la conf
        ts_col = F.expr(f"try_to_timestamp({cname}, {pattern})")

        # ✅ Format sortie : TIMESTAMP (pas DATE)
        parsed_timestamp = ts_col
        print(f"🕓 La sortie du parsed_timestamp : ", parsed_timestamp)

        # Identifier les erreurs
        df_with_parsed = df.withColumn(f"{cname}_parsed", parsed_timestamp)

        # Condition d'erreur : input non-vide mais parsing échoué
        error_condition = (
            F.col(f"{cname}_parsed").isNull()
            & F.col(cname).isNotNull()
            & (F.trim(F.col(cname)) != "")
        )

        # Compter erreurs
        error_count = df_with_parsed.filter(error_condition).count()

        if error_count > 0:
            print(
                f"⚠️ {error_count} erreur(s) parsing date sur {cname} (pattern: {pattern})"
            )

            # Créer DataFrame d'erreurs
            errs = (
                df_with_parsed.filter(error_condition)
                .limit(1000)
                .select(
                    F.lit(table_name).alias("table_name"),
                    F.lit(filename).alias("filename"),
                    F.lit(cname).alias("column_name"),
                    F.concat(
                        F.lit(
                            f"DATE_PARSE_ERROR: Pattern '{pattern}' ne match pas. Valeur: "
                        ),
                        raw_col.cast("string"),
                    ).alias("error_message"),
                    raw_col.cast("string").alias("raw_value"),
                    F.lit(1).alias("error_count"),
                )
            )
        else:
            errs = self.spark.createDataFrame([], self.ERROR_SCHEMA)

        # Appliquer Error Action
        if error_action == "REJECT" and error_count > 0:
            # Supprimer les lignes invalides
            df_with_parsed = df_with_parsed.filter(~error_condition)
            print(f"🧹 REJECT : {error_count} ligne(s) supprimée(s)")

        # Colonne finale = parsed (NULL si échec)
        df_final = df_with_parsed.withColumn(cname, F.col(f"{cname}_parsed")).drop(
            f"{cname}_parsed"
        )

        return df_final, errs

    def process_columns(
        self,
        df: DataFrame,
        column_defs,
        table_name: str,
        filename: str,
        total_rows: int,
    ) -> tuple:
        """
        Traite toutes les colonnes avec typage et gestion d'erreurs

        Returns:
            (df_processed, all_errors, invalid_flags)
        """
        print(f"🧠 Typage colonnes pour {table_name}...")

        invalid_flags = []
        all_column_errors = []

        for _, crow in column_defs.iterrows():
            cname = crow["Column Name"]
            if cname not in df.columns:
                continue

            # Configuration colonne
            stype_str = str(crow.get("Field type", "STRING")).strip().upper()
            tr_type = str(crow.get("Transformation Type", "")).strip().lower()
            tr_patt = str(crow.get("Transformation pattern", "")).strip()
            regex_repl = str(crow.get("Regex replacement", "")).strip()
            is_nullable = parse_bool(crow.get("Is Nullable", "False"), False)
            err_action = str(crow.get("Error action", "ICT_DRIVEN")).strip().upper()
            if err_action in ["", "NAN", "NONE", "NULL"]:
                err_action = "ICT_DRIVEN"

            default_inv = str(crow.get("Default when invalid", "")).strip()

            # Transformations texte
            if tr_type == "uppercase":
                df = df.withColumn(cname, F.upper(F.col(cname)))
            elif tr_type == "lowercase":
                df = df.withColumn(cname, F.lower(F.col(cname)))
            elif tr_type == "regex" and tr_patt:
                df = df.withColumn(
                    cname,
                    F.regexp_replace(
                        F.col(cname), tr_patt, regex_repl if regex_repl else ""
                    ),
                )

            # Typage selon type
            stype = self._get_spark_type(stype_str)

            if isinstance(stype, (DateType, TimestampType)):
                # Dates : Validation stricte avec le pattern défini UNIQUEMENT
                if not tr_patt:
                    # Si pas de pattern défini, utiliser premier pattern par défaut
                    tr_patt = self.config.date_patterns[0]
                    print(
                        f"🕓 Pas de pattern pour {cname}, utilisation par défaut : {tr_patt}"
                    )

                df, errs = self.parse_date_with_logs(
                    df, cname, tr_patt, table_name, filename, err_action
                )

                if errs.limit(1).count() > 0:  # force action Spark
                    all_column_errors.append(errs)
            else:
                # Types numériques et autres
                df, col_errors, flag_col = self.process_numeric_column(
                    df,
                    cname,
                    stype_str,
                    is_nullable,
                    err_action,
                    table_name,
                    filename,
                    total_rows,
                    crow,
                )
                if col_errors:
                    all_column_errors.extend(col_errors)
                if flag_col:
                    invalid_flags.append(flag_col)

        return df, all_column_errors, invalid_flags

    def process_numeric_column(
        self,
        df: DataFrame,
        cname: str,
        stype_str: str,
        is_nullable: bool,
        err_action: str,
        table_name: str,
        filename: str,
        total_rows: int,
        col_config,
    ) -> tuple:
        """
        Traite une colonne numérique avec gestion REJECT/ICT_DRIVEN/LOG_ONLY
        
        ✅ MODIFICATION MAJEURE: Lève ToleranceExceededError si seuil dépassé en ICT_DRIVEN
        """

        #
        # ÉTAPE 1 : SAUVEGARDER ORIGINALE
        # ===============================================================

        df = df.withColumn(f"{cname}_original", F.col(cname))

        #
        # ÉTAPE 2 : CRÉER LA COLONNE CAST
        # ===============================================================

        df = df.withColumn(f"{cname}_cast", F.expr(f"try_cast({cname} as {stype_str})"))

        #
        # ÉTAPE 3 : VALIDATION (maintenant _cast existe !)
        # ===============================================================

        # Identifier invalides
        invalid_cond = (
            F.col(f"{cname}_cast").isNull()
            & F.col(f"{cname}_original").isNotNull()
            & (F.trim(F.col(f"{cname}_original")) != "")
        )

        # Compter les invalides
        invalid_count = df.filter(invalid_cond).count()

        #
        # ÉTAPE 4 : APPLIQUER LE CAST
        # ===============================================================

        df = df.withColumn(
            cname,
            F.when(
                F.col(f"{cname}_cast").isNotNull(), F.col(f"{cname}_cast")
            ).otherwise(F.lit(None)),
        )

        #
        # ÉTAPE 5 : GESTION DES ERREURS
        # ===============================================================

        errors = []
        flag_col = None

        # Gestion selon error_action
        if not is_nullable and invalid_count > 0:
            # ✅ Parser la tolérance depuis la config
            tolerance = parse_tolerance(
                col_config.get("Rejected line per file tolerance", "10%"), total_rows
            )

            # Calcul statistiques pour le contexte
            invalid_percentage = (
                (invalid_count / total_rows * 100) if total_rows > 0 else 0
            )

            # Extraction des exemples de valeurs invalides
            sample_invalid = (
                df.filter(invalid_cond)
                .select(F.col(f"{cname}_original").alias("sample_value"))
                .distinct()
                .limit(5)
                .collect()
            )

            sample_values = [
                str(row.sample_value) for row in sample_invalid if row.sample_value
            ]

            # =====================================================
            # ACTION #1: REJECT
            # =====================================================
            if err_action == "REJECT":
                print(f"❌ REJECT : {invalid_count} ligne(s) invalidée(s) pour {cname}")
                
                # Créer DataFrame d'erreurs détaillé
                errs = (
                    df.filter(invalid_cond)
                    .limit(1000)
                    .select(
                        F.lit(table_name).alias("table_name"),
                        F.lit(filename).alias("filename"),
                        F.lit(cname).alias("column_name"),
                        F.concat(
                            F.lit(f"❌ COLONNE REJETÉE : {cname}. "),
                            F.lit(f"Type attendu: {stype_str}, "),
                            F.lit(
                                f"{invalid_count} valeurs invalides sur {total_rows} lignes "
                                f"({invalid_percentage:.1f}%). "
                            ),
                            F.lit(
                                f"Exemples: {', '.join(sample_values[:3])}..."
                            ),
                        ).alias("error_message"),
                        F.col(f"{cname}_original").cast("string").alias("raw_value"),
                        F.lit(1).alias("error_count"),
                    )
                )
                errors.append(errs)

            # =====================================================
            # ACTION #2: ICT_DRIVEN (AVEC FAIL FAST)
            # =====================================================
            elif err_action == "ICT_DRIVEN":
                # Calculer le ratio d'erreurs
                error_ratio = invalid_count / float(total_rows) if total_rows > 0 else 0
                
                # ✅ VÉRIFIER SI LE SEUIL EST DÉPASSÉ
                if invalid_percentage > tolerance:
                    # ⚠️ FAIL FAST: LEVER UNE EXCEPTION
                    print(
                        f"⛔ ICT_DRIVEN FAIL FAST: {invalid_count}/{total_rows} "
                        f"({invalid_percentage:.1f}%) > seuil ({tolerance:.1f}%)"
                    )
                    print(f"   Colonne: {cname}")
                    print(f"   Type attendu: {stype_str}")
                    print(f"   Exemples valeurs invalides: {', '.join(sample_values[:3])}")
                    
                    # ✅ LEVER L'EXCEPTION ToleranceExceededError
                    raise ToleranceExceededError(
                        tolerance_type="RLK",  # Rejected Lines Keys
                        actual=error_ratio,
                        threshold=tolerance / 100.0,  # Convertir % en ratio
                        table_name=table_name,
                        filename=filename,
                        column_name=cname
                    )
                
                else:
                    # Sous le seuil : continuer avec flag d'invalidité
                    print(
                        f"⚠️ ICT_DRIVEN: {invalid_count} valeur(s) invalide(s) pour {cname} "
                        f"({invalid_percentage:.1f}% < {tolerance:.1f}%)"
                    )
                    
                    # Créer colonne de flag
                    flag_col = f"{cname}_invalid"
                    df = df.withColumn(flag_col, invalid_cond.cast("int"))
                    
                    # Logger l'erreur pour traçabilité
                    errs_summary = self.spark.createDataFrame(
                        [
                            (
                                table_name,
                                filename,
                                cname,
                                f"ICT_DRIVEN: {invalid_count}/{total_rows} invalid ({invalid_percentage:.1f}%)",
                                ", ".join(sample_values[:3]),
                                invalid_count,
                            )
                        ],
                        self.ERROR_SCHEMA,
                    )
                    errors.append(errs_summary)

            # =====================================================
            # ACTION #3: LOG_ONLY
            # =====================================================
            elif err_action == "LOG_ONLY":
                print(
                    f"🪶 LOG ONLY: {invalid_count} valeurs invalides ({invalid_percentage:.1f}%) sur {cname}"
                )
                
                # Logger uniquement, ne rien modifier
                errs = self.spark.createDataFrame(
                    [
                        (
                            table_name,
                            filename,
                            cname,
                            f"LOG_ONLY: {invalid_count}/{total_rows} invalid ({invalid_percentage:.1f}%)",
                            ", ".join(sample_values[:3]),
                            invalid_count,
                        )
                    ],
                    self.ERROR_SCHEMA,
                )
                errors.append(errs)

        #
        # ÉTAPE 6 : NETTOYAGE
        # ===============================================================

        # Nettoyer colonnes temporaires
        df = df.drop(f"{cname}_original", f"{cname}_cast")

        return df, errors, flag_col

    def reject_invalid_lines(
        self, df: DataFrame, invalid_flags: list, table_name: str, filename: str
    ) -> tuple:
        """
        Rejette les lignes avec trop d'erreurs ICT_DRIVEN
        Returns: (df_valid, errors)
        """

        if not invalid_flags:
            return df, []

        df = df.withColumn(
            "invalid_column_count",
            reduce(operator.add, [F.col(c) for c in invalid_flags]),
        )

        # Seuil max: 10% des colonnes invalides
        max_invalid_per_line = max(1, int(len(invalid_flags) * 0.1))

        df_valid = df.filter(F.col("invalid_column_count") <= max_invalid_per_line)
        df_invalid = df.filter(F.col("invalid_column_count") > max_invalid_per_line)

        errors = []
        if df_invalid.count() > 0:
            print(f"🧹 ICT_DRIVEN LINE REJECT : {df_invalid.count()} lignes rejetées")
            err_lines = self.spark.createDataFrame(
                [
                    (
                        table_name,
                        filename,
                        "MULTIPLE_COLUMNS",
                        f"Lignes avec trop d'erreurs: {df_invalid.count()}",
                        None,
                        df_invalid.count(),
                    )
                ],
                self.ERROR_SCHEMA,
            )
            errors.append(err_lines)

        # Nettoyer flags
        df_valid = df_valid.drop(*invalid_flags, "invalid_column_count")

        return df_valid, errors

    def _get_spark_type(self, type_str: str):
        """
        Convertit type string -> Spark type
        DATE est TOUJOURS converti en TIMESTAMP
        """
        from pyspark.sql.types import (
            StringType,
            IntegerType,
            LongType,
            FloatType,
            DoubleType,
            BooleanType,
            DateType,
            TimestampType,
        )

        mapping = {
            "STRING": StringType(),
            "INTEGER": IntegerType(),
            "LONG": LongType(),
            "FLOAT": FloatType(),
            "DOUBLE": DoubleType(),
            "BOOLEAN": BooleanType(),
            "DATE": TimestampType(),
            "TIMESTAMP": TimestampType(),
        }

        return mapping.get(type_str.strip().upper(), StringType())
