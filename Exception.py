"""
Exceptions personnalisées pour le pipeline WAX.

Ce module définit toutes les exceptions métier avec leurs codes d'erreur (error_no)
pour une traçabilité complète des erreurs dans le pipeline d'ingestion.
"""


class WAXIngestionError(Exception):
    """
    Exception de base pour toutes les erreurs du pipeline WAX.
    
    Attributes:
        error_no (str): Code d'erreur unique pour traçabilité
        message (str): Message d'erreur détaillé
        details (dict): Détails supplémentaires sur l'erreur
    """
    
    def __init__(self, error_no, message, **details):
        """
        Initialise une exception WAX.
        
        Args:
            error_no (str): Code d'erreur (ex: "000000007")
            message (str): Message d'erreur
            **details: Détails additionnels (table_name, filename, etc.)
        """
        self.error_no = error_no
        self.message = message
        self.details = details
        
        # Message complet incluant l'error_no
        full_message = f"[{error_no}] {message}"
        if details:
            details_str = ", ".join([f"{k}={v}" for k, v in details.items()])
            full_message += f" | {details_str}"
        
        super().__init__(full_message)
    
    def to_dict(self):
        """
        Convertit l'exception en dictionnaire pour logging.
        
        Returns:
            dict: Représentation de l'erreur
        """
        return {
            "error_no": self.error_no,
            "error_type": self.__class__.__name__,
            "message": self.message,
            **self.details
        }


class ToleranceExceededError(WAXIngestionError):
    """
    Exception levée quand un seuil de tolérance est dépassé (Fail Fast).
    
    Types de tolérance:
    - RLI: Rejected Lines Ingestion (lignes rejetées/corrompues en ingestion)
    - RLD: Rejected Lines Delta (lignes rejetées en delta - doublons)
    - RLK: Rejected Lines Keys (lignes rejetées sur clés/valeurs invalides)
    
    Cette exception déclenche l'arrêt immédiat du pipeline (Fail Fast).
    """
    
    # Codes d'erreur par type de tolérance
    ERROR_CODES = {
        "RLI": "000000007",  # Tolérance dépassée en ingestion (lignes corrompues)
        "RLD": "000000008",  # Tolérance dépassée en delta (doublons)
        "RLK": "000000009",  # Tolérance dépassée sur clés/valeurs (nullabilité, typage)
    }
    
    def __init__(self, tolerance_type, actual, threshold, table_name, 
                 filename=None, column_name=None):  # ✅ AJOUT DE column_name
        """
        Initialise une erreur de tolérance dépassée.
        
        Args:
            tolerance_type (str): Type de tolérance ("RLI", "RLD", "RLK")
            actual (float): Valeur actuelle (ratio 0.0-1.0, ex: 0.15 = 15%)
            threshold (float): Seuil de tolérance (ratio 0.0-1.0, ex: 0.05 = 5%)
            table_name (str): Nom de la table concernée
            filename (str, optional): Nom du fichier concerné
            column_name (str, optional): Nom de la colonne concernée (pour RLK)
        """
        error_no = self.ERROR_CODES.get(tolerance_type, "000000007")
        
        # Construire message descriptif
        message = (
            f"{tolerance_type} tolerance exceeded: "
            f"{actual:.2%} > {threshold:.2%}"
        )
        
        if column_name:
            message = f"Column '{column_name}': {message}"
        
        # Préparer les détails
        details = {
            "tolerance_type": tolerance_type,
            "actual_ratio": actual,
            "threshold": threshold,
            "table_name": table_name,
        }
        
        if filename:
            details["filename"] = filename
        if column_name:
            details["column_name"] = column_name
        
        super().__init__(
            error_no=error_no,
            message=message,
            **details
        )


class InvalidDateFormatError(WAXIngestionError):
    """
    Exception levée quand une date a un format invalide.
    
    Error code: 000000004
    """
    
    def __init__(self, value, expected_format, column_name, table_name, filename=None):
        """
        Initialise une erreur de format de date invalide.
        
        Args:
            value: Valeur de date invalide
            expected_format (str): Format attendu (ex: "yyyyMMdd")
            column_name (str): Nom de la colonne
            table_name (str): Nom de la table
            filename (str, optional): Nom du fichier
        """
        message = (
            f"Invalid date format in column '{column_name}': "
            f"'{value}' does not match expected format '{expected_format}'"
        )
        
        super().__init__(
            error_no="000000004",
            message=message,
            value=value,
            expected_format=expected_format,
            column_name=column_name,
            table_name=table_name,
            filename=filename or "unknown"
        )


class InvalidDateValueError(WAXIngestionError):
    """
    Exception levée quand une date a une valeur invalide (ex: 30 février).
    
    Error code: 000000005
    """
    
    def __init__(self, value, reason, column_name, table_name, filename=None):
        """
        Initialise une erreur de valeur de date invalide.
        
        Args:
            value: Valeur de date invalide
            reason (str): Raison de l'invalidité
            column_name (str): Nom de la colonne
            table_name (str): Nom de la table
            filename (str, optional): Nom du fichier
        """
        message = (
            f"Invalid date value in column '{column_name}': "
            f"'{value}' - {reason}"
        )
        
        super().__init__(
            error_no="000000005",
            message=message,
            value=value,
            reason=reason,
            column_name=column_name,
            table_name=table_name,
            filename=filename or "unknown"
        )


class MandatoryColumnMissingError(WAXIngestionError):
    """
    Exception levée quand une colonne obligatoire est manquante.
    
    Error code: 000000010
    """
    
    def __init__(self, missing_columns, table_name):
        """
        Initialise une erreur de colonne obligatoire manquante.
        
        Args:
            missing_columns (list): Liste des colonnes manquantes
            table_name (str): Nom de la table
        """
        message = (
            f"Mandatory columns missing in table '{table_name}': "
            f"{', '.join(missing_columns)}"
        )
        
        super().__init__(
            error_no="000000010",
            message=message,
            missing_columns=missing_columns,
            table_name=table_name
        )


# Alias pour compatibilité avec l'ancien code (si nécessaire)
class FailFastException(ToleranceExceededError):
    """Alias pour ToleranceExceededError (compatibilité)."""
    pass


# Export des classes principales
__all__ = [
    "WAXIngestionError",
    "ToleranceExceededError",
    "InvalidDateFormatError",
    "InvalidDateValueError",
    "MandatoryColumnMissingError",
    "FailFastException",
]
