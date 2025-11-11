🔧 MODIFICATIONS À APPORTER À VOS FICHIERS EXISTANTS
1. Ajout dans config.py
Ajouter ces paramètres à la classe Config :

python
def __init__(self, spark, config_dict=None):
    # Configuration existante...
    
    # NOUVEAUX PARAMÈTRES POUR FAIL FAST ET VALIDATION
    self.fail_fast_enabled = True
    self.fail_fast_threshold = 10  # 10% d'erreurs
    self.fail_fast_error_code = "000000007"
    
    self.rli_threshold = 2.5  # 2.5% de lignes rejetées
    self.ict_threshold = 0.15  # 0.15% de colonnes invalides
    
    self.date_validation_error_code = "000000004"
    self.date_tolerance_days = 10
    
    # Patterns de fichiers pour validation des dates
    self.file_patterns = {
        "billing": {
            "fixed_part": "billing",
            "date_pattern": "yyyyMMdd_HHmmss",
            "validation_required": True
        }
    }
2. Modification de autoloader_module.py
Ajouter cette méthode dans la classe AutoloaderModule :

python
def _validate_fail_fast_condition(self, error_count, total_processed, file_path):
    """Valide la condition Fail Fast et arrête si seuil dépassé"""
    if not self.config.fail_fast_enabled:
        return False
    
    error_rate = (error_count / max(total_processed, 1)) * 100
    threshold = self.config.fail_fast_threshold
    
    if error_rate >= threshold:
        print(f"❌ FAIL FAST ACTIVÉ : {error_rate:.1f}% d'erreurs (seuil: {threshold}%)")
        print(f"   Fichier rejeté : {file_path}")
        print(f"   Code erreur : {self.config.fail_fast_error_code}")
        return True
    return False

def _validate_filename_date_pattern(self, filename, validate_date):
    """Valide le pattern de date dans le nom de fichier"""
    try:
        # Extraction de la date selon les patterns configurés
        extracted_date = self._extract_date_from_filename(filename)
        
        if extracted_date != validate_date:
            print(f"❌ Validation date échouée : {filename}")
            print(f"   Extracted: {extracted_date}, Expected: {validate_date}")
            print(f"   Code erreur : {self.config.date_validation_error_code}")
            return False
            
        return True
    except Exception as e:
        print(f"❌ Erreur validation date : {e}")
        return False

def _apply_rli_validation(self, rejected_lines_count, total_lines_count):
    """Applique la validation RLI"""
    rejection_rate = (rejected_lines_count / max(total_lines_count, 1)) * 100
    
    if rejection_rate > self.config.rli_threshold:
        return False, rejection_rate
    return True, rejection_rate

def _apply_ict_validation(self, invalid_columns_count, total_columns_count):
    """Applique la validation ICT"""
    invalid_rate = (invalid_columns_count / max(total_columns_count, 1)) * 100
    
    if invalid_rate > self.config.ict_threshold:
        return False, invalid_rate
    return True, invalid_rate
Modifier la méthode process_all_tables pour intégrer les nouvelles validations :

python
def process_all_tables(self, excel_config_path: str) -> dict:
    # Code existant...
    
    # AJOUT: Validation Fail Fast au début du traitement
    if self._validate_fail_fast_condition(initial_error_count, initial_total, "batch_start"):
        return {
            "status": "FAILED", 
            "error": f"Fail Fast triggered: {self.config.fail_fast_error_code}"
        }
    
    # AJOUT: Validation RLI/ICT pendant le traitement
    for table_name, files_info in files_by_table.items():
        # Validation RLI
        rli_valid, rli_rate = self._apply_rli_validation(
            files_info.get('rejected_lines', 0), 
            files_info.get('total_lines', 1)
        )
        
        if not rli_valid:
            print(f"❌ RLI validation failed: {rli_rate:.1f}% > {self.config.rli_threshold}%")
        
        # Validation ICT
        ict_valid, ict_rate = self._apply_ict_validation(
            files_info.get('invalid_columns', 0),
            files_info.get('total_columns', 1)
        )
        
        if not ict_valid:
            print(f"❌ ICT validation failed: {ict_rate:.1f}% > {self.config.ict_threshold}%")
3. Modification de column_processor.py
Ajouter ces méthodes à la classe ColumnProcessor :

python
def should_stop_ingestion(self, error_count, total_lines):
    """Rule: Fail Fast Option - Vérifie si le seuil d'erreur est atteint"""
    if not self.config.fail_fast_enabled:
        return False
    
    error_percentage = (error_count / max(total_lines, 1)) * 100
    threshold = self.config.fail_fast_threshold
    
    if error_percentage >= threshold:
        print(f"⛔ ARRÊT IMMÉDIAT : {error_percentage:.1f}% d'erreurs (seuil: {threshold}%)")
        return True
    return False

def validate_filename_date_pattern(self, filename, validate_date):
    """Scenario: Validity of the date extracted from filename pattern"""
    try:
        # Votre logique existante d'extraction de date
        extracted_date = self._extract_date_from_filename(filename)
        
        # Validation stricte
        if extracted_date != validate_date:
            print(f"❌ DATE VALIDATION FAILED: {filename}")
            print(f"   Expected: {validate_date}, Got: {extracted_date}")
            print(f"   Error code: {self.config.date_validation_error_code}")
            return False
            
        return True
    except Exception as e:
        print(f"❌ Date validation error: {e}")
        return False
4. Modification de ingestion.py
Ajouter cette méthode dans IngestionManager :

python
def check_fail_fast_condition(self, error_metrics, current_file):
    """Vérifie la condition d'arrêt rapide pour le fichier en cours"""
    if not self.config.fail_fast_enabled:
        return False
    
    total_errors = error_metrics.get('total_errors', 0)
    total_processed = error_metrics.get('total_processed', 1)
    
    error_rate = (total_errors / total_processed) * 100
    
    if error_rate >= self.config.fail_fast_threshold:
        print(f"⛔ INGESTION STOPPÉE - Fail Fast activé")
        print(f"   Fichier: {current_file}")
        print(f"   Taux d'erreur: {error_rate:.1f}%")
        print(f"   Seuil: {self.config.fail_fast_threshold}%")
        print(f"   Code erreur: {self.config.fail_fast_error_code}")
        return True
    
    return False
5. Modification de main.py
Ajouter ces vérifications dans la fonction principale :

python
def main():
    # Code existant...
    
    # AJOUT: Initialisation des compteurs pour Fail Fast
    global_error_count = 0
    global_total_processed = 0
    
    for table_idx, trow in file_tables_df.iterrows():
        # AJOUT: Vérification Fail Fast avant chaque table
        if global_error_count > 0:
            error_rate = (global_error_count / max(global_total_processed, 1)) * 100
            if error_rate >= config.fail_fast_threshold:
                print(f"⛔ FAIL FAST - Arrêt du traitement")
                print(f"   Taux d'erreur global: {error_rate:.1f}%")
                print(f"   Code: {config.fail_fast_error_code}")
                break
        
        # Traitement existant...
        
        # AJOUT: Mise à jour des compteurs globaux
        global_error_count += current_error_count
        global_total_processed += current_total_processed
        
        # AJOUT: Validation RLI/ICT
        rli_valid, rli_rate = validator._apply_rli_validation(
            corrupt_rows, total_rows_initial
        )
        if not rli_valid:
            print(f"⚠️  RLI dépassé: {rli_rate:.1f}%")
        
        ict_valid, ict_rate = validator._apply_ict_validation(
            len(invalid_flags), len(df_raw.columns)
        )
        if not ict_valid:
            print(f"⚠️  ICT dépassé: {ict_rate:.1f}%")
6. Modification de validator.py
Ajouter ces méthodes à DataValidator :

python
def validate_fail_fast_condition(self, error_count, total_processed, threshold):
    """Valide la condition Fail Fast"""
    if total_processed == 0:
        return False
        
    error_rate = (error_count / total_processed) * 100
    return error_rate >= threshold

def validate_rli_threshold(self, rejected_lines, total_lines, threshold):
    """Valide le seuil RLI"""
    if total_lines == 0:
        return True, 0
        
    rejection_rate = (rejected_lines / total_lines) * 100
    return rejection_rate <= threshold, rejection_rate

def validate_ict_threshold(self, invalid_columns, total_columns, threshold):
    """Valide le seuil ICT"""
    if total_columns == 0:
        return True, 0
        
    invalid_rate = (invalid_columns / total_columns) * 100
    return invalid_rate <= threshold, invalid_rate
7. Ajout dans utils.py
Ajouter ces fonctions utilitaires :

python
def parse_fail_fast_threshold(config_value, default=10):
    """Parse le seuil Fail Fast depuis la configuration"""
    if not config_value:
        return default
    
    try:
        return float(str(config_value).replace('%', '').strip())
    except:
        return default

def parse_rli_threshold(config_value, default=2.5):
    """Parse le seuil RLI depuis la configuration"""
    return parse_fail_fast_threshold(config_value, default)

def parse_ict_threshold(config_value, default=0.15):
    """Parse le seuil ICT depuis la configuration"""
    return parse_fail_fast_threshold(config_value, default)
Ces modifications s'intègrent directement dans votre code existant sans créer de nouveaux fichiers. Elles ajoutent :

✅ Fail Fast Option avec seuil configurable

✅ Validation des dates dans les noms de fichiers avec code d'erreur

✅ Paramètres RLI/ICT dans la configuration

✅ Arrêt immédiat quand les seuils sont dépassés
