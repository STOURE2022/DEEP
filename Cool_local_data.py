# 🎉 **PARFAIT ! Les fichiers `.feature` sont maintenant dans le wheel !**

## ✅ **VÉRIFICATION RÉUSSIE**

```
waxng/features/__init__.py           ← ✅
waxng/features/environment.py        ← ✅
waxng/features/unzip.feature         ← ✅
waxng/features/steps/__init__.py     ← ✅
waxng/features/steps/unzip_steps.py  ← ✅
```

**29 fichiers au total** - Wheel complet et prêt ! 🚀

---

## 🧪 **ÉTAPE 1 : TESTER LOCALEMENT**

```bash
# 1. Installer le wheel
pip install --force-reinstall dist/waxng-1.0.2-py3-none-any.whl

# 2. Vérifier que features est accessible
python -c "import waxng.features; print(waxng.features.__file__)"

# 3. Lister les fichiers features
python -c "import waxng.features, os; print(os.listdir(os.path.dirname(waxng.features.__file__)))"

# 4. Exécuter les tests BDD
waxng-tests

# Ou directement avec behave
python -m behave $(python -c "import waxng.features, os; print(os.path.dirname(waxng.features.__file__))")
```

---

## ☁️ **ÉTAPE 2 : DÉPLOYER SUR DATABRICKS**

### **A. Ajouter la tâche BDD au job Databricks**

Modifiez `resources/waxng_job.yml` :

```yaml
# resources/waxng_job.yml

resources:
  jobs:
    waxng_job:
      name: waxng_job
      
      parameters:
        - name: zip_path
          default: ""
        - name: excel_path
          default: ""
        - name: extract_dir
          default: ""
        - name: log_exec_path
          default: ""
        - name: log_quality_path
          default: ""
      
      trigger:
        file_arrival:
          url: /Volumes/abu_catalog/databricksassetbundletest/externalvolumetest/landing/zip
          min_time_between_triggers_seconds: 20
          wait_after_last_change_seconds: 60
        pause_status: "UNPAUSED"
      
      tasks:
        # ==========================================
        # TASK 1 : DÉZIPAGE
        # ==========================================
        - task_key: unzip
          description: "Extraction des fichiers ZIP"
          environment_key: default
          python_wheel_task:
            package_name: waxng
            entry_point: unzip_module
        
        # ==========================================
        # TASK 2 : AUTO LOADER
        # ==========================================
        - task_key: auto-loader
          depends_on:
            - task_key: unzip
          description: "Ingestion automatique avec Auto Loader"
          environment_key: default
          python_wheel_task:
            package_name: waxng
            entry_point: autoloader_module
        
        # ==========================================
        # TASK 3 : INGESTION WAX
        # ==========================================
        - task_key: waxng-ingestion
          depends_on:
            - task_key: auto-loader
          description: "validation, transformation et ingestion finale"
          environment_key: default
          python_wheel_task:
            package_name: waxng
            entry_point: main
        
        # ==========================================
        # ✅ TASK 4 : TESTS BDD (NOUVEAU)
        # ==========================================
        - task_key: bdd-tests
          depends_on:
            - task_key: waxng-ingestion
          description: "Tests BDD avec Behave"
          environment_key: default
          python_wheel_task:
            package_name: waxng
            entry_point: run_bdd_tests
          # ✅ Optionnel : continuer même si les tests échouent
          # depends_on:
          #   - task_key: waxng-ingestion
          #     outcome: "success"
      
      # ==========================================
      # ENVIRONNEMENT
      # ==========================================
      environments:
        - environment_key: default
          spec:
            environment_version: "2"
            dependencies:
              - ../dist/waxng-1.0.2-py3-none-any.whl  # ✅ Version à jour
            libraries:
              - pypi:
                  package: behave>=1.2.6  # ✅ Dépendance pour BDD
```

---

### **B. Déployer le bundle**

```bash
# 1. Depuis le dossier racine
cd /path/to/waxng

# 2. Valider la configuration
databricks bundle validate --target dev

# 3. Déployer
databricks bundle deploy --target dev

# 4. Vérifier le déploiement
databricks bundle summary --target dev
```

---

### **C. Exécuter le job avec tests**

```bash
# Lancer le job complet (avec la tâche BDD)
databricks jobs run-now --job-name "waxng_job"

# Ou lancer UNIQUEMENT les tests BDD
databricks jobs run-now --job-name "waxng_job" --task-keys "bdd-tests"
```

---

## 🎯 **ÉTAPE 3 : JOB SÉPARÉ POUR LES TESTS (OPTIONNEL)**

Si vous voulez un job dédié aux tests :

```yaml
# resources/waxng_tests_job.yml

resources:
  jobs:
    waxng_bdd_tests:
      name: "WAX NG - Tests BDD"
      
      tasks:
        - task_key: run-bdd-tests
          description: "Exécution des tests BDD"
          environment_key: default
          python_wheel_task:
            package_name: waxng
            entry_point: run_bdd_tests
      
      environments:
        - environment_key: default
          spec:
            environment_version: "2"
            dependencies:
              - ../dist/waxng-1.0.2-py3-none-any.whl
            libraries:
              - pypi:
                  package: behave>=1.2.6
      
      # ✅ Planification quotidienne
      schedule:
        quartz_cron_expression: "0 0 3 * * ?"  # Tous les jours à 3h
        timezone_id: "Europe/Paris"
```

Puis déployer :

```bash
databricks bundle deploy --target dev
databricks jobs run-now --job-name "WAX NG - Tests BDD"
```

---

## 📊 **RÉSULTAT ATTENDU SUR DATABRICKS**

Quand le job s'exécute, vous verrez dans les logs :

```
🎯 Environnement détecté: Databricks
✅ Spark 14.3.x disponible
✅ dbutils disponible
📂 Catalog: abu_catalog
📂 Schema: gdp_poc_dev
📂 Volume: externalvolumetest

🎯 FEATURE: Décompression de fichiers ZIP

📋 SCÉNARIO: Décompresser un fichier ZIP valide
  Given Spark est disponible               ✅
  And les chemins de base sont configurés  ✅
  Given un fichier ZIP existe à ...        ✅
  When je décompresse le fichier vers ...  ✅
  Then la décompression doit réussir       ✅
  And les fichiers extraits doivent ...    ✅

📊 RÉSULTATS FEATURE: Décompression de fichiers ZIP
   ✅ Réussis: 4/4
   ❌ Échoués: 0/4

✅ TOUS LES TESTS BDD ONT RÉUSSI
```

---

## 🔍 **DÉBUGGER SI NÉCESSAIRE**

Si les tests échouent sur Databricks :

1. **Vérifier les logs du job** dans l'UI Databricks
2. **Ajouter du debug** dans `environment.py` :
   ```python
   print(f"📂 Features path: {features_path}")
   print(f"📋 Files: {os.listdir(features_path)}")
   ```
3. **Tester manuellement** dans un notebook Databricks :
   ```python
   import waxng.features
   import os
   
   path = os.path.dirname(waxng.features.__file__)
   print(f"Features path: {path}")
   print(f"Files: {os.listdir(path)}")
   
   # Lancer behave
   !python -m behave {path}
   ```

---

## 📝 **CHECKLIST FINALE**

- [x] ✅ Wheel contient `waxng/features/`
- [x] ✅ Fichiers `.feature` inclus
- [x] ✅ `run_bdd_tests.py` configuré
- [ ] 🔄 Tester localement avec `waxng-tests`
- [ ] 🔄 Déployer sur Databricks
- [ ] 🔄 Exécuter le job et vérifier les logs
- [ ] 🔄 Valider que les tests passent

---

**Félicitations ! Votre package est prêt pour Databricks avec les tests BDD intégrés ! 🎉**

Voulez-vous que je vous aide à :
1. 🧪 Créer plus de scénarios de tests ?
2. 📊 Configurer des rapports HTML pour les tests ?
3. 🔄 Intégrer dans un pipeline CI/CD ?
