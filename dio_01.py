# ✅ **OUI - STRUCTURE RECOMMANDÉE AVEC FICHIERS SÉPARÉS**

C'est une **excellente pratique** de séparer les jobs de production et les jobs de test !

---

## 📁 **STRUCTURE RECOMMANDÉE**

```
dio/
├── resources/
│   ├── dio_job.yml                    # ✅ Jobs de PRODUCTION
│   ├── dio_test_job.yml               # ✅ Jobs de TEST
│   └── dio_orchestrator_job.yml       # ✅ Job ORCHESTRATEUR (optionnel)
│
├── databricks.yml                     # Configuration principale
└── ...
```

---

## 🎯 **POURQUOI SÉPARER ?**

| Critère | Fichier unique | Fichiers séparés |
|---------|----------------|------------------|
| **Clarté** | ❌ Tout mélangé | ✅ Organisation claire |
| **Déploiement** | ❌ Tout ou rien | ✅ Sélectif possible |
| **Maintenance** | ❌ Difficile | ✅ Facile |
| **Environnements** | ❌ Complexe | ✅ Simple (dev/prod) |
| **Collaboration** | ❌ Conflits Git | ✅ Moins de conflits |

---

## 📝 **1. `resources/dio_job.yml` (PRODUCTION)**

```yaml
# resources/dio_job.yml
# Jobs de PRODUCTION - Pipelines d'ingestion

resources:
  jobs:
    # ==========================================
    # JOB 1 : PIPELINE SITE (Production)
    # ==========================================
    dio_pipeline_site:
      name: dio_pipeline_site
      description: "Pipeline d'ingestion pour le dataset SITE"
      
      tags:
        environment: production
        team: data-engineering
        project: dio
      
      parameters:
        - name: dataset
          default: "site"
        - name: excel_path
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/config/config.xlsx"
        - name: zip_path
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/landing/zip/"
        - name: extract_dir
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/preprocessed/"
      
      tasks:
        # Task 1 : Unzip
        - task_key: unzip
          description: "Extraction ZIP - SITE"
          python_wheel_task:
            package_name: dio
            entry_point: unzip_module
            parameters:
              - "--dataset={{job.parameters.dataset}}"
              - "--zip_path={{job.parameters.zip_path}}"
              - "--extract_dir={{job.parameters.extract_dir}}"
          
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
            spark_conf:
              spark.databricks.delta.preview.enabled: "true"
          
          libraries:
            - whl: ../dist/dio-1.0.0-py3-none-any.whl
        
        # Task 2 : Auto Loader
        - task_key: auto-loader
          depends_on:
            - task_key: unzip
          description: "Auto Loader - SITE"
          python_wheel_task:
            package_name: dio
            entry_point: autoloader_module
            parameters:
              - "--dataset={{job.parameters.dataset}}"
              - "--source_path={{job.parameters.extract_dir}}"
          
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
        
        # Task 3 : Ingestion finale
        - task_key: dio-ingestion
          depends_on:
            - task_key: auto-loader
          description: "Ingestion finale - SITE"
          python_wheel_task:
            package_name: dio
            entry_point: main
            parameters:
              - "--dataset={{job.parameters.dataset}}"
              - "--excel_path={{job.parameters.excel_path}}"
          
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
      
      # Configuration du job
      max_concurrent_runs: 1
      timeout_seconds: 3600
      
      # Notifications (optionnel)
      email_notifications:
        on_failure:
          - your-email@company.com
    
    # ==========================================
    # JOB 2 : PIPELINE ACTIVITE (Production)
    # ==========================================
    dio_pipeline_activite:
      name: dio_pipeline_activite
      description: "Pipeline d'ingestion pour le dataset ACTIVITE"
      
      tags:
        environment: production
        team: data-engineering
        project: dio
      
      parameters:
        - name: dataset
          default: "activite"
        - name: excel_path
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/config/config.xlsx"
        - name: zip_path
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/landing/zip/"
        - name: extract_dir
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/preprocessed/"
      
      tasks:
        - task_key: unzip
          description: "Extraction ZIP - ACTIVITE"
          python_wheel_task:
            package_name: dio
            entry_point: unzip_module
            parameters:
              - "--dataset={{job.parameters.dataset}}"
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
          libraries:
            - whl: ../dist/dio-1.0.0-py3-none-any.whl
        
        - task_key: auto-loader
          depends_on:
            - task_key: unzip
          description: "Auto Loader - ACTIVITE"
          python_wheel_task:
            package_name: dio
            entry_point: autoloader_module
            parameters:
              - "--dataset={{job.parameters.dataset}}"
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
        
        - task_key: dio-ingestion
          depends_on:
            - task_key: auto-loader
          description: "Ingestion finale - ACTIVITE"
          python_wheel_task:
            package_name: dio
            entry_point: main
            parameters:
              - "--dataset={{job.parameters.dataset}}"
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
      
      max_concurrent_runs: 1
      timeout_seconds: 3600
    
    # ==========================================
    # JOB 3 : PIPELINE CLYMENE (Production)
    # ==========================================
    dio_pipeline_clymene:
      name: dio_pipeline_clymene
      description: "Pipeline d'ingestion pour le dataset CLYMENE"
      
      tags:
        environment: production
        team: data-engineering
        project: dio
      
      parameters:
        - name: dataset
          default: "clymene"
        - name: excel_path
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/config/config.xlsx"
        - name: zip_path
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/landing/zip/"
        - name: extract_dir
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/preprocessed/"
      
      tasks:
        - task_key: unzip
          description: "Extraction ZIP - CLYMENE"
          python_wheel_task:
            package_name: dio
            entry_point: unzip_module
            parameters:
              - "--dataset={{job.parameters.dataset}}"
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
          libraries:
            - whl: ../dist/dio-1.0.0-py3-none-any.whl
        
        - task_key: auto-loader
          depends_on:
            - task_key: unzip
          description: "Auto Loader - CLYMENE"
          python_wheel_task:
            package_name: dio
            entry_point: autoloader_module
            parameters:
              - "--dataset={{job.parameters.dataset}}"
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
        
        - task_key: dio-ingestion
          depends_on:
            - task_key: auto-loader
          description: "Ingestion finale - CLYMENE"
          python_wheel_task:
            package_name: dio
            entry_point: main
            parameters:
              - "--dataset={{job.parameters.dataset}}"
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
      
      max_concurrent_runs: 1
      timeout_seconds: 3600
```

---

## 📝 **2. `resources/dio_test_job.yml` (TESTS)**

```yaml
# resources/dio_test_job.yml
# Jobs de TEST - Tests d'intégration BDD

resources:
  jobs:
    # ==========================================
    # JOB DE TEST : TESTS D'INTÉGRATION
    # ==========================================
    dio_integration_tests:
      name: dio_integration_tests
      description: "Tests d'intégration BDD pour valider le pipeline DIO"
      
      tags:
        environment: test
        team: data-engineering
        project: dio
        type: integration-tests
      
      parameters:
        - name: test_tags
          default: "integration"
          description: "Tags Behave à exécuter (ex: integration, @F12_01, @IT_RL1)"
        - name: test_path
          default: "features/integration"
          description: "Chemin vers les features à tester"
        - name: report_path
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/test_reports/"
          description: "Chemin pour sauvegarder les rapports"
      
      tasks:
        # Task unique : Exécuter tous les tests
        - task_key: run-integration-tests
          description: "Exécution des tests d'intégration avec Behave"
          
          notebook_task:
            notebook_path: /Workspace/Repos/{{workspace.current_user.userName}}/dio/notebooks/run_integration_tests
            base_parameters:
              test_tags: "{{job.parameters.test_tags}}"
              test_path: "{{job.parameters.test_path}}"
              report_path: "{{job.parameters.report_path}}"
          
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
            spark_conf:
              spark.databricks.delta.preview.enabled: "true"
          
          libraries:
            - whl: ../dist/dio-1.0.0-py3-none-any.whl
            - pypi:
                package: behave>=1.2.6
            - pypi:
                package: databricks-sdk
      
      # Les tests peuvent tourner en parallèle
      max_concurrent_runs: 3
      timeout_seconds: 7200  # 2 heures max pour les tests
      
      # Notifications
      email_notifications:
        on_failure:
          - data-engineering-team@company.com
    
    # ==========================================
    # JOB DE TEST : TESTS SPÉCIFIQUES PAR TAG
    # ==========================================
    dio_test_format_files:
      name: dio_test_format_files
      description: "Tests sur les formats de fichiers (F12)"
      
      tags:
        environment: test
        team: data-engineering
        project: dio
        test-suite: file-formats
      
      tasks:
        - task_key: test-file-formats
          notebook_task:
            notebook_path: /Workspace/Repos/{{workspace.current_user.userName}}/dio/notebooks/run_integration_tests
            base_parameters:
              test_tags: "@F12_01_FORMAT_1,@IT_CSV_FORMAT_1,@IT_NONSTRUCT_1"
              test_path: "features/integration/file_formats.feature"
          
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
          
          libraries:
            - whl: ../dist/dio-1.0.0-py3-none-any.whl
            - pypi:
                package: behave>=1.2.6
      
      max_concurrent_runs: 1
      timeout_seconds: 3600
    
    dio_test_rejection_rules:
      name: dio_test_rejection_rules
      description: "Tests sur les règles de rejet (RLT)"
      
      tags:
        environment: test
        team: data-engineering
        project: dio
        test-suite: rejection-rules
      
      tasks:
        - task_key: test-rejection
          notebook_task:
            notebook_path: /Workspace/Repos/{{workspace.current_user.userName}}/dio/notebooks/run_integration_tests
            base_parameters:
              test_tags: "@IT_RL1,@IT_RLT_2"
              test_path: "features/integration/rejection_rules.feature"
          
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
          
          libraries:
            - whl: ../dist/dio-1.0.0-py3-none-any.whl
            - pypi:
                package: behave>=1.2.6
      
      max_concurrent_runs: 1
      timeout_seconds: 3600
```

---

## 📝 **3. `resources/dio_orchestrator_job.yml` (ORCHESTRATEUR - OPTIONNEL)**

```yaml
# resources/dio_orchestrator_job.yml
# Job ORCHESTRATEUR - Lance les pipelines et valide avec baseline

resources:
  jobs:
    dio_test_orchestrator:
      name: dio_test_orchestrator
      description: "Orchestrateur E2E - Lance tous les pipelines et valide avec baseline"
      
      tags:
        environment: test
        team: data-engineering
        project: dio
        type: orchestrator
      
      parameters:
        - name: baseline_path
          default: "s3://baseline/dio/"
        - name: generate_report
          default: "true"
      
      tasks:
        # Task 1 : Lancer les pipelines de prod
        - task_key: run-all-pipelines
          description: "Lancer tous les pipelines de production"
          
          notebook_task:
            notebook_path: /Workspace/Repos/{{workspace.current_user.userName}}/dio/notebooks/orchestrator/launch_pipelines
            base_parameters:
              pipelines: "site,activite,clymene"
          
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
          
          libraries:
            - whl: ../dist/dio-1.0.0-py3-none-any.whl
            - pypi:
                package: databricks-sdk
        
        # Task 2 : Validation baseline
        - task_key: validate-baseline
          depends_on:
            - task_key: run-all-pipelines
          description: "Comparer les résultats avec la baseline"
          
          notebook_task:
            notebook_path: /Workspace/Repos/{{workspace.current_user.userName}}/dio/notebooks/orchestrator/validate_baseline
            base_parameters:
              baseline_path: "{{job.parameters.baseline_path}}"
          
          existing_cluster_id: "{{tasks.run-all-pipelines.cluster_id}}"
        
        # Task 3 : Générer rapport
        - task_key: generate-report
          depends_on:
            - task_key: validate-baseline
          description: "Générer le rapport consolidé"
          
          notebook_task:
            notebook_path: /Workspace/Repos/{{workspace.current_user.userName}}/dio/notebooks/orchestrator/generate_report
            base_parameters:
              report_format: "html"
          
          existing_cluster_id: "{{tasks.run-all-pipelines.cluster_id}}"
      
      max_concurrent_runs: 1
      timeout_seconds: 10800  # 3 heures
      
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"  # Tous les jours à 2h du matin
        timezone_id: "Europe/Paris"
```

---

## 📝 **4. `databricks.yml` (CONFIGURATION PRINCIPALE)**

```yaml
# databricks.yml
# Configuration principale du bundle

bundle:
  name: dio

include:
  - resources/*.yml  # ✅ Inclut TOUS les fichiers .yml dans resources/

# Définir les cibles (environnements)
targets:
  # Environnement DEV
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-xxxxx.azuredatabricks.net
    
    # Override pour dev
    resources:
      jobs:
        dio_pipeline_site:
          name: "[DEV] dio_pipeline_site"
        dio_integration_tests:
          name: "[DEV] dio_integration_tests"
  
  # Environnement PROD
  prod:
    mode: production
    workspace:
      host: https://adb-xxxxx.azuredatabricks.net
      root_path: /Workspace/Production/dio
    
    # En prod, on ne déploie PAS les jobs de test
    resources:
      jobs:
        dio_pipeline_site:
          name: "[PROD] dio_pipeline_site"
        dio_pipeline_activite:
          name: "[PROD] dio_pipeline_activite"
        dio_pipeline_clymene:
          name: "[PROD] dio_pipeline_clymene"
    
    # Exclure les jobs de test en prod
    sync:
      exclude:
        - "resources/dio_test_job.yml"
        - "resources/dio_orchestrator_job.yml"
```

---

## 🚀 **COMMANDES DE DÉPLOIEMENT**

### **Déployer TOUT en DEV**

```bash
# Déploie les 3 fichiers (production + test + orchestrator)
databricks bundle deploy --target dev
```

**Résultat :** 6 jobs créés
- ✅ `[DEV] dio_pipeline_site`
- ✅ `[DEV] dio_pipeline_activite`
- ✅ `[DEV] dio_pipeline_clymene`
- ✅ `[DEV] dio_integration_tests`
- ✅ `[DEV] dio_test_format_files`
- ✅ `[DEV] dio_test_orchestrator`

---

### **Déployer UNIQUEMENT les tests**

```bash
# Option 1 : Déployer un fichier spécifique (nécessite config)
databricks bundle deploy --target dev --include resources/dio_test_job.yml

# Option 2 : Lancer uniquement le job de test
databricks jobs run-now --job-name "[DEV] dio_integration_tests"
```

---

### **Déployer en PROD (sans les tests)**

```bash
# Grâce à la config dans databricks.yml, les tests sont exclus
databricks bundle deploy --target prod
```

**Résultat :** 3 jobs créés
- ✅ `[PROD] dio_pipeline_site`
- ✅ `[PROD] dio_pipeline_activite`
- ✅ `[PROD] dio_pipeline_clymene`
- ❌ Tests EXCLUS automatiquement

---

## 📊 **AVANTAGES DE CETTE STRUCTURE**

| Avantage | Description |
|----------|-------------|
| **Séparation claire** | Production ≠ Test |
| **Déploiement sélectif** | Déployer prod sans tests |
| **Environnements** | Config différente dev/prod |
| **Maintenance** | Fichiers plus petits, plus lisibles |
| **Collaboration** | Moins de conflits Git |
| **CI/CD** | Facile d'intégrer dans pipeline |

---

## 🎯 **RÉPONSE DIRECTE**

**OUI, créez :**
1. ✅ `dio_job.yml` → Jobs de PRODUCTION
2. ✅ `dio_test_job.yml` → Jobs de TEST
3. ✅ `dio_orchestrator_job.yml` → Job ORCHESTRATEUR (optionnel)

**Databricks Bundle va automatiquement :**
- Lire tous les fichiers `.yml` dans `resources/`
- Les fusionner
- Les déployer selon l'environnement (dev/prod)

---

**C'est la meilleure pratique ! Ça vous permet de gérer production et tests séparément. 🎯**
