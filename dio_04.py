# 📝 **FICHIERS COMPLETS YAML**

---

## 1️⃣ **`resources/waxng_job.yml` (PRODUCTION)**

```yaml
# resources/waxng_job.yml
# Job de PRODUCTION - Pipeline d'ingestion WAX NG

resources:
  jobs:
    waxng_job:
      name: waxng_job
      description: "Pipeline d'ingestion WAX NG - Production"
      
      tags:
        environment: "{{bundle.target}}"
        project: waxng
        type: production
      
      parameters:
        - name: dataset
          default: "site"
          description: "Nom du dataset à ingérer (site, activite, clymene, etc.)"
        
        - name: zip_path
          default: ""
          description: "Chemin vers le dossier contenant les ZIP"
        
        - name: excel_path
          default: ""
          description: "Chemin vers le fichier Excel de configuration"
        
        - name: extract_dir
          default: ""
          description: "Dossier de destination pour l'extraction"
        
        - name: log_exec_path
          default: ""
          description: "Chemin pour les logs d'exécution"
        
        - name: log_quality_path
          default: ""
          description: "Chemin pour les logs de qualité"
      
      tasks:
        # ==========================================
        # TASK 1 : DÉZIPAGE
        # ==========================================
        - task_key: unzip
          description: "Extraction des fichiers ZIP"
          
          python_wheel_task:
            package_name: waxng
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
              spark.databricks.delta.optimizeWrite.enabled: "true"
          
          libraries:
            - whl: ../dist/*.whl
        
        # ==========================================
        # TASK 2 : AUTO LOADER
        # ==========================================
        - task_key: auto-loader
          depends_on:
            - task_key: unzip
          description: "Ingestion automatique avec Auto Loader"
          
          python_wheel_task:
            package_name: waxng
            entry_point: autoloader_module
            parameters:
              - "--dataset={{job.parameters.dataset}}"
              - "--source_path={{job.parameters.extract_dir}}"
          
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
        
        # ==========================================
        # TASK 3 : INGESTION WAXNG
        # ==========================================
        - task_key: waxng-ingestion
          depends_on:
            - task_key: auto-loader
          description: "Validation, transformation et ingestion finale"
          
          python_wheel_task:
            package_name: waxng
            entry_point: main
            parameters:
              - "--dataset={{job.parameters.dataset}}"
              - "--excel_path={{job.parameters.excel_path}}"
              - "--log_exec_path={{job.parameters.log_exec_path}}"
              - "--log_quality_path={{job.parameters.log_quality_path}}"
          
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
      
      # Configuration du job
      max_concurrent_runs: 1
      timeout_seconds: 3600
      
      # Environnements
      environments:
        - environment_key: default
          spec:
            client: "1"
            dependencies:
              - ../dist/*.whl
              - pytest>=7.0.0
      
      # Notifications (optionnel)
      # email_notifications:
      #   on_failure:
      #     - your-email@company.com
      #   on_success:
      #     - your-email@company.com
```

---

## 2️⃣ **`resources/waxng_test_job.yml` (TESTS)**

```yaml
# resources/waxng_test_job.yml
# Jobs de TEST - Validation avec baseline

resources:
  jobs:
    # ==========================================
    # JOB 1 : VALIDATION AVEC BASELINE
    # ==========================================
    waxng_test_validation:
      name: waxng_test_validation
      description: "Valide les résultats du pipeline en comparant avec une baseline de référence"
      
      tags:
        environment: "{{bundle.target}}"
        project: waxng
        type: validation-test
      
      parameters:
        - name: dataset
          default: "site"
          description: "Dataset à valider (site, activite, clymene)"
        
        - name: pipeline_job_name
          default: "waxng_job"
          description: "Nom du job de pipeline à lancer"
        
        - name: excel_path
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/config/config.xlsx"
          description: "Chemin Excel de configuration"
        
        - name: zip_path
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/landing/zip/"
          description: "Chemin des fichiers ZIP de test"
        
        - name: tolerance_pct
          default: "5"
          description: "Tolérance en % pour la comparaison (ex: 5 pour ±5%)"
      
      tasks:
        # ==========================================
        # TASK 1 : Lancer le pipeline de production
        # ==========================================
        - task_key: run-pipeline
          description: "Exécuter le pipeline WAX NG"
          
          run_job_task:
            job_name: "{{job.parameters.pipeline_job_name}}"
            job_parameters:
              dataset: "{{job.parameters.dataset}}"
              excel_path: "{{job.parameters.excel_path}}"
              zip_path: "{{job.parameters.zip_path}}"
        
        # ==========================================
        # TASK 2 : Comparer avec baseline
        # ==========================================
        - task_key: compare-with-baseline
          depends_on:
            - task_key: run-pipeline
          description: "Comparer les résultats avec la baseline de référence"
          
          notebook_task:
            notebook_path: /Workspace/Repos/{{workspace.current_user.userName}}/waxng/notebooks/compare_with_baseline
            base_parameters:
              dataset: "{{job.parameters.dataset}}"
              tolerance_pct: "{{job.parameters.tolerance_pct}}"
          
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
            spark_conf:
              spark.databricks.delta.preview.enabled: "true"
        
        # ==========================================
        # TASK 3 : Générer le rapport
        # ==========================================
        - task_key: generate-report
          depends_on:
            - task_key: compare-with-baseline
          description: "Générer le rapport de validation"
          
          notebook_task:
            notebook_path: /Workspace/Repos/{{workspace.current_user.userName}}/waxng/notebooks/generate_test_report
            base_parameters:
              dataset: "{{job.parameters.dataset}}"
          
          existing_cluster_id: "{{tasks.compare-with-baseline.cluster_id}}"
      
      max_concurrent_runs: 2
      timeout_seconds: 7200
      
      email_notifications:
        on_failure:
          - data-team@company.com
        on_success:
          - data-team@company.com
    
    # ==========================================
    # JOB 2 : TESTS BDD BEHAVE (optionnel)
    # ==========================================
    waxng_bdd_tests:
      name: waxng_bdd_tests
      description: "Tests BDD avec Behave pour valider les scénarios d'intégration"
      
      tags:
        environment: "{{bundle.target}}"
        project: waxng
        type: bdd-tests
      
      parameters:
        - name: test_tags
          default: "integration"
          description: "Tags Behave à exécuter (@F12_01, @IT_RL1, etc.)"
        
        - name: test_feature
          default: "all"
          description: "Feature spécifique ou 'all'"
      
      tasks:
        - task_key: run-bdd-tests
          description: "Exécuter les tests BDD avec Behave"
          
          notebook_task:
            notebook_path: /Workspace/Repos/{{workspace.current_user.userName}}/waxng/notebooks/run_bdd_tests
            base_parameters:
              test_tags: "{{job.parameters.test_tags}}"
              test_feature: "{{job.parameters.test_feature}}"
          
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
          
          libraries:
            - whl: ../dist/*.whl
            - pypi:
                package: behave>=1.2.6
      
      max_concurrent_runs: 3
      timeout_seconds: 3600
      
      environments:
        - environment_key: default
          spec:
            client: "1"
            dependencies:
              - ../dist/*.whl
              - behave>=1.2.6
              - pytest>=7.0.0
    
    # ==========================================
    # JOB 3 : ORCHESTRATEUR E2E (optionnel)
    # ==========================================
    waxng_e2e_orchestrator:
      name: waxng_e2e_orchestrator
      description: "Orchestrateur E2E - Lance plusieurs datasets et valide"
      
      tags:
        environment: "{{bundle.target}}"
        project: waxng
        type: e2e-orchestrator
      
      parameters:
        - name: datasets
          default: "site,activite,clymene"
          description: "Liste des datasets séparés par des virgules"
        
        - name: baseline_path
          default: "s3://baseline/waxng/"
          description: "Chemin vers les baselines"
      
      tasks:
        - task_key: orchestrate-tests
          description: "Orchestrer les tests sur plusieurs datasets"
          
          notebook_task:
            notebook_path: /Workspace/Repos/{{workspace.current_user.userName}}/waxng/notebooks/orchestrator/e2e_orchestrator
            base_parameters:
              datasets: "{{job.parameters.datasets}}"
              baseline_path: "{{job.parameters.baseline_path}}"
          
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
          
          libraries:
            - whl: ../dist/*.whl
            - pypi:
                package: databricks-sdk
      
      max_concurrent_runs: 1
      timeout_seconds: 10800
      
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"  # Tous les jours à 2h du matin
        timezone_id: "Europe/Paris"
```

---

## 3️⃣ **`databricks.yml` (Configuration principale)**

```yaml
# databricks.yml

bundle:
  name: waxng
  uuid: f6a5aabf-da0c-4743-813d-d29d1d03e094

artifacts:
  python_artifact:
    type: whl
    build: uv build --wheel

include:
  - resources/*.yml  # Inclut waxng_job.yml ET waxng_test_job.yml

targets:
  # ==========================================
  # ENVIRONNEMENT DEV
  # ==========================================
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-4320079867405973.13.azuredatabricks.net
    
    # En dev, on déploie tout (prod + tests)
  
  # ==========================================
  # ENVIRONNEMENT PROD
  # ==========================================
  prod:
    mode: production
    workspace:
      host: https://adb-4320079867405973.13.azuredatabricks.net
      root_path: /Workspace/Tech/waxng/.bundle/${bundle.name}/${bundle.target}
    
    permissions:
      - user_name: soumailou.toure.external@axa.com
        level: CAN_MANAGE
    
    # En prod, on exclut les tests
    sync:
      exclude:
        - "resources/waxng_test_job.yml"
        - "features/**"
        - "notebooks/compare_with_baseline.py"
        - "notebooks/generate_test_report.py"
        - "notebooks/run_bdd_tests.py"
        - "notebooks/orchestrator/**"
```

---

## 🚀 **UTILISATION**

### **Déployer en DEV (avec tests)**

```bash
# 1. Builder
rm -rf dist/ build/ *.egg-info
uv build --wheel

# 2. Déployer (inclut prod + tests)
databricks bundle deploy --target dev

# 3. Jobs créés:
# - waxng_job (production)
# - waxng_test_validation (validation baseline)
# - waxng_bdd_tests (tests BDD)
# - waxng_e2e_orchestrator (orchestrateur)
```

### **Lancer le pipeline de production**

```bash
databricks jobs run-now --job-name waxng_job \
  --parameters dataset=site,excel_path=/Volumes/.../config.xlsx
```

### **Lancer la validation avec baseline**

```bash
databricks jobs run-now --job-name waxng_test_validation \
  --parameters dataset=site
```

### **Déployer en PROD (sans tests)**

```bash
# Les tests sont automatiquement exclus en prod
databricks bundle deploy --target prod

# Seul waxng_job sera déployé
```

---

## 📊 **RÉCAPITULATIF DES JOBS**

| Job | Fichier | Description | Déployé en DEV | Déployé en PROD |
|-----|---------|-------------|----------------|-----------------|
| `waxng_job` | waxng_job.yml | Pipeline production | ✅ | ✅ |
| `waxng_test_validation` | waxng_test_job.yml | Validation baseline | ✅ | ❌ |
| `waxng_bdd_tests` | waxng_test_job.yml | Tests BDD | ✅ | ❌ |
| `waxng_e2e_orchestrator` | waxng_test_job.yml | Orchestrateur E2E | ✅ | ❌ |

---

**Voilà les 3 fichiers complets et prêts à déployer ! 🎯**
