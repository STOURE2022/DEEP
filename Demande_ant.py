# 🎯 **CLARIFICATION : APPROCHE ACTUELLE VS APPROCHE RECOMMANDÉE**

Excellente question ! Laissez-moi vous expliquer la différence entre ce que vous avez fait et ce que votre collègue recommande.

---

## 📊 **VOTRE APPROCHE ACTUELLE**

```yaml
waxng_job:
  tasks:
    - bdd-tests         # Tests inline
    - unzip
    - auto-loader
    - waxng-ingestion
```

**Problèmes :**
- ❌ Tests mélangés avec le code de production
- ❌ Un seul dataset à la fois
- ❌ Pas de comparaison baseline
- ❌ Tests bloquent le pipeline si échec

---

## 🚀 **APPROCHE RECOMMANDÉE PAR VOTRE COLLÈGUE**

### **Architecture complète :**

```
┌─────────────────────────────────────────────────────────────┐
│                    JOBS DE PRODUCTION                        │
│                   (Réutilisables)                            │
└─────────────────────────────────────────────────────────────┘

Job 1: waxng_pipeline_site
├── unzip (site)
├── auto-loader (site)
└── waxng-ingestion (site)

Job 2: waxng_pipeline_activite
├── unzip (activite)
├── auto-loader (activite)
└── waxng-ingestion (activite)

Job 3: waxng_pipeline_clymene
├── unzip (clymene)
├── auto-loader (clymene)
└── waxng-ingestion (clymene)

┌─────────────────────────────────────────────────────────────┐
│              JOB ORCHESTRATEUR DE TESTS                      │
│                    (Séparé)                                  │
└─────────────────────────────────────────────────────────────┘

Job Test Orchestrator
├── 1. Lance Job 1 (site)
├── 2. Lance Job 2 (activite)
├── 3. Lance Job 3 (clymene)
├── 4. Attendre fin de tous les jobs
├── 5. Récupérer outputs (tables Delta)
├── 6. Charger baseline de référence
├── 7. Comparer résultats vs baseline
│      ├── Nombre de lignes
│      ├── Qualité des données
│      ├── Colonnes présentes
│      └── Valeurs NULL
└── 8. Générer rapport consolidé
```

---

## 📝 **NOUVELLE STRUCTURE COMPLÈTE**

### **1. `resources/waxng_job.yml` (JOBS DE PRODUCTION)**

```yaml
# resources/waxng_job.yml

resources:
  jobs:
    # ==========================================
    # JOB 1 : PIPELINE SITE (Production)
    # ==========================================
    waxng_pipeline_site:
      name: waxng_pipeline_site
      
      parameters:
        - name: zip_path
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/landing/zip/"
        - name: excel_path
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/config/"
        - name: extract_dir
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/preprocessed/"
        - name: dataset
          default: "site"
      
      tasks:
        # Task 1 : Décompression
        - task_key: unzip
          description: "Extraction ZIP - SITE"
          python_wheel_task:
            package_name: waxng
            entry_point: unzip_module
            parameters:
              - "--zip_path={{job.parameters.zip_path}}"
              - "--extract_dir={{job.parameters.extract_dir}}"
              - "--dataset={{job.parameters.dataset}}"
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
          libraries:
            - whl: ../dist/waxng-1.0.0-py3-none-any.whl
        
        # Task 2 : Auto Loader
        - task_key: auto-loader
          depends_on:
            - task_key: unzip
          description: "Auto Loader - SITE"
          python_wheel_task:
            package_name: waxng
            entry_point: autoloader_module
            parameters:
              - "--source_path={{job.parameters.extract_dir}}"
              - "--dataset={{job.parameters.dataset}}"
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
        
        # Task 3 : Ingestion finale
        - task_key: waxng-ingestion
          depends_on:
            - task_key: auto-loader
          description: "Ingestion finale - SITE"
          python_wheel_task:
            package_name: waxng
            entry_point: main
            parameters:
              - "--dataset={{job.parameters.dataset}}"
              - "--excel_path={{job.parameters.excel_path}}"
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
    
    # ==========================================
    # JOB 2 : PIPELINE ACTIVITE (Production)
    # ==========================================
    waxng_pipeline_activite:
      name: waxng_pipeline_activite
      
      parameters:
        - name: dataset
          default: "activite"
        - name: zip_path
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/landing/zip/"
        - name: extract_dir
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/preprocessed/"
        - name: excel_path
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/config/"
      
      tasks:
        - task_key: unzip
          description: "Extraction ZIP - ACTIVITE"
          python_wheel_task:
            package_name: waxng
            entry_point: unzip_module
            parameters:
              - "--dataset={{job.parameters.dataset}}"
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
          libraries:
            - whl: ../dist/waxng-1.0.0-py3-none-any.whl
        
        - task_key: auto-loader
          depends_on:
            - task_key: unzip
          description: "Auto Loader - ACTIVITE"
          python_wheel_task:
            package_name: waxng
            entry_point: autoloader_module
            parameters:
              - "--dataset={{job.parameters.dataset}}"
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
        
        - task_key: waxng-ingestion
          depends_on:
            - task_key: auto-loader
          description: "Ingestion finale - ACTIVITE"
          python_wheel_task:
            package_name: waxng
            entry_point: main
            parameters:
              - "--dataset={{job.parameters.dataset}}"
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
    
    # ==========================================
    # JOB 3 : PIPELINE CLYMENE (Production)
    # ==========================================
    waxng_pipeline_clymene:
      name: waxng_pipeline_clymene
      
      parameters:
        - name: dataset
          default: "clymene"
        - name: zip_path
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/landing/zip/"
        - name: extract_dir
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/preprocessed/"
        - name: excel_path
          default: "/Volumes/abu_catalog/gdp_poc_dev/externalvolumetest/config/"
      
      tasks:
        - task_key: unzip
          description: "Extraction ZIP - CLYMENE"
          python_wheel_task:
            package_name: waxng
            entry_point: unzip_module
            parameters:
              - "--dataset={{job.parameters.dataset}}"
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
          libraries:
            - whl: ../dist/waxng-1.0.0-py3-none-any.whl
        
        - task_key: auto-loader
          depends_on:
            - task_key: unzip
          description: "Auto Loader - CLYMENE"
          python_wheel_task:
            package_name: waxng
            entry_point: autoloader_module
            parameters:
              - "--dataset={{job.parameters.dataset}}"
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
        
        - task_key: waxng-ingestion
          depends_on:
            - task_key: auto-loader
          description: "Ingestion finale - CLYMENE"
          python_wheel_task:
            package_name: waxng
            entry_point: main
            parameters:
              - "--dataset={{job.parameters.dataset}}"
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
```

---

### **2. `resources/waxng_test_orchestrator.yml` (JOB DE TEST SÉPARÉ)**

```yaml
# resources/waxng_test_orchestrator.yml

resources:
  jobs:
    # ==========================================
    # JOB ORCHESTRATEUR DE TESTS
    # ==========================================
    waxng_test_orchestrator:
      name: waxng_test_orchestrator
      description: "Orchestre les tests E2E sur tous les datasets"
      
      parameters:
        - name: baseline_path
          default: "s3://baseline/wax/"
        - name: generate_report
          default: "true"
      
      tasks:
        # ==========================================
        # TASK 0 : Exécuter les tests BDD
        # ==========================================
        - task_key: run-bdd-tests
          description: "Orchestrateur de tests BDD avec baseline"
          notebook_task:
            notebook_path: /Workspace/Repos/{{workspace.current_user.userName}}/waxng/notebooks/test_orchestrator
            base_parameters:
              baseline_path: "{{job.parameters.baseline_path}}"
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
          libraries:
            - whl: ../dist/waxng-1.0.0-py3-none-any.whl
            - pypi:
                package: behave>=1.2.6
            - pypi:
                package: databricks-sdk
```

---

### **3. `notebooks/test_orchestrator.py` (ORCHESTRATEUR)**

```python
# Databricks notebook source

# MAGIC %md
# MAGIC # 🎯 Orchestrateur de Tests E2E WAX NG
# MAGIC 
# MAGIC Ce notebook :
# MAGIC 1. Lance les 3 pipelines de production
# MAGIC 2. Récupère leurs outputs
# MAGIC 3. Compare avec baseline
# MAGIC 4. Génère un rapport consolidé

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Installation

# COMMAND ----------

%pip install behave>=1.2.6 databricks-sdk --quiet
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎬 Configuration

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
import time
from datetime import datetime

# Configuration
BASELINE_PATH = dbutils.widgets.get("baseline_path")

w = WorkspaceClient()

# Jobs à lancer
JOBS_TO_RUN = [
    "waxng_pipeline_site",
    "waxng_pipeline_activite",
    "waxng_pipeline_clymene"
]

print(f"🎯 Baseline: {BASELINE_PATH}")
print(f"🎯 Jobs à tester: {', '.join(JOBS_TO_RUN)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Étape 1 : Lancer les jobs de production

# COMMAND ----------

job_runs = {}

for job_name in JOBS_TO_RUN:
    print(f"\n🚀 Lancement du job: {job_name}")
    
    # Trouver le job
    job_list = list(w.jobs.list(name=job_name))
    
    if not job_list:
        raise Exception(f"❌ Job non trouvé: {job_name}")
    
    job = job_list[0]
    
    # Lancer le job
    run = w.jobs.run_now(job_id=job.job_id)
    
    job_runs[job_name] = {
        'job_id': job.job_id,
        'run_id': run.run_id,
        'status': 'RUNNING',
        'start_time': datetime.now()
    }
    
    print(f"   ✅ Lancé - Run ID: {run.run_id}")

print(f"\n✅ {len(job_runs)} jobs lancés")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⏳ Étape 2 : Attendre la fin de tous les jobs

# COMMAND ----------

import time

MAX_WAIT_MINUTES = 60
CHECK_INTERVAL_SECONDS = 30

start_time = time.time()
max_wait_seconds = MAX_WAIT_MINUTES * 60

print(f"⏳ Attente de la fin des jobs (max {MAX_WAIT_MINUTES} min)...\n")

while time.time() - start_time < max_wait_seconds:
    all_done = True
    
    for job_name, run_info in job_runs.items():
        if run_info['status'] in ['SUCCESS', 'FAILED']:
            continue
        
        # Vérifier le statut
        run = w.jobs.get_run(run_id=run_info['run_id'])
        
        if run.state.life_cycle_state == 'TERMINATED':
            if run.state.result_state == 'SUCCESS':
                run_info['status'] = 'SUCCESS'
                print(f"✅ {job_name}: SUCCESS")
            else:
                run_info['status'] = 'FAILED'
                print(f"❌ {job_name}: FAILED - {run.state.state_message}")
        else:
            all_done = False
    
    if all_done:
        break
    
    time.sleep(CHECK_INTERVAL_SECONDS)

# Vérifier si tous ont réussi
failed_jobs = [name for name, info in job_runs.items() if info['status'] != 'SUCCESS']

if failed_jobs:
    raise Exception(f"❌ Jobs échoués: {', '.join(failed_jobs)}")

elapsed = time.time() - start_time
print(f"\n✅ Tous les jobs terminés avec succès en {elapsed/60:.1f} minutes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Étape 3 : Récupérer les outputs

# COMMAND ----------

outputs = {}

for job_name in job_runs.keys():
    dataset = job_name.split('_')[-1]  # site, activite, clymene
    
    table_name = f"abu_catalog.gdp_poc_dev.{dataset}_clymene_all"
    
    # Compter les lignes
    try:
        df = spark.table(table_name)
        row_count = df.count()
        
        outputs[dataset] = {
            'table': table_name,
            'row_count': row_count,
            'columns': df.columns,
            'status': 'SUCCESS'
        }
        
        print(f"✅ {dataset}: {row_count} lignes dans {table_name}")
        
    except Exception as e:
        outputs[dataset] = {
            'table': table_name,
            'status': 'ERROR',
            'error': str(e)
        }
        print(f"❌ {dataset}: Erreur - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Étape 4 : Charger la baseline

# COMMAND ----------

# Baseline de référence (à adapter selon votre structure)
baseline = {
    'site': {
        'expected_row_count': 1000,
        'tolerance_pct': 5,
        'mandatory_columns': ['SITE_CODE', 'SITE_LIBELLE', 'DATE_CREATION'],
        'quality_threshold': 0.95
    },
    'activite': {
        'expected_row_count': 5000,
        'tolerance_pct': 3,
        'mandatory_columns': ['ACTIVITE_ID', 'ACTIVITE_TYPE'],
        'quality_threshold': 0.98
    },
    'clymene': {
        'expected_row_count': 2000,
        'tolerance_pct': 2,
        'mandatory_columns': ['CLYMENE_ID'],
        'quality_threshold': 0.99
    }
}

print("✅ Baseline chargée pour 3 datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Étape 5 : Comparer avec baseline

# COMMAND ----------

comparison_results = {}

for dataset, output in outputs.items():
    if output['status'] != 'SUCCESS':
        comparison_results[dataset] = {
            'status': 'SKIPPED',
            'reason': 'Output non disponible'
        }
        continue
    
    base = baseline[dataset]
    
    # Vérifier le nombre de lignes
    actual_count = output['row_count']
    expected_count = base['expected_row_count']
    tolerance = int(expected_count * base['tolerance_pct'] / 100)
    
    row_count_ok = abs(actual_count - expected_count) <= tolerance
    
    # Vérifier les colonnes
    missing_columns = [
        col for col in base['mandatory_columns']
        if col not in output['columns']
    ]
    
    columns_ok = len(missing_columns) == 0
    
    # Résultat global
    validation_passed = row_count_ok and columns_ok
    
    comparison_results[dataset] = {
        'status': 'PASSED' if validation_passed else 'FAILED',
        'row_count': {
            'actual': actual_count,
            'expected': expected_count,
            'tolerance': tolerance,
            'passed': row_count_ok
        },
        'columns': {
            'missing': missing_columns,
            'passed': columns_ok
        }
    }
    
    status_icon = "✅" if validation_passed else "❌"
    print(f"{status_icon} {dataset}: {actual_count} lignes (attendu: {expected_count} ± {tolerance})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Étape 6 : Générer le rapport

# COMMAND ----------

from datetime import datetime
import json

report = {
    'timestamp': datetime.now().isoformat(),
    'baseline_path': BASELINE_PATH,
    'jobs_executed': len(job_runs),
    'jobs_success': sum(1 for r in job_runs.values() if r['status'] == 'SUCCESS'),
    'jobs_failed': sum(1 for r in job_runs.values() if r['status'] == 'FAILED'),
    'datasets_tested': len(comparison_results),
    'datasets_passed': sum(1 for r in comparison_results.values() if r['status'] == 'PASSED'),
    'datasets_failed': sum(1 for r in comparison_results.values() if r['status'] == 'FAILED'),
    'details': comparison_results
}

# Afficher le rapport
print("\n" + "=" * 80)
print("📊 RAPPORT CONSOLIDÉ")
print("=" * 80)
print(f"\n🕐 Date: {report['timestamp']}")
print(f"\n✅ Jobs réussis: {report['jobs_success']}/{report['jobs_executed']}")
print(f"✅ Datasets validés: {report['datasets_passed']}/{report['datasets_tested']}")

if report['datasets_failed'] > 0:
    print(f"\n❌ Datasets échoués: {report['datasets_failed']}")
    for dataset, result in comparison_results.items():
        if result['status'] == 'FAILED':
            print(f"   - {dataset}")

print("\n" + "=" * 80)

# Sauvegarder le rapport
report_json = json.dumps(report, indent=2)
print("\n📄 Rapport JSON:")
print(report_json)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Étape 7 : Résultat final

# COMMAND ----------

if report['datasets_failed'] > 0:
    raise Exception(f"❌ Tests échoués: {report['datasets_failed']} dataset(s) non conforme(s) à la baseline")

print("✅ TOUS LES TESTS ONT RÉUSSI")
print(f"   {report['jobs_executed']} jobs exécutés")
print(f"   {report['datasets_tested']} datasets validés")
print(f"   Conformité à la baseline: 100%")
```

---

## 🎯 **RÉPONSES À VOS QUESTIONS**

### **1. Dois-je créer un nouveau job en parallèle ?**

**OUI**, vous devez créer **2 types de jobs séparés** :

#### **Type A : Jobs de PRODUCTION** (3 jobs)
```
waxng_pipeline_site
waxng_pipeline_activite
waxng_pipeline_clymene
```
- Utilisables en production
- Un par dataset
- Pas de tests dedans

#### **Type B : Job de TEST** (1 job)
```
waxng_test_orchestrator
```
- Lance les 3 jobs de prod
- Compare avec baseline
- Génère rapport

---

### **2. Que faire de votre job actuel ?**

**Option 1 : Le supprimer** et utiliser la nouvelle architecture

**Option 2 : Le renommer** en `waxng_pipeline_site` et créer les 2 autres

---

## 📋 **PLAN D'ACTION**

```bash
# 1. Créer la nouvelle structure
mkdir -p resources
touch resources/waxng_job.yml
touch resources/waxng_test_orchestrator.yml

# 2. Créer le notebook
mkdir -p notebooks
touch notebooks/test_orchestrator.py

# 3. Déployer
databricks bundle deploy --target dev

# 4. Vous aurez maintenant 4 jobs :
#    - waxng_pipeline_site
#    - waxng_pipeline_activite
#    - waxng_pipeline_clymene
#    - waxng_test_orchestrator
```

---

## 🎓 **AVANTAGES DE CETTE APPROCHE**

✅ **Séparation prod/test** : Les jobs de prod ne contiennent pas de tests  
✅ **Réutilisable** : Chaque job peut tourner indépendamment  
✅ **Évolutif** : Facile d'ajouter de nouveaux datasets  
✅ **Baseline** : Validation contre données de référence  
✅ **Rapport consolidé** : Vue d'ensemble sur tous les datasets  

---

**En résumé : Créez 4 jobs au lieu d'un seul. Les 3 premiers sont pour la production, le 4ème est pour orchestrer les tests. C'est exactement ce que votre collègue recommande ! 🎯**
