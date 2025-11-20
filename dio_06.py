# 🔧 **ERREUR DÉTECTÉE : `job_name` n'est pas permis !**

Je vois l'erreur dans votre capture d'écran :

```
Property job_name is not allowed. yaml-schema: dabs://databricks-asset-bundles.json(513)
```

**Problème à la ligne 42 :** La propriété `job_name` n'existe pas dans `run_job_task` pour Databricks Asset Bundles.

---

## ✅ **SOLUTION : Utiliser `job_id`**

Il y a **2 solutions** :

### **Solution 1 : Référence par ID (recommandée pour l'instant)**

```yaml
# waxng_test_job.yml - LIGNE 38-46

- task_key: run-pipeline
  description: "Exécuter le pipeline WAX NG"
  
  run_job_task:
    job_id: "{{bundle.resources.jobs.waxng_job.id}}"  # ✅ CORRECTION
    job_parameters:
      dataset: "{{job.parameters.dataset}}"
      excel_path: "{{job.parameters.excel_path}}"
      zip_path: "{{job.parameters.zip_path}}"
```

---

### **Solution 2 : Appel via Databricks SDK (alternative)**

Si la solution 1 ne fonctionne pas, utilisez un **notebook** pour lancer le job :

```yaml
# waxng_test_job.yml - LIGNE 38-50

- task_key: run-pipeline
  description: "Exécuter le pipeline WAX NG"
  
  notebook_task:
    notebook_path: /Workspace/Repos/{{workspace.current_user.userName}}/waxng/notebooks/launch_pipeline
    base_parameters:
      pipeline_job_name: "{{job.parameters.pipeline_job_name}}"
      dataset: "{{job.parameters.dataset}}"
      excel_path: "{{job.parameters.excel_path}}"
      zip_path: "{{job.parameters.zip_path}}"
  
  new_cluster:
    spark_version: "14.3.x-scala2.12"
    node_type_id: "Standard_DS3_v2"
    num_workers: 1
  
  libraries:
    - pypi:
        package: databricks-sdk
```

**Créer le notebook :** `notebooks/launch_pipeline.py`

```python
# Databricks notebook source

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunNow

# Paramètres
pipeline_job_name = dbutils.widgets.get("pipeline_job_name")
dataset = dbutils.widgets.get("dataset")
excel_path = dbutils.widgets.get("excel_path")
zip_path = dbutils.widgets.get("zip_path")

print(f"🚀 Lancement du job: {pipeline_job_name}")
print(f"   Dataset: {dataset}")

# Client Databricks
w = WorkspaceClient()

# Trouver le job par nom
jobs = w.jobs.list(name=pipeline_job_name)
job_id = None

for job in jobs:
    if job.settings.name == pipeline_job_name:
        job_id = job.job_id
        break

if not job_id:
    raise Exception(f"Job '{pipeline_job_name}' non trouvé")

print(f"   Job ID: {job_id}")

# Lancer le job
run = w.jobs.run_now(
    job_id=job_id,
    job_parameters={
        "dataset": dataset,
        "excel_path": excel_path,
        "zip_path": zip_path
    }
)

print(f"   Run ID: {run.run_id}")

# Attendre la fin
print("⏳ Attente de la fin du job...")

w.jobs.wait_get_run_job_terminated_or_skipped(run_id=run.run_id)

# Vérifier le statut
run_info = w.jobs.get_run(run_id=run.run_id)

if run_info.state.life_cycle_state == "TERMINATED":
    if run_info.state.result_state == "SUCCESS":
        print("✅ Job terminé avec succès")
    else:
        raise Exception(f"❌ Job échoué: {run_info.state.state_message}")
else:
    raise Exception(f"❌ Job dans un état inattendu: {run_info.state.life_cycle_state}")
```

---

## 📝 **FICHIER COMPLET CORRIGÉ : `waxng_test_job.yml`**

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
          
          # ✅ SOLUTION : Utiliser un notebook au lieu de run_job_task
          notebook_task:
            notebook_path: /Workspace/Repos/{{workspace.current_user.userName}}/waxng/notebooks/launch_pipeline
            base_parameters:
              pipeline_job_name: "{{job.parameters.pipeline_job_name}}"
              dataset: "{{job.parameters.dataset}}"
              excel_path: "{{job.parameters.excel_path}}"
              zip_path: "{{job.parameters.zip_path}}"
          
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
          
          libraries:
            - whl: ../dist/*.whl
            - pypi:
                package: databricks-sdk
        
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
          
          existing_cluster_id: "{{tasks.run-pipeline.cluster_id}}"
        
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
          
          existing_cluster_id: "{{tasks.run-pipeline.cluster_id}}"
      
      max_concurrent_runs: 2
      timeout_seconds: 7200
      
      email_notifications:
        on_failure:
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
```

---

## 📂 **FICHIERS À CRÉER**

### **1. `notebooks/launch_pipeline.py`**

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Lancer le pipeline WAX NG

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunNow
import time

# COMMAND ----------

# Paramètres
dbutils.widgets.text("pipeline_job_name", "waxng_job")
dbutils.widgets.text("dataset", "site")
dbutils.widgets.text("excel_path", "")
dbutils.widgets.text("zip_path", "")

pipeline_job_name = dbutils.widgets.get("pipeline_job_name")
dataset = dbutils.widgets.get("dataset")
excel_path = dbutils.widgets.get("excel_path")
zip_path = dbutils.widgets.get("zip_path")

# COMMAND ----------

print("=" * 80)
print(f"🚀 LANCEMENT DU PIPELINE")
print("=" * 80)
print(f"Job: {pipeline_job_name}")
print(f"Dataset: {dataset}")
print(f"Excel: {excel_path}")
print(f"ZIP: {zip_path}")
print("=" * 80)

# COMMAND ----------

# Client Databricks
w = WorkspaceClient()

# Trouver le job par nom
print(f"\n🔍 Recherche du job '{pipeline_job_name}'...")

jobs_list = list(w.jobs.list(name=pipeline_job_name))
job_id = None

for job in jobs_list:
    if job.settings.name == pipeline_job_name:
        job_id = job.job_id
        print(f"✅ Job trouvé - ID: {job_id}")
        break

if not job_id:
    raise Exception(f"❌ Job '{pipeline_job_name}' non trouvé")

# COMMAND ----------

# Lancer le job
print(f"\n🚀 Lancement du job...")

run = w.jobs.run_now(
    job_id=job_id,
    job_parameters={
        "dataset": dataset,
        "excel_path": excel_path,
        "zip_path": zip_path
    }
)

run_id = run.run_id
print(f"✅ Run lancé - Run ID: {run_id}")
print(f"🔗 URL: https://adb-4320079867405973.13.azuredatabricks.net/#job/{job_id}/run/{run_id}")

# COMMAND ----------

# Attendre la fin
print(f"\n⏳ Attente de la fin du job...")

try:
    # Attendre avec timeout
    w.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=run_id,
        timeout=timedelta(hours=1)
    )
    
    # Récupérer les détails
    run_info = w.jobs.get_run(run_id=run_id)
    
    print("\n" + "=" * 80)
    if run_info.state.result_state == "SUCCESS":
        print("✅ JOB TERMINÉ AVEC SUCCÈS")
        print("=" * 80)
    else:
        print(f"❌ JOB ÉCHOUÉ")
        print(f"État: {run_info.state.result_state}")
        print(f"Message: {run_info.state.state_message}")
        print("=" * 80)
        raise Exception(f"Job échoué: {run_info.state.state_message}")

except Exception as e:
    print(f"\n❌ ERREUR: {e}")
    raise
```

---

## 🚀 **DÉPLOIEMENT**

```bash
# 1. Créer le notebook
mkdir -p notebooks
# Copier le contenu de launch_pipeline.py

# 2. Builder
rm -rf dist/ build/ *.egg-info
uv build --wheel

# 3. Déployer
databricks bundle deploy --target dev

# 4. Tester
databricks jobs run-now --job-name waxng_test_validation \
  --parameters dataset=site
```

---

## ✅ **RÉSUMÉ DE LA CORRECTION**

| Avant (❌ Erreur) | Après (✅ Corrigé) |
|-------------------|---------------------|
| `run_job_task: job_name: ...` | `notebook_task: launch_pipeline.py` |
| Propriété non supportée | Appel via SDK Databricks |

---

**Voilà ! L'erreur est corrigée. Le job de test lance maintenant le pipeline via un notebook avec le SDK Databricks. 🎯**
