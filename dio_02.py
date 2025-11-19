# 🎯 **REFACTORING COMPLET DE VOS FICHIERS**

Parfait ! Je vois votre configuration actuelle. Voici la refonte complète avec séparation prod/test.

---

## 📝 **1. `databricks.yml` (MISE À JOUR)**

```yaml
# databricks.yml
# Configuration principale - Renommé pour DIO

bundle:
  name: dio  # ✅ Changé de "wax-ng" à "dio"
  uuid: f6a5aabf-da0c-4743-813d-d29d1d03e094  # Garder votre UUID

artifacts:
  python_artifact:
    type: whl
    build: uv build --wheel

include:
  - resources/*.yml  # ✅ Inclut TOUS les .yml (dio_job.yml + dio_test_job.yml)

targets:
  # ==========================================
  # ENVIRONNEMENT DEV
  # ==========================================
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-4320079867405973.13.azuredatabricks.net
    
    # Pas de permissions spécifiques en dev
    
  # ==========================================
  # ENVIRONNEMENT PROD
  # ==========================================
  prod:
    mode: production
    workspace:
      host: https://adb-4320079867405973.13.azuredatabricks.net
      root_path: /Workspace/Tech/dio/.bundle/${bundle.name}/${bundle.target}
    
    permissions:
      - user_name: soumailou.toure.external@axa.com
        level: CAN_MANAGE
    
    # ✅ IMPORTANT : Exclure les tests en PROD
    sync:
      exclude:
        - "resources/dio_test_job.yml"
        - "features/**"
        - "tests/**"
```

---

## 📝 **2. `resources/dio_job.yml` (PRODUCTION UNIQUEMENT)**

```yaml
# resources/dio_job.yml
# Jobs de PRODUCTION - Pipelines d'ingestion par dataset

resources:
  jobs:
    # ==========================================
    # JOB 1 : PIPELINE SITE
    # ==========================================
    dio_pipeline_site:
      name: dio_pipeline_site
      description: "Pipeline d'ingestion DIO - Dataset SITE"
      
      tags:
        environment: "{{bundle.target}}"
        project: dio
        dataset: site
      
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
      
      tasks:
        # ==========================================
        # TASK 1 : Unzip
        # ==========================================
        - task_key: unzip
          description: "Extraction des fichiers ZIP - SITE"
          
          python_wheel_task:
            package_name: dio
            entry_point: unzip_module
            parameters:
              - "--dataset=site"
              - "--zip_path={{job.parameters.zip_path}}"
              - "--extract_dir={{job.parameters.extract_dir}}"
          
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
            spark_conf:
              spark.databricks.delta.preview.enabled: "true"
          
          libraries:
            - whl: ../dist/*.whl
        
        # ==========================================
        # TASK 2 : Auto Loader
        # ==========================================
        - task_key: auto-loader
          depends_on:
            - task_key: unzip
          description: "Ingestion automatique avec Auto Loader - SITE"
          
          python_wheel_task:
            package_name: dio
            entry_point: autoloader_module
            parameters:
              - "--dataset=site"
              - "--source_path={{job.parameters.extract_dir}}"
          
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
        
        # ==========================================
        # TASK 3 : Ingestion DIO
        # ==========================================
        - task_key: dio-ingestion
          depends_on:
            - task_key: auto-loader
          description: "Validation, transformation et ingestion finale - SITE"
          
          python_wheel_task:
            package_name: dio
            entry_point: main
            parameters:
              - "--dataset=site"
              - "--excel_path={{job.parameters.excel_path}}"
          
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
      
      # Configuration du job
      max_concurrent_runs: 1
      timeout_seconds: 3600
      
      # Environnements
      environments:
        - environment_key: default
          spec:
            environment_version: "2"
            dependencies:
              - ../dist/*.whl
              - behave>=1.2.6
              - pytest>=7.0.0
    
    # ==========================================
    # JOB 2 : PIPELINE ACTIVITE
    # ==========================================
    dio_pipeline_activite:
      name: dio_pipeline_activite
      description: "Pipeline d'ingestion DIO - Dataset ACTIVITE"
      
      tags:
        environment: "{{bundle.target}}"
        project: dio
        dataset: activite
      
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
      
      tasks:
        - task_key: unzip
          description: "Extraction des fichiers ZIP - ACTIVITE"
          python_wheel_task:
            package_name: dio
            entry_point: unzip_module
            parameters:
              - "--dataset=activite"
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
          libraries:
            - whl: ../dist/*.whl
        
        - task_key: auto-loader
          depends_on:
            - task_key: unzip
          description: "Auto Loader - ACTIVITE"
          python_wheel_task:
            package_name: dio
            entry_point: autoloader_module
            parameters:
              - "--dataset=activite"
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
        
        - task_key: dio-ingestion
          depends_on:
            - task_key: auto-loader
          description: "Ingestion finale - ACTIVITE"
          python_wheel_task:
            package_name: dio
            entry_point: main
            parameters:
              - "--dataset=activite"
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
      
      max_concurrent_runs: 1
      timeout_seconds: 3600
    
    # ==========================================
    # JOB 3 : PIPELINE CLYMENE
    # ==========================================
    dio_pipeline_clymene:
      name: dio_pipeline_clymene
      description: "Pipeline d'ingestion DIO - Dataset CLYMENE"
      
      tags:
        environment: "{{bundle.target}}"
        project: dio
        dataset: clymene
      
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
      
      tasks:
        - task_key: unzip
          description: "Extraction des fichiers ZIP - CLYMENE"
          python_wheel_task:
            package_name: dio
            entry_point: unzip_module
            parameters:
              - "--dataset=clymene"
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
          libraries:
            - whl: ../dist/*.whl
        
        - task_key: auto-loader
          depends_on:
            - task_key: unzip
          description: "Auto Loader - CLYMENE"
          python_wheel_task:
            package_name: dio
            entry_point: autoloader_module
            parameters:
              - "--dataset=clymene"
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
        
        - task_key: dio-ingestion
          depends_on:
            - task_key: auto-loader
          description: "Ingestion finale - CLYMENE"
          python_wheel_task:
            package_name: dio
            entry_point: main
            parameters:
              - "--dataset=clymene"
          existing_cluster_id: "{{tasks.unzip.cluster_id}}"
      
      max_concurrent_runs: 1
      timeout_seconds: 3600
```

---

## 📝 **3. `resources/dio_test_job.yml` (TESTS SÉPARÉS)**

```yaml
# resources/dio_test_job.yml
# Jobs de TEST - Tests d'intégration BDD

resources:
  jobs:
    # ==========================================
    # JOB DE TEST : INTEGRATION TESTS
    # ==========================================
    dio_integration_tests:
      name: dio_integration_tests
      description: "Tests d'intégration BDD avec Behave pour valider le pipeline DIO"
      
      tags:
        environment: "{{bundle.target}}"
        project: dio
        type: integration-tests
      
      parameters:
        - name: test_tags
          default: "integration"
          description: "Tags Behave à exécuter (@F12_01, @IT_RL1, etc.)"
        - name: test_feature
          default: "all"
          description: "Feature spécifique à tester (ou 'all')"
      
      tasks:
        # ==========================================
        # TASK : Exécuter tests d'intégration
        # ==========================================
        - task_key: run-integration-tests
          description: "Exécution des tests d'intégration avec Behave"
          
          python_wheel_task:
            package_name: dio
            entry_point: run_integration_tests
            parameters:
              - "--tags={{job.parameters.test_tags}}"
              - "--feature={{job.parameters.test_feature}}"
          
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
            spark_conf:
              spark.databricks.delta.preview.enabled: "true"
          
          libraries:
            - whl: ../dist/*.whl
          
          # Environnement avec dépendances de test
          environment_key: default
      
      # Configuration
      max_concurrent_runs: 3  # Peut tourner en parallèle
      timeout_seconds: 7200   # 2 heures max
      
      # Environnements avec behave
      environments:
        - environment_key: default
          spec:
            environment_version: "2"
            dependencies:
              - ../dist/*.whl
              - behave>=1.2.6
              - pytest>=7.0.0
    
    # ==========================================
    # JOB TEST : Format de fichiers
    # ==========================================
    dio_test_file_formats:
      name: dio_test_file_formats
      description: "Tests spécifiques aux formats de fichiers (F12, IT_CSV, IT_NONSTRUCT)"
      
      tags:
        environment: "{{bundle.target}}"
        project: dio
        test-suite: file-formats
      
      tasks:
        - task_key: test-formats
          description: "Tests formats"
          python_wheel_task:
            package_name: dio
            entry_point: run_integration_tests
            parameters:
              - "--tags=@F12_01_FORMAT_1,@IT_CSV_FORMAT_1,@IT_NONSTRUCT_1"
              - "--feature=file_formats"
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
          libraries:
            - whl: ../dist/*.whl
          environment_key: default
      
      max_concurrent_runs: 1
      timeout_seconds: 3600
      
      environments:
        - environment_key: default
          spec:
            environment_version: "2"
            dependencies:
              - ../dist/*.whl
              - behave>=1.2.6
    
    # ==========================================
    # JOB TEST : Règles de rejet
    # ==========================================
    dio_test_rejection_rules:
      name: dio_test_rejection_rules
      description: "Tests des règles de rejet RLT (IT_RL1, IT_RLT_2)"
      
      tags:
        environment: "{{bundle.target}}"
        project: dio
        test-suite: rejection-rules
      
      tasks:
        - task_key: test-rejection
          description: "Tests RLT"
          python_wheel_task:
            package_name: dio
            entry_point: run_integration_tests
            parameters:
              - "--tags=@IT_RL1,@IT_RLT_2"
              - "--feature=rejection_rules"
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
          libraries:
            - whl: ../dist/*.whl
          environment_key: default
      
      max_concurrent_runs: 1
      timeout_seconds: 3600
      
      environments:
        - environment_key: default
          spec:
            environment_version: "2"
            dependencies:
              - ../dist/*.whl
              - behave>=1.2.6
```

---

## 📝 **4. `dio/run_integration_tests.py` (ENTRY POINT)**

```python
# dio/run_integration_tests.py
"""
Entry point pour exécuter les tests d'intégration via python_wheel_task
"""

import sys
import argparse
import subprocess
from pathlib import Path


def main():
    """Exécuter les tests d'intégration avec Behave"""
    
    parser = argparse.ArgumentParser(description="Tests d'intégration DIO")
    parser.add_argument('--tags', default='integration', help='Tags Behave')
    parser.add_argument('--feature', default='all', help='Feature à tester')
    args = parser.parse_args()
    
    # Trouver le dossier features
    import dio
    package_dir = Path(dio.__file__).parent
    features_dir = package_dir.parent / "features" / "integration"
    
    print("=" * 80)
    print("🧪 TESTS D'INTÉGRATION DIO")
    print("=" * 80)
    print(f"📂 Features: {features_dir}")
    print(f"🏷️  Tags: {args.tags}")
    print(f"📋 Feature: {args.feature}")
    print("=" * 80 + "\n")
    
    # Construire la commande Behave
    cmd = [
        sys.executable, "-m", "behave",
        str(features_dir),
        "--format", "pretty",
        "--no-capture",
        "--color"
    ]
    
    # Ajouter les tags
    if args.tags and args.tags != 'all':
        cmd.extend(["--tags", args.tags])
    
    # Exécuter
    result = subprocess.run(cmd, capture_output=False)
    
    # Résultat
    print("\n" + "=" * 80)
    if result.returncode == 0:
        print("✅ TOUS LES TESTS ONT RÉUSSI")
        print("=" * 80)
        sys.exit(0)
    else:
        print("❌ CERTAINS TESTS ONT ÉCHOUÉ")
        print("=" * 80)
        sys.exit(1)


if __name__ == "__main__":
    main()
```

---

## 📝 **5. `setup.py` (MISE À JOUR)**

```python
# setup.py

from setuptools import setup, find_packages
import os


def read_requirements():
    req_path = os.path.join(os.path.dirname(__file__), "requirements.txt")
    if os.path.exists(req_path):
        with open(req_path, "r") as f:
            return [line.strip() for line in f if line.strip() and not line.startswith("#")]
    return []


setup(
    name="dio",  # ✅ Changé de "waxng" à "dio"
    version="1.0.0",
    packages=find_packages(),
    
    # Inclure les features
    package_data={
        "": ["*.feature", "*.md", "*.txt"],
    },
    include_package_data=True,
    
    # Dépendances
    install_requires=read_requirements(),
    
    # Entry points
    entry_points={
        "console_scripts": [
            "dio-pipeline=dio.main:main",
            "dio-unzip=dio.unzip_module:main",
            "dio-autoloader=dio.autoloader_module:main",
            "dio-tests=dio.run_integration_tests:main",  # ✅ NOUVEAU
        ],
    },
    
    zip_safe=False,
)
```

---

## 🚀 **PLAN DE MIGRATION**

### **Étape 1 : Renommer les fichiers**

```bash
# 1. Renommer le dossier principal
mv waxng/ dio/

# 2. Renommer le fichier job
mv resources/waxng_job.yml resources/dio_job.yml

# 3. Créer le fichier de tests
touch resources/dio_test_job.yml
```

---

### **Étape 2 : Mettre à jour les imports**

```bash
# Dans tous les fichiers Python
find dio/ -name "*.py" -exec sed -i 's/import waxng/import dio/g' {} +
find dio/ -name "*.py" -exec sed -i 's/from waxng/from dio/g' {} +
```

---

### **Étape 3 : Builder et déployer**

```bash
# 1. Nettoyer
rm -rf dist/ build/ *.egg-info

# 2. Builder avec le nouveau nom
uv build --wheel

# 3. Vérifier le wheel
ls -lh dist/
# Devrait afficher : dio-1.0.0-py3-none-any.whl

# 4. Déployer en DEV (inclut prod + tests)
databricks bundle deploy --target dev

# 5. Vérifier les jobs créés
databricks jobs list --output json | jq '.jobs[] | select(.settings.name | contains("dio")) | .settings.name'
```

**Résultat attendu en DEV :**
```
dio_pipeline_site
dio_pipeline_activite
dio_pipeline_clymene
dio_integration_tests
dio_test_file_formats
dio_test_rejection_rules
```

---

### **Étape 4 : Tester**

```bash
# Lancer les tests
databricks jobs run-now --job-name dio_integration_tests

# Lancer un pipeline
databricks jobs run-now --job-name dio_pipeline_site
```

---

### **Étape 5 : Déployer en PROD (sans tests)**

```bash
# En prod, les tests sont automatiquement exclus
databricks bundle deploy --target prod
```

**Résultat attendu en PROD :**
```
dio_pipeline_site          ✅
dio_pipeline_activite      ✅
dio_pipeline_clymene       ✅
dio_integration_tests      ❌ EXCLU
dio_test_file_formats      ❌ EXCLU
dio_test_rejection_rules   ❌ EXCLU
```

---

## 📊 **COMPARAISON AVANT/APRÈS**

| Aspect | Avant (waxng_job.yml) | Après (dio_job.yml + dio_test_job.yml) |
|--------|----------------------|----------------------------------------|
| **Fichiers** | 1 fichier | 2 fichiers séparés |
| **Jobs** | 1 job mixte | 6 jobs (3 prod + 3 test) |
| **Tests** | Task 0 dans prod | Jobs séparés |
| **Dépendances** | Tests bloquent prod | Indépendants |
| **Parallélisme** | ❌ Séquentiel | ✅ Parallèle possible |
| **Déploiement** | ❌ Tout ou rien | ✅ Sélectif |
| **Prod** | ❌ Tests inclus | ✅ Tests exclus |

---

**Voilà ! Votre projet est maintenant correctement structuré avec une séparation claire entre production et tests. 🎯**

**Prêt à déployer avec `databricks bundle deploy` ! 🚀**
