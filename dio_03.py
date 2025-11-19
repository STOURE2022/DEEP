# 🎯 **OK, SOYONS TRÈS CONCRETS !**

Je comprends mieux maintenant. Laissez-moi vous expliquer **concrètement** comment ça va fonctionner.

---

## 📊 **SITUATION ACTUELLE**

Vous avez **1 job qui marche** :

```yaml
waxng_job:
  tasks:
    - task 0: bdd-tests (À ENLEVER)
    - task 1: unzip
    - task 2: auto-loader
    - task 3: waxng-ingestion (lit Excel, fait tout le traitement)
```

**Ce job :**
- ✅ Lit votre fichier Excel
- ✅ Traite les fichiers
- ✅ Crée les tables Delta (ex: `site_clymene_all`, `activite_all`)
- ✅ Fonctionne déjà

**Problème :** La task 0 (tests BDD) est mélangée avec la production.

---

## 🔧 **CE QU'ON VA FAIRE**

### **Étape 1 : Nettoyer le job de production**

Enlever la task 0 (tests) de votre job actuel :

```yaml
# resources/dio_job.yml (anciennement waxng_job.yml)

resources:
  jobs:
    dio_pipeline:  # Votre job actuel, sans les tests
      name: dio_pipeline
      
      parameters:
        - name: dataset
          default: "site"
        - name: zip_path
          default: ""
        - name: excel_path
          default: ""
        - name: extract_dir
          default: ""
      
      tasks:
        # ❌ PLUS DE TASK 0 (tests)
        
        # Task 1 : Unzip
        - task_key: unzip
          python_wheel_task:
            package_name: dio
            entry_point: unzip_module
          # ... votre config actuelle
        
        # Task 2 : Auto Loader
        - task_key: auto-loader
          depends_on:
            - task_key: unzip
          python_wheel_task:
            package_name: dio
            entry_point: autoloader_module
          # ... votre config actuelle
        
        # Task 3 : Ingestion
        - task_key: dio-ingestion
          depends_on:
            - task_key: auto-loader
          python_wheel_task:
            package_name: dio
            entry_point: main
          # ... votre config actuelle
```

**Résultat :** Votre job `dio_pipeline` est maintenant **pur production**, pas de tests dedans.

---

### **Étape 2 : Créer des tables de BASELINE (référence)**

**C'est ça la clé !** Vous devez avoir des **tables de référence** avec les résultats attendus.

#### **Comment créer la baseline ?**

**Option A : Première exécution manuelle**

```sql
-- 1. Lancer votre pipeline une première fois sur des données de test connues
-- 2. Vérifier manuellement que les résultats sont corrects
-- 3. Sauvegarder ces résultats comme BASELINE

CREATE TABLE abu_catalog.gdp_poc_dev.baseline_site_clymene_all
AS SELECT * FROM abu_catalog.gdp_poc_dev.site_clymene_all;

CREATE TABLE abu_catalog.gdp_poc_dev.baseline_activite_all
AS SELECT * FROM abu_catalog.gdp_poc_dev.activite_all;

-- 4. Sauvegarder aussi les métriques
CREATE TABLE abu_catalog.gdp_poc_dev.baseline_metrics (
  dataset STRING,
  row_count LONG,
  null_count LONG,
  quality_score DOUBLE,
  date_created DATE
);

INSERT INTO abu_catalog.gdp_poc_dev.baseline_metrics VALUES
('site', 1000, 0, 0.98, '2024-01-15'),
('activite', 5000, 5, 0.95, '2024-01-15');
```

**Option B : Fichiers CSV de référence**

```
baseline/
├── site_expected.csv       # Résultats attendus pour SITE
├── activite_expected.csv   # Résultats attendus pour ACTIVITE
└── metrics.json            # Métriques attendues
```

---

### **Étape 3 : Créer le job de TEST**

Voici le **nouveau job de test** qui va comparer les résultats :

```yaml
# resources/dio_test_job.yml

resources:
  jobs:
    dio_test_validation:
      name: dio_test_validation
      description: "Valide les résultats du pipeline en comparant avec la baseline"
      
      parameters:
        - name: pipeline_job_name
          default: "dio_pipeline"
        - name: dataset
          default: "site"
      
      tasks:
        # ==========================================
        # TASK 1 : Lancer le pipeline de production
        # ==========================================
        - task_key: run-pipeline
          description: "Lancer le pipeline DIO"
          
          run_job_task:
            job_name: "{{job.parameters.pipeline_job_name}}"
            job_parameters:
              dataset: "{{job.parameters.dataset}}"
        
        # ==========================================
        # TASK 2 : Comparer avec la baseline
        # ==========================================
        - task_key: compare-results
          depends_on:
            - task_key: run-pipeline
          description: "Comparer résultats avec baseline"
          
          notebook_task:
            notebook_path: /Workspace/Repos/.../notebooks/compare_with_baseline
            base_parameters:
              dataset: "{{job.parameters.dataset}}"
          
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
        
        # ==========================================
        # TASK 3 : Générer le rapport
        # ==========================================
        - task_key: generate-report
          depends_on:
            - task_key: compare-results
          description: "Générer rapport de validation"
          
          notebook_task:
            notebook_path: /Workspace/Repos/.../notebooks/generate_test_report
            base_parameters:
              dataset: "{{job.parameters.dataset}}"
          
          existing_cluster_id: "{{tasks.compare-results.cluster_id}}"
```

---

### **Étape 4 : Le notebook de comparaison**

Voici **le notebook concret** qui fait la comparaison :

```python
# Databricks notebook source
# notebooks/compare_with_baseline.py

# MAGIC %md
# MAGIC # 🔍 Comparaison avec Baseline

# COMMAND ----------

# Récupérer le paramètre
dataset = dbutils.widgets.get("dataset")

print(f"📊 Validation du dataset: {dataset}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Charger les données

# COMMAND ----------

# Table produite par le pipeline (résultat actuel)
actual_table = f"abu_catalog.gdp_poc_dev.{dataset}_clymene_all"
df_actual = spark.table(actual_table)

# Table de baseline (résultat attendu)
baseline_table = f"abu_catalog.gdp_poc_dev.baseline_{dataset}_clymene_all"
df_baseline = spark.table(baseline_table)

print(f"✅ Actual: {df_actual.count()} lignes")
print(f"✅ Baseline: {df_baseline.count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Comparaison : Nombre de lignes

# COMMAND ----------

actual_count = df_actual.count()
baseline_count = df_baseline.count()

diff_count = actual_count - baseline_count
diff_pct = (diff_count / baseline_count * 100) if baseline_count > 0 else 0

print(f"📊 NOMBRE DE LIGNES:")
print(f"   Attendu (baseline): {baseline_count}")
print(f"   Obtenu (actual):    {actual_count}")
print(f"   Différence:         {diff_count:+d} ({diff_pct:+.2f}%)")

# Vérification
tolerance_pct = 5  # Tolérance de 5%
row_count_ok = abs(diff_pct) <= tolerance_pct

if row_count_ok:
    print(f"✅ NOMBRE DE LIGNES OK (tolérance: ±{tolerance_pct}%)")
else:
    print(f"❌ NOMBRE DE LIGNES HORS TOLÉRANCE !")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Comparaison : Schéma

# COMMAND ----------

actual_columns = set(df_actual.columns)
baseline_columns = set(df_baseline.columns)

missing_columns = baseline_columns - actual_columns
extra_columns = actual_columns - baseline_columns

print(f"📋 SCHÉMA:")
print(f"   Colonnes attendues: {len(baseline_columns)}")
print(f"   Colonnes obtenues:  {len(actual_columns)}")

if missing_columns:
    print(f"   ❌ Colonnes manquantes: {missing_columns}")

if extra_columns:
    print(f"   ⚠️  Colonnes en plus: {extra_columns}")

schema_ok = (len(missing_columns) == 0)

if schema_ok:
    print("✅ SCHÉMA OK")
else:
    print("❌ SCHÉMA DIFFÉRENT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Comparaison : Valeurs NULL

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, count, when

# Compter les NULL dans chaque colonne
null_counts_actual = {}
null_counts_baseline = {}

for col_name in baseline_columns:
    if col_name in actual_columns:
        null_counts_actual[col_name] = df_actual.filter(col(col_name).isNull()).count()
        null_counts_baseline[col_name] = df_baseline.filter(col(col_name).isNull()).count()

print(f"🔍 VALEURS NULL:")
null_ok = True

for col_name in sorted(null_counts_baseline.keys()):
    baseline_null = null_counts_baseline[col_name]
    actual_null = null_counts_actual.get(col_name, 0)
    diff_null = actual_null - baseline_null
    
    if diff_null != 0:
        print(f"   {col_name}: {actual_null} NULL (baseline: {baseline_null}) {diff_null:+d}")
        if actual_null > baseline_null:
            null_ok = False
    else:
        print(f"   {col_name}: {actual_null} NULL ✅")

if null_ok:
    print("✅ VALEURS NULL OK")
else:
    print("❌ PLUS DE NULL QUE DANS LA BASELINE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Comparaison : Échantillon de données

# COMMAND ----------

# Comparer quelques lignes (si possible avec une clé)
if dataset == "site" and "SITE_CODE" in actual_columns:
    # Prendre 10 lignes pour comparaison
    sample_codes = df_baseline.select("SITE_CODE").limit(10).rdd.flatMap(lambda x: x).collect()
    
    print(f"🔍 COMPARAISON ÉCHANTILLON (10 lignes):")
    
    for code in sample_codes:
        actual_row = df_actual.filter(col("SITE_CODE") == code).first()
        baseline_row = df_baseline.filter(col("SITE_CODE") == code).first()
        
        if actual_row is None:
            print(f"   ❌ {code}: MANQUANT dans actual")
        elif actual_row == baseline_row:
            print(f"   ✅ {code}: IDENTIQUE")
        else:
            print(f"   ⚠️  {code}: DIFFÉRENCES détectées")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Sauvegarder les résultats

# COMMAND ----------

import json
from datetime import datetime

# Résumé de la comparaison
comparison_result = {
    "dataset": dataset,
    "timestamp": datetime.now().isoformat(),
    "row_count": {
        "baseline": baseline_count,
        "actual": actual_count,
        "diff": diff_count,
        "diff_pct": diff_pct,
        "ok": row_count_ok
    },
    "schema": {
        "missing_columns": list(missing_columns),
        "extra_columns": list(extra_columns),
        "ok": schema_ok
    },
    "null_check": {
        "ok": null_ok
    },
    "overall_status": "PASSED" if (row_count_ok and schema_ok and null_ok) else "FAILED"
}

# Sauvegarder dans une table
result_df = spark.createDataFrame([comparison_result])
result_df.write.mode("append").saveAsTable("abu_catalog.gdp_poc_dev.test_validation_results")

print("\n" + "=" * 80)
print(f"📊 RÉSULTAT GLOBAL: {comparison_result['overall_status']}")
print("=" * 80)

# Si échec, lever une exception
if comparison_result['overall_status'] == "FAILED":
    raise Exception("❌ Validation échouée - Résultats différents de la baseline")

print("✅ Validation réussie - Résultats conformes à la baseline")
```

---

## 🚀 **UTILISATION CONCRÈTE**

### **1. Créer la baseline (une seule fois)**

```bash
# Lancer le pipeline sur des données de test connues
databricks jobs run-now --job-name dio_pipeline --parameters dataset=site

# Vérifier manuellement les résultats
# Si OK, créer la baseline via SQL
```

```sql
-- Dans Databricks SQL
CREATE TABLE abu_catalog.gdp_poc_dev.baseline_site_clymene_all
AS SELECT * FROM abu_catalog.gdp_poc_dev.site_clymene_all;
```

### **2. Lancer le job de test**

```bash
# Le job de test va :
# 1. Lancer dio_pipeline
# 2. Comparer avec baseline
# 3. Générer rapport

databricks jobs run-now --job-name dio_test_validation \
  --parameters dataset=site
```

### **3. Voir les résultats**

```sql
-- Table des résultats de validation
SELECT * 
FROM abu_catalog.gdp_poc_dev.test_validation_results
ORDER BY timestamp DESC
LIMIT 10;
```

---

## 📊 **SCHÉMA COMPLET**

```
┌─────────────────────────────────────┐
│   JOB 1: dio_pipeline (PRODUCTION)  │
│   ─────────────────────────────────  │
│   1. Unzip                           │
│   2. Auto Loader                     │
│   3. Ingestion (lit Excel)           │
│   → Crée: site_clymene_all          │
└─────────────────────────────────────┘
            ↓
┌─────────────────────────────────────┐
│   JOB 2: dio_test_validation (TEST) │
│   ─────────────────────────────────  │
│   1. Lance dio_pipeline             │
│   2. Compare avec baseline          │
│      ├─ Nombre lignes                │
│      ├─ Schéma                       │
│      ├─ Valeurs NULL                 │
│      └─ Échantillon données          │
│   3. Génère rapport                  │
│   → Crée: test_validation_results   │
└─────────────────────────────────────┘
```

---

## ✅ **RÉSUMÉ SIMPLE**

1. **Votre job actuel** → reste identique, on enlève juste la task 0 (tests)
2. **Baseline** → Créer des tables de référence avec les résultats attendus
3. **Job de test** → Lance votre pipeline + compare les résultats avec baseline
4. **Rapport** → Tables SQL avec résultats de comparaison

**Pas besoin de toucher à votre Excel ni à votre logique actuelle !** On ajoute juste un job de test à côté.

---

**C'est plus clair comme ça ? 🎯**
