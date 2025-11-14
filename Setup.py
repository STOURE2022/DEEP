# setup.py

from setuptools import setup, find_packages
import os

# Lire le README
def read_long_description():
    here = os.path.abspath(os.path.dirname(__file__))
    readme_path = os.path.join(here, 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, encoding='utf-8') as f:
            return f.read()
    return "WAX NG - Pipeline de données pour Databricks"

setup(
    name="waxng",
    version="1.0.1",
    author="Soumailou Touré",
    author_email="soumailou.toure.external@axa.com",
    description="WAX NG - Pipeline de données pour Databricks",
    long_description=read_long_description(),
    long_description_content_type="text/markdown",
    
    # ✅ Trouver automatiquement les packages
    packages=find_packages(exclude=["tests", "tests.*", "*.tests", "*.tests.*"]),
    
    # ✅ Inclure les fichiers non-Python
    package_data={
        "": ["*.feature", "*.md"],  # Inclure tous les .feature et .md
    },
    include_package_data=True,
    
    # ✅ Python minimum
    python_requires=">=3.11",
    
    # ✅ Dépendances principales
    install_requires=[
        "pyspark>=3.0.0",
        "pandas>=1.3.0",
        "openpyxl>=3.0.0",
    ],
    
    # ✅ Dépendances optionnelles
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "behave>=1.2.6",
        ],
    },
    
    # ✅ Points d'entrée (scripts exécutables)
    entry_points={
        "console_scripts": [
            "waxng-unzip=waxng.unzip_module:main",
            "waxng-autoloader=waxng.autoloader_module:main",
            "waxng-ingestion=waxng.main:main",
            "waxng-tests=waxng.run_bdd_tests:main",
        ],
    },
    
    # ✅ Classifiers pour PyPI (optionnel)
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
    ],
)
