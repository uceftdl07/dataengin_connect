# 🧪 Testing Guide

Guide complet pour exécuter et écrire des tests pour le framework d'ingestion.

## Installation

### 1. Installer les dépendances de test

```bash
pip install -r requirements-test.txt
```

## Exécution des Tests

### Tests Unitaires (Rapides)

```bash
# Tous les tests unitaires
pytest tests/unit/ -v

# Tests d'un module spécifique
pytest tests/unit/test_config_loader.py -v

# Tests d'une classe spécifique
pytest tests/unit/test_config_loader.py::TestConfigLoader -v

# Un seul test
pytest tests/unit/test_config_loader.py::TestConfigLoader::test_init_valid_domain -v
```

### Tests par Marker

```bash
# Seulement les tests unitaires
pytest -m unit

# Seulement les tests API
pytest -m api

# Seulement les tests SQL
pytest -m sql

# Exclure les tests lents
pytest -m "not slow"
```

### Tests avec Coverage

```bash
# Coverage rapport terminal
pytest --cov=src --cov-report=term-missing

# Coverage rapport HTML
pytest --cov=src --cov-report=html
# Ouvrir htmlcov/index.html dans un navigateur

# Échec si coverage < 80%
pytest --cov=src --cov-fail-under=80
```

### Tests en Parallèle

```bash
# Utiliser tous les CPU
pytest -n auto

# Utiliser 4 workers
pytest -n 4
```

### Options Utiles

```bash
# Stop au premier échec
pytest -x

# Afficher les print statements
pytest -s

# Mode verbeux avec durée
pytest -v --durations=10

# Ré-exécuter les tests échoués
pytest --lf

# Exécuter seulement les tests qui ont changé
pytest --testmon
```

## Structure des Tests

### Tests Unitaires (`tests/unit/`)

Tests rapides sans dépendances externes :
- `test_config_loader.py` - Chargement YAML
- `test_secret_manager.py` - Gestion secrets
- `test_api_connector.py` - Connecteur API
- `test_sql_connector.py` - Connecteur SQL Server
- `test_ingestor.py` - Logique d'ingestion

### Tests d'Intégration (`tests/integration/`)

Tests avec dépendances réelles (optionnels) :
- Connexion SQL Server réelle
- Appels API réels
- Écriture Delta Lake

## Écrire de Nouveaux Tests

### Template de Test Unitaire

```python
import pytest
from src.module import MyClass


@pytest.mark.unit
class TestMyClass:
    """Test MyClass functionality."""
    
    def test_basic_functionality(self):
        """Test basic case."""
        obj = MyClass()
        result = obj.method()
        assert result == expected_value
    
    def test_error_handling(self):
        """Test error cases."""
        obj = MyClass()
        with pytest.raises(ValueError, match="error message"):
            obj.method_that_fails()
    
    def test_with_mock(self, mocker):
        """Test with mocked dependency."""
        mock_dep = mocker.Mock()
        mock_dep.method.return_value = "mocked"
        
        obj = MyClass(dependency=mock_dep)
        result = obj.method()
        
        assert result == "expected"
        mock_dep.method.assert_called_once()
```

### Template de Test avec Fixtures

```python
import pytest


@pytest.fixture
def sample_data():
    """Provide sample data for tests."""
    return {"key": "value"}


@pytest.mark.unit
def test_with_fixture(sample_data):
    """Test using fixture."""
    assert sample_data["key"] == "value"
```

### Template de Test Paramétrisé

```python
import pytest


@pytest.mark.parametrize("input,expected", [
    ("hello", "HELLO"),
    ("world", "WORLD"),
    ("test", "TEST"),
])
def test_uppercase(input, expected):
    """Test with multiple inputs."""
    assert input.upper() == expected
```

## Mock des Composants

### Mock de dbutils

```python
def test_with_dbutils(mock_dbutils):
    """Test code that uses dbutils."""
    from src.utils.secret_manager import SecretManager
    
    manager = SecretManager(mock_dbutils)
    credentials = manager.get_sqlserver_credentials(config)
    
    # Vérifie que les secrets sont appelés
    mock_dbutils.secrets.get.assert_called()
```

### Mock de Spark

```python
def test_with_spark(spark):
    """Test with Spark session."""
    df = spark.createDataFrame([(1, "test")], ["id", "name"])
    assert df.count() == 1
```

### Mock de Requêtes HTTP

```python
import responses


@responses.activate
def test_api_call():
    """Test API call with mocked response."""
    responses.add(
        responses.GET,
        'https://api.example.com/data',
        json={'result': 'success'},
        status=200
    )
    
    # Code qui fait l'appel HTTP
    result = fetch_data()
    assert result == {'result': 'success'}
```

## Fixtures Disponibles

### Spark & Databricks
- `spark` - SparkSession de test
- `mock_dbutils` - Mock de dbutils avec secrets

### Configuration
- `sample_table_config` - Config table exemple
- `sample_incremental_table_config` - Config incremental
- `sample_api_config` - Config API exemple
- `sample_connector_config` - Config connector
- `sample_global_options` - Options globales

### Données
- `sample_dataframe` - DataFrame Spark de test
- `sample_api_response_data` - Réponse API exemple

### Fichiers
- `temp_config_dir` - Dossier config temporaire
- `temp_yaml_files` - Fichiers YAML de test

### Connecteurs
- `mock_sql_connector` - Mock du connector SQL
- `mock_api_connector` - Mock du connector API

## Bonnes Pratiques

### 1. Tests Isolés
```python
# ✅ BON - Chaque test est indépendant
def test_case_1():
    data = create_test_data()
    result = process(data)
    assert result == expected

def test_case_2():
    data = create_test_data()  # Nouvelles données
    result = process(data)
    assert result == expected

# ❌ MAUVAIS - Tests dépendants
shared_data = None

def test_case_1():
    global shared_data
    shared_data = create_test_data()
    # ...

def test_case_2():
    global shared_data  # Dépend de test_case_1
    # ...
```

### 2. Noms Descriptifs
```python
# ✅ BON
def test_load_incremental_returns_only_new_rows():
    """Test that incremental load returns only rows after watermark."""
    pass

# ❌ MAUVAIS
def test_1():
    pass
```

### 3. Un Assert par Test (quand possible)
```python
# ✅ BON
def test_table_name_is_correct():
    config = load_config()
    assert config['table_name'] == 'dbo.TestTable'

def test_load_mode_is_incremental():
    config = load_config()
    assert config['load_mode'] == 'incremental'

# 💡 ACCEPTABLE si logiquement liés
def test_config_structure():
    config = load_config()
    assert 'table_name' in config
    assert 'load_mode' in config
    assert config['enabled'] is True
```

### 4. Tester les Cas Limites
```python
def test_empty_dataframe():
    """Test behavior with empty DataFrame."""
    pass

def test_null_values():
    """Test handling of NULL values."""
    pass

def test_special_characters():
    """Test handling of special characters."""
    pass

def test_large_dataset():
    """Test with large volume of data."""
    pass
```

### 5. Messages d'Erreur Clairs
```python
# ✅ BON
assert result == expected, f"Expected {expected} but got {result}"

# ❌ MAUVAIS
assert result == expected
```

## CI/CD Integration

### GitHub Actions

```yaml
# .github/workflows/tests.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.9'
    
    - name: Install dependencies
      run: |
        pip install -r requirements-test.txt
    
    - name: Run tests
      run: |
        pytest --cov=src --cov-report=xml
    
    - name: Upload coverage
      uses: codecov/codecov-action@v2
```

## Métriques de Qualité

### Coverage Requis
- **Minimum**: 80% pour tout le projet
- **Objectif**: 90%+ pour code critique

### Types de Tests
- **Unitaires**: 70%+ des tests (rapides)
- **Intégration**: 20%+ des tests (réalistes)
- **E2E**: 10%+ des tests (complets)

## Debugging Tests

### Mode Debug
```bash
# Lancer avec debugger
pytest --pdb

# S'arrêter au premier échec et lancer debugger
pytest -x --pdb

# Tracer l'exécution
pytest --trace
```

### Logs Détaillés
```bash
# Afficher tous les logs
pytest --log-cli-level=DEBUG

# Capturer stdout/stderr
pytest -s --capture=no
```

## Commandes Rapides

```bash
# Quick check (tests rapides seulement)
pytest -m unit --tb=short

# Full check (tous les tests + coverage)
pytest --cov=src --cov-report=term-missing

# Pre-commit check
pytest -m unit --cov=src --cov-fail-under=80

# CI check
pytest --cov=src --cov-report=xml --junitxml=junit.xml
```

## Troubleshooting

### Erreur: "Module not found"
```bash
# Installer le projet en mode dev
pip install -e .

# Ou ajouter au PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

### Erreur: "Spark not found"
```bash
# Installer PySpark
pip install pyspark==3.5.0
```

### Tests Lents
```bash
# Identifier les tests lents
pytest --durations=10

# Paralléliser
pytest -n auto
```

---

**Version**: 1.0.0  
**Dernière mise à jour**: Février 2026
