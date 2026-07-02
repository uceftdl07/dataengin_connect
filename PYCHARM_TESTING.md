# 🐍 PyCharm - Guide de Tests

Guide pour exécuter les tests dans PyCharm IDE.

## 🚀 Configuration Initiale

### 1. Ouvrir le Projet
```
File → Open → Sélectionner: jdbc-ingestion-engine/
```

### 2. Configurer l'Interpréteur Python

**Option A: Créer un Virtual Environment**
```bash
# Dans le terminal PyCharm
python -m venv .venv
source .venv/bin/activate  # Mac/Linux
# ou
.venv\Scripts\activate     # Windows
```

**Option B: Utiliser PyCharm**
```
PyCharm → Preferences → Project → Python Interpreter
→ Add Interpreter → Virtualenv Environment → New
```

### 3. Installer les Dépendances

```bash
# Dans le terminal PyCharm
pip install -r requirements-test.txt
```

### 4. Configurer pytest comme Test Runner

```
PyCharm → Preferences → Tools → Python Integrated Tools
→ Testing → Default test runner: pytest
→ Apply
```

## ✅ Exécuter les Tests dans PyCharm

### Méthode 1: Clic Droit

1. **Tous les tests d'un fichier**
   - Clic droit sur `test_config_loader.py`
   - `Run 'pytest in test_config_loader.py'`

2. **Une classe de tests**
   - Clic droit sur `class TestConfigLoader:`
   - `Run 'pytest in TestConfigLoader'`

3. **Un seul test**
   - Clic droit sur `def test_load_tables_config():`
   - `Run 'pytest in test_load_tables_config'`

### Méthode 2: Icône Verte ▶️

Cliquez sur l'icône verte à gauche de :
- ✅ Fichier de test
- ✅ Classe de test
- ✅ Fonction de test

### Méthode 3: Raccourcis Clavier

```
Ctrl+Shift+F10  (Mac: Cmd+Shift+R)  - Exécuter le test sous le curseur
Shift+F10       (Mac: Ctrl+R)        - Ré-exécuter le dernier test
```

### Méthode 4: Configuration Run/Debug

```
Run → Edit Configurations → + → Python tests → pytest
→ Target: Custom
→ Additional Arguments: tests/unit/
→ Apply → Run
```

## 🐛 Debug des Tests

### Avec Points d'Arrêt

1. Cliquez dans la marge pour ajouter un breakpoint (point rouge)
2. Clic droit sur le test → `Debug 'pytest in ...'`
3. Le debugger s'arrêtera au breakpoint

### Console de Debug

```python
# Dans votre test, ajoutez:
import pdb; pdb.set_trace()  # Breakpoint programmatique

# Ou utilisez les commandes PyCharm
```

### Variables et Stack

Pendant le debug :
- **Variables** : Panel en bas à gauche
- **Call Stack** : Panel en bas
- **Evaluate Expression** : Alt+F8 (Mac: Option+F8)

## 📊 Coverage dans PyCharm

### Option 1: Via Interface

```
Run → Run 'pytest in ...' with Coverage
```

Ou clic droit sur test → `Run with Coverage`

### Option 2: Via Terminal

```bash
# Terminal PyCharm
pytest --cov=src --cov-report=html

# Ouvrir le rapport
open htmlcov/index.html  # Mac
start htmlcov/index.html # Windows
```

### Voir le Coverage

Après avoir run avec coverage :
- Lignes vertes = couvertes
- Lignes rouges = non couvertes
- Panel en bas : % par fichier

## 🎯 Résolution des Problèmes

### Problème 1: "unrecognized arguments --cov"

**Solution** : Le fichier `pytest.ini` est déjà corrigé. Si le problème persiste :

1. Supprimez les options de coverage de `pytest.ini`:
   ```ini
   # NE PAS mettre --cov dans addopts pour PyCharm
   addopts = -v --tb=short
   ```

2. Utilisez coverage via terminal :
   ```bash
   pytest --cov=src --cov-report=html
   ```

### Problème 2: "Module not found: src"

**Solution** : Ajouter le projet au PYTHONPATH

**Option A**: Via PyCharm
```
Preferences → Project Structure
→ Clic droit sur le dossier racine → Mark as → Sources Root
```

**Option B**: Via conftest.py
```python
# Ajouter au début de tests/conftest.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
```

**Option C**: Installer en mode dev
```bash
pip install -e .
```

### Problème 3: "pytest-cov not installed"

```bash
pip install pytest-cov
```

### Problème 4: Tests ne se découvrent pas

**Vérifier** :
1. Le test runner est bien `pytest` (pas `unittest`)
2. Les fichiers commencent par `test_`
3. Les fonctions commencent par `test_`
4. Les classes commencent par `Test`

**Recharger** :
```
Tools → Python Integrated Tools → Testing
→ Cocher/décocher pytest → Apply
```

### Problème 5: "Fixture not found"

**Vérifier** :
- `conftest.py` est dans `tests/`
- Le fichier est nommé exactement `conftest.py`
- Pas d'erreurs de syntaxe dans conftest.py

**Tester** :
```bash
# Terminal
pytest --collect-only  # Liste tous les tests
```

### Problème 6: PySpark errors

**Solution** : Installer Java

```bash
# Mac
brew install openjdk@11

# Ou utiliser les mocks sans vraiment lancer Spark
```

## 💡 Astuces PyCharm

### 1. Exécuter Tous les Tests

```
Clic droit sur dossier tests/ → Run 'pytest in tests'
```

### 2. Ré-exécuter les Tests Échoués

```
Run → Rerun Failed Tests
```

### 3. Voir l'Historique des Tests

```
Panel "Run" en bas → Onglet "History"
```

### 4. Tests Paramétrés

PyCharm affiche chaque paramètre comme un test séparé :
```python
@pytest.mark.parametrize("input,expected", [
    ("hello", "HELLO"),
    ("world", "WORLD"),
])
def test_upper(input, expected):
    assert input.upper() == expected
```

### 5. Marqueurs (Markers)

Exécuter seulement certains tests :

**Via Run Configuration** :
```
Additional Arguments: -m unit
```

**Via Terminal** :
```bash
pytest -m unit        # Seulement tests unitaires
pytest -m "not slow"  # Exclure tests lents
```

### 6. Auto-reload

Activer le watch mode :
```bash
# Terminal
pip install pytest-watch
ptw  # Ré-exécute les tests à chaque changement
```

## 🎨 Configuration Avancée

### Run Configuration Template

Créer une config réutilisable :

```
Run → Edit Configurations → Edit Configuration Templates
→ Python tests → pytest

Name: Unit Tests
Target: Custom
Additional Arguments: tests/unit/ -v
Working directory: $ProjectFileDir$

Apply → OK
```

### Raccourcis Personnalisés

```
Preferences → Keymap → Search "pytest"
→ Assigner vos raccourcis préférés
```

### Live Templates

Créer un snippet pour nouveaux tests :

```
Preferences → Editor → Live Templates → Python
→ + → Live Template

Abbreviation: test
Description: Create pytest test
Template text:
@pytest.mark.unit
def test_$NAME$():
    """Test $DESCRIPTION$."""
    $END$
```

Utilisation : Tapez `test` puis Tab

## 📋 Checklist PyCharm

Avant de commencer :
- [ ] Python 3.9+ installé
- [ ] Virtual environment créé
- [ ] `pip install -r requirements-test.txt`
- [ ] Test runner = pytest
- [ ] Project structure correcte

Pour exécuter :
- [ ] Clic droit sur test → Run
- [ ] Ou icône verte ▶️
- [ ] Ou Ctrl+Shift+F10

Pour debug :
- [ ] Ajouter breakpoint (clic dans marge)
- [ ] Clic droit → Debug
- [ ] Inspecter variables dans panel

Pour coverage :
- [ ] Run with Coverage
- [ ] Ou terminal : `pytest --cov=src --cov-report=html`

## 🔧 pytest.ini Compatible

Le fichier `pytest.ini` est déjà configuré pour PyCharm :

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*

markers =
    unit: Unit tests
    integration: Integration tests
    slow: Slow tests

addopts =
    -v
    --tb=short
    --strict-markers

# Coverage via command line: pytest --cov=src
```

## 📞 Support

Si problème persiste :
1. Vérifiez les logs PyCharm : `Help → Show Log`
2. Invalidez les caches : `File → Invalidate Caches → Invalidate and Restart`
3. Consultez TESTING.md pour plus de détails

---

**Ready to test in PyCharm!** 🚀

**Raccourci rapide** : Clic droit sur `tests/unit/` → `Run 'pytest in unit'`
