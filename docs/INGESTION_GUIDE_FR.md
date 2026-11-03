# Guide utilisateur : ingestion JDBC, MongoDB et Kafka

Ce guide explique comment configurer et lancer les ingestions du projet. Les
fichiers d'exemple dans `config/domains/` sont des modèles : remplacez les
valeurs d'exemple par celles de votre environnement et ne mettez jamais de
mot de passe, token ou URI avec identifiants dans Git.

## 1. Choisir le bon parcours

| Source | Configuration principale | Job / notebook | Destination |
|---|---|---|---|
| Base relationnelle via JDBC | `connector_config.yml` + `tables_config.yml` | `jdbc_ingestion` / `main_ingestion.py` | ADLS Parquet, Unity Catalog Delta, ou les deux |
| MongoDB | `connector_config.yml` + `tables_config.yml` | `jdbc_ingestion` / `main_ingestion.py` | ADLS Parquet, Unity Catalog Delta, ou les deux |
| Kafka (batch journalier) | `connector_config.yml` + `kafka_config.yml` | `kafka_daily_ingestion` / `main_kafka_ingestion.py` | Unity Catalog Delta |

Chaque source est configurée dans son propre dossier :

```text
config/domains/<nom_du_domaine>/
```

Le nom doit être identique dans le champ `domain` de chaque fichier YAML et
dans la liste de domaines du job correspondant dans `databricks.yml`.

## 2. JDBC — base relationnelle

### Périmètre

Le connecteur JDBC fourni est actuellement configuré pour SQL Server : il
construit une URL `jdbc:sqlserver://...` et utilise le driver SQL Server par
défaut. Le fait que Spark lise via JDBC ne signifie donc pas que PostgreSQL,
Oracle ou MySQL sont directement pris en charge sans adaptation du connecteur,
de l'URL, du driver et de l'authentification.

### Fichiers à configurer

`connector_config.yml` contient le type de source, l'authentification et les
options JDBC :

```yaml
domain: commandes

connector:
  type: "sqlserver"
  auth_type: "sql"
  secret_scope: "sqlserver-commandes"
  secret_keys:
    hostname: "hostname"
    port: "port"
    database: "database"
    username: "username"
    password: "password"
  jdbc_options:
    driver: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    encrypt: "true"
    trustServerCertificate: "false"
    loginTimeout: "30"
    fetchsize: "10000"
```

Les credentials vont dans un Databricks Secret Scope. Le YAML ne contient que
les noms des clés :

```bash
databricks secrets create-scope --scope sqlserver-commandes
databricks secrets put --scope sqlserver-commandes --key hostname --string-value "<serveur>"
databricks secrets put --scope sqlserver-commandes --key port --string-value "1433"
databricks secrets put --scope sqlserver-commandes --key database --string-value "<base>"
databricks secrets put --scope sqlserver-commandes --key username --string-value "<utilisateur>"
databricks secrets put --scope sqlserver-commandes --key password --string-value "<mot-de-passe>"
```

`tables_config.yml` déclare les tables sources, la table cible et le mode de
chargement :

```yaml
domain: commandes

tables:
  - table_name: "dbo.Orders"
    target_table: "orders"
    enabled: true
    load_mode: "incremental"
    incremental_column: "LastModified"
    incremental_type: "timestamp"
    merge_key: "OrderID"
    columns:
      - "OrderID"
      - "LastModified"
      - "CustomerID"
      - "Amount"
    write_mode: "unity_catalog"
    partition_by:
      - "order_year"
    derived_columns:
      order_year: "year(LastModified)"
    read_options:
      numPartitions: 4
      fetchsize: 10000
```

Modes et options principaux :

| Option | Comportement |
|---|---|
| `load_mode: full` | Relit toute la source et remplace la cible |
| `load_mode: incremental` sans `merge_key` | Lit les lignes après le watermark et ajoute les lignes à Delta |
| `load_mode: incremental` avec `merge_key` | Met à jour les clés existantes et insère les nouvelles lignes via Delta `MERGE` |
| `incremental_column` | Colonne source utilisée pour identifier les données nouvelles |
| `write_mode: unity_catalog` | Écrit dans `${catalog_name}.${bronze_schema}.${target_table}` |
| `write_mode: adls` | Écrit du Parquet sous le chemin ADLS du domaine et `adls_path` |
| `write_mode: both` | Écrit vers ADLS et Unity Catalog |
| `columns` | Projection source facultative ; la colonne incrémentale et les clés de merge sont ajoutées si nécessaires |
| `partition_by` | Colonnes présentes dans le DataFrame utilisées pour partitionner l'écriture |

Pour activer un domaine JDBC, ajoutez-le à `domains` dans le target voulu de
`databricks.yml`. Chaque entrée fournit `name` et `adls_base_path`, même si
l'écriture choisie est uniquement Unity Catalog :

```yaml
targets:
  dev:
    variables:
      domains:
        - name: "commandes"
          adls_base_path: "abfss://bronze@<storage>.dfs.core.windows.net"
```

Le job `jdbc_ingestion` lance le notebook une fois par domaine configuré. Le
notebook prend en charge SQL Server et MongoDB selon `connector.type`.

## 3. MongoDB

### Configurer la connexion

Dans `connector_config.yml`, le nom de la base et la collection par défaut ne
sont pas des secrets. L'URI complète, elle, est stockée dans Databricks Secrets :

```yaml
domain: mongo_app

connector:
  type: "mongodb"
  database: "application"
  collection: "events"
  secret_scope: "mongodb-app"
  uri_secret_key: "connection_uri"
```

Créer le scope et y déposer l'URI depuis un terminal sécurisé :

```bash
databricks secrets create-scope --scope mongodb-app
databricks secrets put --scope mongodb-app --key connection_uri --string-value "<URI MongoDB>"
```

L'URI peut inclure des caractères réservés dans le mot de passe ; encodez-les
selon la syntaxe MongoDB. Ne collez pas l'URI réelle dans un fichier de config,
une commande conservée dans un script ou un ticket.

### Déclarer les collections

Dans `tables_config.yml`, `table_name` est le nom logique utilisé dans les
résultats. Utilisez `collection` pour sélectionner le nom réel dans MongoDB.
Cette valeur par table remplace `connector.collection` :

```yaml
domain: mongo_app

tables:
  - table_name: "customer_events"
    collection: "events"
    target_table: "customer_events"
    enabled: true
    load_mode: "full"
    write_mode: "unity_catalog"
    filter_query: '{"status": "active"}'
    read_options:
      aggregation_pipeline:
        - $project:
            _id: 0
            customer_id: 1
            status: 1
            updated_at: 1

  - table_name: "customer_profiles"
    collection: "profiles"
    target_table: "customer_profiles"
    enabled: true
    load_mode: "full"
    write_mode: "unity_catalog"
```

`filter_query` doit être un objet JSON valide ; il est ajouté en tête du
pipeline sous forme de `$match`. `read_options.aggregation_pipeline` est une
liste d'étapes d'agrégation MongoDB.

Le chargement incrémental utilise `incremental_column` avec une colonne de
watermark et, facultativement, `merge_key` pour faire un upsert vers Unity
Catalog. Vérifiez que le champ est présent dans la collection et qu'il a un type
cohérent entre la source et la cible. Faites un premier essai contrôlé avant de
l'activer sur une grande collection.

Pour lancer MongoDB, ajoutez `mongo_app` à la variable `domains` du target dans
`databricks.yml`, avec son `adls_base_path`, puis déployez et lancez
`jdbc_ingestion`. Le job installe le MongoDB Spark Connector 10.4.1 (Spark 3.5,
Scala 2.12).

## 4. Kafka — ingestion journalière des événements de la veille

Ce parcours est un **batch quotidien**, pas un flux streaming continu. Il lit
les offsets conservés par Kafka, parse le JSON, sélectionne la date d'événement
et remplace cette date dans une table Delta.

### Connexion et secrets

`connector_config.yml` définit les brokers et les options Kafka. Les options
sensibles sont référencées par nom de clé dans un Secret Scope :

```yaml
domain: kafka_app

connector:
  type: "kafka"
  bootstrap_servers: "broker1.example.com:9093,broker2.example.com:9093"
  secret_scope: "kafka-app"
  options:
    kafka.security.protocol: "SASL_SSL"
    kafka.sasl.mechanism: "PLAIN"
  secret_options:
    kafka.sasl.jaas.config: "sasl_jaas_config"
```

Créer le secret contenant la configuration JAAS complète :

```bash
databricks secrets create-scope --scope kafka-app
databricks secrets put --scope kafka-app --key sasl_jaas_config --string-value "<configuration JAAS>"
```

Les noms des options Kafka doivent commencer par `kafka.`. Configurez les
brokers avec `bootstrap_servers`, pas dans `options`.

### Déclarer les topics et le schéma

`kafka_config.yml` déclare un schéma Spark pour chaque payload JSON. Le champ
`event_timestamp_field` doit être un champ de premier niveau de type
`timestamp` :

```yaml
domain: kafka_app
timezone: "Europe/Paris"

topics:
  - topic: "orders.events"
    target_table: "order_events"
    enabled: true
    event_timestamp_field: "event_timestamp"
    schema:
      type: "struct"
      fields:
        - name: "event_id"
          type: "string"
          nullable: true
          metadata: {}
        - name: "event_timestamp"
          type: "timestamp"
          nullable: true
          metadata: {}
        - name: "event_type"
          type: "string"
          nullable: true
          metadata: {}
        - name: "amount"
          type: "double"
          nullable: true
          metadata: {}
    source_options:
      minPartitions: "4"

global_options:
  error_handling:
    on_error: "fail"
```

Les clés JSON absentes produisent des valeurs nulles selon le schéma. Les
messages mal formés font échouer le topic ; un timestamp métier nul est remplacé
par le timestamp Kafka si celui-ci est disponible. Si les deux timestamps sont
absents, le traitement échoue au lieu d'écrire l'événement dans une mauvaise
date.

### Date, idempotence et rétention Kafka

- Sans paramètre, la date traitée est hier selon `timezone`.
- Pour un backfill, passez `run_date` au format `YYYY-MM-DD`.
- La lecture couvre les offsets `earliest` à `latest` actuellement conservés
  dans le topic ; chaque exécution rescannera donc la rétention disponible.
- Une livraison tardive ne peut être incluse que si son message est encore
  conservé dans Kafka au moment du run.
- La table cible est `${catalog_name}.${bronze_schema}.${target_table}`. La
  cible existante doit être une table Delta dont le schéma est compatible.
- Le rerun remplace atomiquement les données de la date demandée avec
  `replaceWhere`. Les autres dates ne sont pas remplacées.
- Le job est livré en pause et `kafka_domains` est vide par défaut. Ajoutez le
  domaine aux variables du target dans `databricks.yml` et ne dé-pauser le job
  qu'après avoir renseigné brokers, secret, catalog/schema et rétention requise.

Exemple d'activation :

```yaml
targets:
  dev:
    variables:
      kafka_domains:
        - name: "kafka_app"
```

Déploiement et lancement manuel :

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev kafka_daily_ingestion
```

Le notebook accepte aussi `run_date` comme paramètre de job lors d'un backfill.
Le connecteur Spark Kafka est installé par le job (Spark 3.5, Scala 2.12).

## 5. Déployer et vérifier

Avant le premier run, remplacez les hôtes Databricks, catalog/schema, chemins
ADLS, notifications et paramètres d'exemple dans `databricks.yml` et les
ressources. Les identités de calcul doivent avoir les droits réseau sur les
sources et les permissions d'accès aux secrets, au stockage et aux tables Unity
Catalog.

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev

# JDBC ou MongoDB
databricks bundle run -t dev jdbc_ingestion

# Kafka
databricks bundle run -t dev kafka_daily_ingestion
```

Vérifiez les logs du run, les nombres de lignes et le schéma des tables cibles.
Pour `write_mode: adls` ou `both`, vérifiez également le chemin calculé à partir
de `adls_base_path` et `adls_path`. Pour Kafka, contrôlez spécifiquement la date
de l'événement, les timestamps manquants, la rétention du topic et le résultat
d'un rerun sur une date déjà chargée.

## 6. Recommandations de mise en production

1. Commencez par un petit périmètre : une table/collection ou un topic, un
   target de développement et une date Kafka limitée.
2. Privilégiez `incremental` pour les sources volumineuses quand le watermark
   est fiable ; choisissez `merge_key` si la source peut renvoyer des lignes
   déjà ingérées ou modifiées.
3. Configurez des partitions seulement quand le volume et les colonnes s'y
   prêtent. Évitez les partitions Delta trop fines ou à forte cardinalité.
4. Considérez ces sorties comme la couche Bronze (données proches de la source).
   Appliquez les règles métier, les contrôles de qualité, la déduplication et
   les contrats de schéma dans des traitements Silver séparés ; ne transformez
   pas silencieusement les données à l'ingestion.
5. Pour Kafka, alignez la fréquence des runs et les exigences de backfill avec
   la rétention effective du topic. Cette lecture par offsets retenus n'est pas
   équivalente à une ingestion continue avec checkpoint.
6. Surveillez la durée, le volume, les erreurs, les watermarks et les retards de
   données. Testez les changements de schéma et les reruns avant production.
