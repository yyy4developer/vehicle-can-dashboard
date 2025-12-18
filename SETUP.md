# セットアップガイド

このドキュメントでは、プロジェクトをDatabricks環境にデプロイするための設定手順を説明します。

## 前提条件

- Databricks ワークスペースへのアクセス
- Unity Catalog が有効化されていること
- Databricks CLI がインストール済み (`databricks` コマンドが使用可能)
- `uv` (Python パッケージマネージャー) がインストール済み

## Step 1: Databricks CLI の設定

```bash
# プロファイルの設定（初回のみ）
databricks configure --profile DEFAULT

# 接続確認
databricks auth profiles
```

## Step 2: 必要な情報の収集

以下の情報を Databricks ワークスペースから取得してください。

### 2.1 ワークスペースホスト

Databricks ワークスペースの URL です。

```
例: https://your-workspace.cloud.databricks.com
```

**取得方法**: ブラウザで Databricks にログインし、URL をコピー

### 2.2 Unity Catalog 名

データを保存する Unity Catalog の名前です。

```
例: main, my_catalog
```

**取得方法**: 
1. Databricks UI → **Catalog** をクリック
2. 左パネルから使用するカタログ名を確認

### 2.3 クラスター ID

ノートブックを実行するためのクラスター ID です。

```
例: 0123-456789-abcdefgh
```

**取得方法**:
1. Databricks UI → **Compute** をクリック
2. 使用するクラスターを選択
3. URL の最後の部分がクラスター ID
   - URL: `.../compute/clusters/0123-456789-abcdefgh/configuration`
   - クラスター ID: `0123-456789-abcdefgh`

### 2.4 SQL Warehouse ID

SQL クエリを実行するための Warehouse ID です。

```
例: 1234567890abcdef
```

**取得方法**:
1. Databricks UI → **SQL Warehouses** をクリック
2. 使用する Warehouse を選択
3. **Connection details** タブを開く
4. **HTTP Path** の最後の部分が Warehouse ID
   - HTTP Path: `/sql/1.0/warehouses/1234567890abcdef`
   - Warehouse ID: `1234567890abcdef`

### 2.5 Lakebase データベース情報（オプション）

Lakebase を使用する場合のみ必要です。

- **Instance Name**: Lakebase インスタンス名
- **Database Name**: データベース名

**取得方法**:
1. Databricks UI → **Catalog** → **Lakebase** をクリック
2. 使用するインスタンスとデータベースの名前を確認

## Step 3: 設定ファイルの更新

### 3.1 `databricks.yml` の更新

以下のプレースホルダーを置き換えてください：

```yaml
# variables セクション
variables:
  catalog:
    default: "<your_catalog>"  # 例: "main"

# resources.apps セクション
resources:
  apps:
    yao-demo-vehicle-app-app:
      resources:
        - name: "db"
          database:
            database_name: "<your_database_name>"  # 例: "my-lakebase"
            instance_name: "<your_instance_name>"  # 例: "my-instance"

# targets セクション
targets:
  dev:
    workspace:
      host: https://<your-workspace>.cloud.databricks.com
    variables:
      cluster_id: "<your_cluster_id>"      # 例: "0123-456789-abcdefgh"
      warehouse_id: "<your_warehouse_id>"  # 例: "1234567890abcdef"

  prod:
    workspace:
      host: https://<your-workspace>.cloud.databricks.com
    variables:
      cluster_id: "<your_cluster_id>"
      warehouse_id: "<your_warehouse_id>"
```

### 3.2 `app.yml` の更新

```yaml
env:
  - name: DATABRICKS_HOST
    value: "https://<your-workspace>.cloud.databricks.com"
  - name: YAO_DEMO_VEHICLE_APP_UNITY__CATALOG
    value: <your_catalog>  # 例: main
  - name: YAO_DEMO_VEHICLE_APP_UNITY__WAREHOUSE_ID
    value: "<your_warehouse_id>"  # 例: 1234567890abcdef
  - name: VOLUME_PATH
    value: "/Volumes/<your_catalog>/yao_demo_vehicle_app/videos"
```

## Step 4: デプロイと実行

### 4.1 バンドルのデプロイ

```bash
# 開発環境にデプロイ
databricks bundle deploy -t dev

# または本番環境
databricks bundle deploy -t prod
```

### 4.2 データパイプラインの実行

```bash
# フルパイプライン実行（セットアップ → データ生成 → DLT → 権限付与）
databricks bundle run full-pipeline -t dev
```

### 4.3 個別ジョブの実行

```bash
# スキーマ・Volume のセットアップのみ
databricks bundle run setup -t dev

# データ生成のみ
databricks bundle run data-generation -t dev

# App 権限付与のみ
databricks bundle run grant-app-permissions -t dev
```

## Step 5: アプリの確認

デプロイ完了後、Databricks UI からアプリにアクセスできます。

1. Databricks UI → **Compute** → **Apps** をクリック
2. `yao-demo-vehicle-app` を選択
3. **Open App** をクリック

## トラブルシューティング

### データが表示されない場合

1. DLT パイプラインが正常に完了しているか確認
   ```bash
   databricks bundle run full-pipeline -t dev
   ```

2. App に権限が付与されているか確認
   ```bash
   databricks bundle run grant-app-permissions -t dev
   ```

### 権限エラーが発生する場合

- SQL Warehouse に `CAN_USE` 権限があることを確認
- Unity Catalog テーブルに `SELECT` 権限があることを確認
- Volume に `READ_VOLUME` 権限があることを確認

## 設定値の一覧

| 設定項目 | ファイル | 説明 |
|---------|---------|------|
| `<your-workspace>` | `databricks.yml`, `app.yml` | Databricks ワークスペースホスト |
| `<your_catalog>` | `databricks.yml`, `app.yml` | Unity Catalog 名 |
| `<your_cluster_id>` | `databricks.yml` | ノートブック実行用クラスター ID |
| `<your_warehouse_id>` | `databricks.yml`, `app.yml` | SQL Warehouse ID |
| `<your_database_name>` | `databricks.yml` | Lakebase データベース名 |
| `<your_instance_name>` | `databricks.yml` | Lakebase インスタンス名 |

