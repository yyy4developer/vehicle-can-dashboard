# Vehicle CAN Data Dashboard

> 車載CANデータのリアルタイム可視化・分析ダッシュボード  
> Built with [apx](https://github.com/databricks-solutions/apx) on Databricks

## 🚗 概要

このアプリは車両のCANバスデータを収集、処理、可視化するフルスタックアプリケーションです。

### 主な機能

- **リアルタイム信号モニタリング**: 速度、RPM、スロットル、ブレーキ、ステアリングの時系列表示
- **イベント検出**: 急ブレーキ、急加速、急ハンドルの自動検出
- **CAN品質メトリクス**: メッセージ欠落率・通信健全性の監視
- **ダッシュカメラ動画再生**: 4カメラ（前後左右）の同期再生

## 🛠️ Tech Stack

| Layer | Technology |
|-------|------------|
| **Backend** | Python + FastAPI |
| **Frontend** | React + Vite + shadcn/ui |
| **Data Pipeline** | DLT (Delta Live Tables) |
| **Data Storage** | Unity Catalog |
| **API Client** | [orval](https://orval.dev/) による自動生成 |
| **Infrastructure** | Databricks Apps |

## 📁 プロジェクト構成

```
├── notebooks/              # データパイプライン (Databricks Notebooks)
│   ├── 00_setup.py         # スキーマ・Volume作成
│   ├── 01_data_generator.py # CANデータ生成
│   ├── 02_generate_dbc.py  # DBCファイル生成
│   ├── 03_download_videos.py # サンプル動画ダウンロード
│   ├── 04_vehicle_dlt_*.sql/py # DLTパイプライン
│   └── 05_grant_app_permissions.py # App権限付与
├── src/yao_demo_vehicle_app/
│   ├── backend/           # FastAPI バックエンド
│   │   ├── app.py         # アプリケーションエントリ
│   │   ├── router.py      # APIルート定義
│   │   ├── models.py      # Pydanticモデル
│   │   └── runtime.py     # Databricks連携
│   └── ui/                # React フロントエンド
│       ├── routes/        # ページコンポーネント
│       ├── components/    # UIコンポーネント
│       └── lib/           # API・ユーティリティ
├── databricks.yml         # DAB設定
├── app.yml               # Databricks Apps設定
└── pyproject.toml        # Python依存関係
```

## 🚀 Quick Start

### 開発環境の起動

```bash
# 全サーバー起動 (backend, frontend, OpenAPI watcher)
uv run apx dev start

# ステータス確認
uv run apx dev status

# ログ確認
uv run apx dev logs -f

# 停止
uv run apx dev stop
```

### データパイプラインの実行

```bash
# Databricks にデプロイ
databricks bundle deploy -p <your-profile>

# フルパイプライン実行 (データ生成 → DLT → 権限付与)
databricks bundle run full-pipeline -p <your-profile>
```

### 個別ジョブの実行

```bash
# スキーマ・Volumeのセットアップ
databricks bundle run setup

# データ生成のみ
databricks bundle run data-generation

# App権限付与
databricks bundle run grant-app-permissions
```

## ✅ コード品質チェック

```bash
# TypeScript + Python の型チェック
uv run apx dev check
```

## 📦 ビルド

```bash
uv run apx build
```

## 🚢 デプロイ

```bash
# Databricks にデプロイ
databricks bundle deploy -p <your-profile>
```

## 📊 データフロー

```
[CANデータ生成] → [Volume (raw)] 
                      ↓
               [DLT Pipeline]
                      ↓
    ┌─────────────────┼─────────────────┐
    ↓                 ↓                 ↓
[bronze_can_frames] [silver_can_signals] [gold_*テーブル]
                                               ↓
                                     [FastAPI Backend]
                                               ↓
                                      [React Dashboard]
```

## 📋 DLTテーブル

| Layer | Table | Description |
|-------|-------|-------------|
| Bronze | `bronze_can_frames` | 生CANフレーム |
| Silver | `silver_can_signals` | デコード済みシグナル |
| Silver | `silver_can_quality` | 通信品質メトリクス |
| Gold | `gold_signals_aggregated` | 100ms集計シグナル |
| Gold | `gold_event_history` | 検出イベント履歴 |
| Gold | `gold_vehicle_stats` | 車両統計 |
| Gold | `gold_latest_signals` | 最新シグナル |

---

<p align="center">Built with ❤️ using <a href="https://github.com/databricks-solutions/apx">apx</a></p>
