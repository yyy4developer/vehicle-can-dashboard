#!/usr/bin/env python3
"""
Databricks Connect テストスクリプト

このスクリプトは以下のテストを実行します:
1. Databricks WorkspaceClient への接続テスト
2. データベースインスタンスの存在確認
3. データベース接続テスト
4. 基本的なクエリの実行テスト
"""

import sys
import yaml
from pathlib import Path
from typing import Dict, Optional

# プロジェクトのルートをパスに追加
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from sqlalchemy import create_engine, text, event
from sqlmodel import Session
from yao_demo_vehicle_app.backend.config import conf


def load_databricks_config() -> Dict[str, Optional[str]]:
    """
    databricks.yml から設定を読み込みます。
    
    Returns:
        データベース設定の辞書 (instance_name, database_name)
    """
    databricks_yml = project_root / "databricks.yml"
    if not databricks_yml.exists():
        return {}
    
    try:
        with open(databricks_yml, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        
        # databricks.yml の構造から設定を抽出
        apps = config.get("resources", {}).get("apps", {})
        for app_key, app_config in apps.items():
            resources = app_config.get("resources", [])
            for resource in resources:
                if resource.get("name") == "db":
                    db_config = resource.get("database", {})
                    return {
                        "instance_name": db_config.get("instance_name"),
                        "database_name": db_config.get("database_name"),
                    }
    except Exception as e:
        print(f"⚠️  databricks.yml の読み込みに失敗: {str(e)}")
    
    return {}


def test_workspace_client() -> tuple[bool, WorkspaceClient | None, str]:
    """
    Databricks WorkspaceClient への接続をテストします。
    
    Returns:
        (success: bool, client: WorkspaceClient | None, message: str)
    """
    try:
        print("🔍 Databricks WorkspaceClient への接続をテスト中...")
        ws = WorkspaceClient()
        
        # 現在のユーザー情報を取得して接続を確認
        user = ws.current_user.me()
        print(f"✅ WorkspaceClient 接続成功")
        print(f"   ユーザー: {user.user_name}")
        print(f"   ホスト: {ws.config.host}")
        return True, ws, "接続成功"
    except Exception as e:
        error_msg = f"WorkspaceClient 接続失敗: {str(e)}"
        print(f"❌ {error_msg}")
        return False, None, error_msg


def list_database_instances(ws: WorkspaceClient) -> list[str]:
    """
    利用可能なデータベースインスタンスのリストを取得します。
    
    Args:
        ws: WorkspaceClient インスタンス
        
    Returns:
        インスタンス名のリスト
    """
    try:
        instances = list(ws.database.list_database_instances())
        return [instance.name for instance in instances if instance.name]
    except Exception as e:
        print(f"⚠️  インスタンス一覧の取得に失敗: {str(e)}")
        return []


def test_database_instance(ws: WorkspaceClient, instance_name: str) -> tuple[bool, str]:
    """
    データベースインスタンスの存在を確認します。
    
    Args:
        ws: WorkspaceClient インスタンス
        instance_name: データベースインスタンス名
        
    Returns:
        (success: bool, message: str)
    """
    try:
        print(f"\n🔍 データベースインスタンス '{instance_name}' の存在を確認中...")
        instance = ws.database.get_database_instance(instance_name)
        print(f"✅ データベースインスタンスが見つかりました")
        print(f"   インスタンス名: {instance.name}")
        print(f"   読み書きDNS: {instance.read_write_dns}")
        return True, "インスタンスが見つかりました"
    except NotFound:
        error_msg = f"データベースインスタンス '{instance_name}' が見つかりません"
        print(f"❌ {error_msg}")
        
        # 利用可能なインスタンスをリストアップ
        print(f"\n📋 利用可能なデータベースインスタンスを確認中...")
        available_instances = list_database_instances(ws)
        if available_instances:
            print(f"   利用可能なインスタンス ({len(available_instances)}件):")
            for inst_name in available_instances:
                marker = " ← 設定値" if inst_name == instance_name else ""
                print(f"     - {inst_name}{marker}")
        else:
            print(f"   ⚠️  利用可能なインスタンスが見つかりませんでした")
        
        return False, error_msg
    except Exception as e:
        error_msg = f"インスタンス確認エラー: {str(e)}"
        print(f"❌ {error_msg}")
        return False, error_msg


def test_database_connection(
    ws: WorkspaceClient,
    instance_name: str,
    database_name: str,
    port: int = 5432
) -> tuple[bool, str]:
    """
    データベースへの接続をテストします。
    
    Args:
        ws: WorkspaceClient インスタンス
        instance_name: データベースインスタンス名
        database_name: データベース名
        port: データベースポート
        
    Returns:
        (success: bool, message: str)
    """
    try:
        print(f"\n🔍 データベース '{database_name}' への接続をテスト中...")
        
        # インスタンス情報を取得
        instance = ws.database.get_database_instance(instance_name)
        host = instance.read_write_dns
        
        # ユーザー名を取得
        username = (
            ws.config.client_id
            if ws.config.client_id
            else ws.current_user.me().user_name
        )
        
        # エンジンURLを作成
        engine_url = f"postgresql+psycopg://{username}:@{host}:{port}/{database_name}"
        
        # 接続前に認証情報を設定するコールバック
        def before_connect(dialect, conn_rec, cargs, cparams):  # type: ignore
            cred = ws.database.generate_database_credential(
                instance_names=[instance_name]
            )
            cparams["password"] = cred.token
        
        # エンジンを作成
        engine = create_engine(
            engine_url,
            pool_recycle=45 * 60,
            connect_args={"sslmode": "require"},
        )
        
        # 接続イベントリスナーを登録
        _ = event.listens_for(engine, "do_connect")(before_connect)
        
        # 接続をテスト
        with Session(engine) as session:
            result = session.connection().execute(text("SELECT version(), current_database(), current_user"))
            row = result.fetchone()
            
            if row is None:
                raise ValueError("クエリ結果が空です")
            
            print(f"✅ データベース接続成功")
            print(f"   データベース: {row[1]}")
            print(f"   ユーザー: {row[2]}")
            version_str = str(row[0]) if row[0] else "不明"
            print(f"   PostgreSQL バージョン: {version_str.split(',')[0]}")
            
            # 追加のテストクエリ
            print(f"\n📊 追加のテストクエリを実行中...")
            
            # 現在時刻を取得
            result = session.connection().execute(text("SELECT NOW()"))
            current_time_row = result.fetchone()
            if current_time_row:
                current_time = current_time_row[0]
                print(f"   現在時刻: {current_time}")
            
            # データベースサイズを取得
            result = session.connection().execute(
                text("SELECT pg_size_pretty(pg_database_size(current_database()))")
            )
            db_size_row = result.fetchone()
            if db_size_row:
                db_size = db_size_row[0]
                print(f"   データベースサイズ: {db_size}")
            
            # テーブル一覧を取得
            result = session.connection().execute(
                text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    ORDER BY table_name
                """)
            )
            tables = [row[0] for row in result.fetchall() if row and row[0]]
            print(f"   テーブル数: {len(tables)}")
            if tables:
                print(f"   テーブル一覧: {', '.join(tables[:10])}")
                if len(tables) > 10:
                    print(f"   ... 他 {len(tables) - 10} テーブル")
            
            session.close()
        
        return True, "データベース接続成功"
        
    except Exception as e:
        error_msg = f"データベース接続エラー: {str(e)}"
        print(f"❌ {error_msg}")
        import traceback
        print(f"\n詳細なエラー情報:")
        traceback.print_exc()
        return False, error_msg


def main():
    """メイン関数"""
    print("=" * 60)
    print("Databricks Connect テストスクリプト")
    print("=" * 60)
    
    # databricks.yml から設定を読み込む
    databricks_config = load_databricks_config()
    
    # 設定の優先順位: databricks.yml > 環境変数/conf > デフォルト
    try:
        if hasattr(conf, 'db') and conf.db:
            conf_instance_name: Optional[str] = conf.db.instance_name
            conf_database_name: Optional[str] = conf.db.database_name
            conf_port: int = conf.db.port
        else:
            conf_instance_name = None
            conf_database_name = None
            conf_port = 5432
    except Exception:
        conf_instance_name = None
        conf_database_name = None
        conf_port = 5432
    
    instance_name: Optional[str] = (
        databricks_config.get("instance_name")
        or conf_instance_name
        or None
    )
    
    database_name: str = (
        databricks_config.get("database_name")
        or conf_database_name
        or "databricks_postgres"
    )
    
    port: int = conf_port
    
    # 設定を確認
    if not instance_name:
        print("❌ エラー: データベースインスタンス名が設定されていません")
        print("   databricks.yml または環境変数で設定してください")
        print(f"\n📋 databricks.yml から読み込まれた設定:")
        print(f"   instance_name: {databricks_config.get('instance_name', '未設定')}")
        print(f"   database_name: {databricks_config.get('database_name', '未設定')}")
        sys.exit(1)
    
    print(f"\n📋 設定情報:")
    print(f"   インスタンス名: {instance_name}")
    print(f"   データベース名: {database_name}")
    print(f"   ポート: {port}")
    
    # テスト1: WorkspaceClient 接続
    success, ws, msg = test_workspace_client()
    if not success or ws is None:
        print(f"\n❌ テスト失敗: {msg}")
        sys.exit(1)
    
    # テスト2: データベースインスタンス確認
    success, msg = test_database_instance(ws, instance_name)
    if not success:
        print(f"\n❌ テスト失敗: {msg}")
        sys.exit(1)
    
    # テスト3: データベース接続
    success, msg = test_database_connection(ws, instance_name, database_name, port)
    if not success:
        print(f"\n❌ テスト失敗: {msg}")
        sys.exit(1)
    
    # すべてのテストが成功
    print("\n" + "=" * 60)
    print("✅ すべてのテストが成功しました！")
    print("=" * 60)


if __name__ == "__main__":
    main()

