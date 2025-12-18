from functools import cached_property
from typing import Any
import os
from databricks.sdk.errors import NotFound
from databricks.sdk.service.sql import StatementState
from sqlalchemy import Engine
from .config import conf, AppConfig
from databricks.sdk import WorkspaceClient
from sqlmodel import SQLModel, Session, text
from sqlalchemy import create_engine, event
from .logger import logger


class Runtime:
    def __init__(self):
        self.config: AppConfig = conf

    @cached_property
    def ws(self) -> WorkspaceClient:
        """
        Returns a WorkspaceClient using Service Principal authentication when available.
        In Databricks Apps, DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET are automatically
        provided for the app's Service Principal.
        Falls back to default authentication (e.g., DATABRICKS_CONFIG_PROFILE) for local development.
        """
        client_id = os.environ.get("DATABRICKS_CLIENT_ID")
        client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
        host = os.environ.get("DATABRICKS_HOST")
        
        if client_id and client_secret and host:
            logger.info(f"Using Service Principal authentication (client_id={client_id[:8]}...)")
            return WorkspaceClient(
                host=host,
                client_id=client_id,
                client_secret=client_secret,
            )
        
        # Fallback for local development
        logger.info("Using default authentication (local dev mode)")
        return WorkspaceClient()

    @cached_property
    def engine_url(self) -> str:
        instance = self.ws.database.get_database_instance(self.config.db.instance_name)
        prefix = "postgresql+psycopg"
        host = instance.read_write_dns
        port = self.config.db.port
        database = self.config.db.database_name
        username = (
            self.ws.config.client_id
            if self.ws.config.client_id
            else self.ws.current_user.me().user_name
        )
        return f"{prefix}://{username}:@{host}:{port}/{database}"

    def _before_connect(self, dialect, conn_rec, cargs, cparams):
        cred = self.ws.database.generate_database_credential(
            instance_names=[self.config.db.instance_name]
        )
        cparams["password"] = cred.token

    @property
    def engine(self) -> Engine:
        engine = create_engine(
            self.engine_url,
            pool_recycle=45 * 60,
            connect_args={"sslmode": "require"},
            pool_size=4,
        )  # 45 minutes
        event.listens_for(engine, "do_connect")(self._before_connect)
        return engine

    def get_session(self) -> Session:
        return Session(self.engine)

    def validate_db(self) -> None:
        logger.info(
            f"Validating database connection to instance {self.config.db.instance_name}"
        )
        # check if the database instance exists
        try:
            self.ws.database.get_database_instance(self.config.db.instance_name)
        except NotFound:
            raise ValueError(
                f"Database instance {self.config.db.instance_name} does not exist"
            )

        # check if a connection to the database can be established
        try:
            with self.get_session() as session:
                session.connection().execute(text("SELECT 1"))
                session.close()

        except Exception as e:
            logger.error(f"Database connection failed: {type(e).__name__}: {str(e)}", exc_info=True)
            raise ConnectionError(f"Failed to connect to the database: {str(e)}")

        logger.info(
            f"Database connection to instance {self.config.db.instance_name} validated successfully"
        )

    def initialize_models(self) -> None:
        logger.info("Initializing database models")
        SQLModel.metadata.create_all(self.engine)
        logger.info("Database models initialized successfully")

    def execute_sql(
        self, 
        sql: str, 
        warehouse_id: str | None = None,
        ws: WorkspaceClient | None = None,
    ) -> list[dict[str, Any]]:
        """
        Execute SQL statement using Databricks Statement Execution API.
        Uses the Service Principal WorkspaceClient by default.
        Returns list of dictionaries (rows).
        """
        # Use provided ws or default to Service Principal client
        client = ws or self.ws
        
        wh_id = warehouse_id or self.config.unity.warehouse_id
        if not wh_id:
            raise ValueError("SQL Warehouse ID is not configured")
        
        catalog = self.config.unity.catalog
        schema = self.config.unity.schema_name
        
        logger.debug(f"Executing SQL: {sql[:200]}...")
        
        response = client.statement_execution.execute_statement(
            warehouse_id=wh_id,
            statement=sql,
            catalog=catalog,
            schema=schema,
            wait_timeout="30s",
        )
        
        if response.status is None:
            raise RuntimeError("SQL execution returned no status")
        
        if response.status.state == StatementState.FAILED:
            error_msg = response.status.error.message if response.status.error else "Unknown error"
            raise RuntimeError(f"SQL execution failed: {error_msg}")
        
        if response.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(f"SQL execution did not complete: {response.status.state}")
        
        # Parse results
        if not response.result or not response.result.data_array:
            return []
        
        if response.manifest is None or response.manifest.schema is None:
            return []
        
        columns = [col.name for col in response.manifest.schema.columns or []]
        rows = []
        for row_data in response.result.data_array:
            row = dict(zip(columns, row_data))
            rows.append(row)
        
        return rows


rt = Runtime()
