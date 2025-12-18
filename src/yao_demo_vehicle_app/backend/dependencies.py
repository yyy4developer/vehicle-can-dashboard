import os
from databricks.sdk import WorkspaceClient
from fastapi import Header
from typing import Annotated, Generator
from sqlmodel import Session
from .runtime import rt
from .logger import logger


def get_obo_ws(
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> WorkspaceClient:
    """
    Returns a Databricks Workspace client with On-Behalf-Of user authentication.
    
    NOTE: This is only used for user-specific operations like getting current user info.
    For data access (SQL queries, file access), use the Service Principal client (rt.ws) instead.
    
    The Service Principal has the necessary permissions:
    - CAN_USE on SQL Warehouse (for SQL queries)
    - READ_VOLUME on Unity Catalog Volumes (for file access)
    
    Example usage (for user info only):
    @api.get("/current-user")
    def me(obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]):
        return obo_ws.current_user.me()
    """

    if not token:
        raise ValueError(
            "OBO token is not provided in the header X-Forwarded-Access-Token"
        )

    # Get host from environment or runtime workspace client
    host = os.environ.get("DATABRICKS_HOST")
    if not host:
        try:
            host = rt.ws.config.host
        except Exception as e:
            logger.warning(f"Failed to get host from rt.ws: {e}")
            # Fallback to default host for demo
            host = "https://e2-demo-field-eng.cloud.databricks.com"
    
    logger.debug(f"Creating OBO WorkspaceClient with host={host}")
    
    return WorkspaceClient(
        host=host,
        token=token, 
        auth_type="pat"
    )  # set pat explicitly to avoid issues with SP client


def get_session() -> Generator[Session, None, None]:
    """
    Returns a SQLModel session.
    """
    with rt.get_session() as session:
        yield session
