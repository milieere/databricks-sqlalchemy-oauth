import logging
from pydantic import BaseModel, HttpUrl
from typing import Optional
from databricks.sdk.oauth import (
    Token,
    Refreshable,
)
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine

logger = logging.getLogger("databricks_sqlalchemy_oauth")


class DbConfig(BaseModel):
    hostname: str
    http_path: str
    db: Optional[str]

class ConnectionBuilder:
    def __init__(self, credential_provider: Refreshable, db_config: DbConfig):
        self.db_config = db_config
        self.token: Optional[Token] = None
        self.engine: Optional[Engine] = None
        self.session: Optional[Session] = None
        self.credential_provider = credential_provider

    def _get_access_token(self) -> Token:
        """
        Calls function token() on credential_provider Refreshable instance
        - for M2M OAuth the Refreshable instance will be of ClientCredentials type,
        - for U2M OAuth, SessionCredentials should be used
        Check the databricks.sdk.oauth module for more info

        The token() function checks if the token is expired and if needed refreshes it.
        If there is token present already, and is not expired, just return the current token.

        Returns:
            Token: token class with token string, information about expiration, etc.
        """
        if self.token is None or self.token.expired:
            logger.debug("Obtaining new OAuth token")
            self.token = self.credential_provider.token()
        return self.token

    def _construct_conn_string(self) -> str:
        """Put together connection string with valid OAuth token

        Returns:
            str: SQLAlchemy Connection string for SQL warehouse
        """
        token = self._get_access_token().access_token
        conn_string = f"databricks://token:{token}@{self.db_config.hostname}/?http_path={self.db_config.http_path}"
        if self.db_config.db:
            conn_string += f"&catalog={self.db_config.db}"
        return conn_string

    def _ensure_engine(self) -> None:
        """Helper method to ensure the engine is created or refreshed.
        If there's no token, token is invalid, or engine doesn't exist,
        create engine and set it.
        """
        if self.token is None or self.token.expired or self.engine is None:
            logger.debug("Refreshing engine with new credentials")
            conn_string = self._construct_conn_string()
            self.engine = create_engine(conn_string, echo=True, pool_recycle=3600)

    def get_engine(self) -> Engine:
        """Creates engine with fresh credentials or returns current engine

        Returns:
            Engine: SQLAlchemy engine with fresh OAuth access token
        """
        self._ensure_engine()
        return self.engine

    def get_session(self) -> Session:
        """Creates a SQLAlchemy session using engine with fresh credentials

        Returns:
            Session: SQLAlchemy session
        """
        self._ensure_engine()
        if self.session is None:
            session_factory = sessionmaker(bind=self.engine)
            self.session = session_factory()
        return self.session
