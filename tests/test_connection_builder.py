import unittest
from pydantic import ValidationError
from unittest.mock import MagicMock, patch, PropertyMock
from databricks_sqlalchemy_oauth.connection_builder import ConnectionBuilder, Token, DbConfig
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session

class DbConfigTests(unittest.TestCase):
    def test_valid_inputs(self):
        # Valid inputs should not raise any exceptions
        config = DbConfig(hostname="https://our-dev-workspace.cloud.databricks.com", http_path="/sql/1.0/warehouses/abcdefghijk", db=None)
        self.assertIsInstance(config, DbConfig)

    def test_invalid_inputs(self):
        # Invalid inputs should raise a ValidationError
        with self.assertRaises(ValidationError):
            # Missing required properties
            config = DbConfig(http_path="/sql/1.0/warehouses/abcdefghijk", db=None)

        with self.assertRaises(ValidationError):
            # Invalid hostname format
            config = DbConfig(hostname="invalid_url", http_path="/sql/1.0/warehouses/abcdefghijk", db=None)


class ConnectionBuilderTests(unittest.TestCase):
    def setUp(self):
        self.db_config = MagicMock()
        self.db_config.hostname = "hostname"
        self.db_config.http_path = "http_path"
        self.db_config.db = "db"

    def test_token_none(self):
        """
        Test that if token is None, it will be fetched.
        """
        credential_manager = MagicMock()
        connection_builder = ConnectionBuilder(credential_manager, self.db_config)
        connection_builder.token = None
        token_mock = MagicMock(access_token="dummy_token")
        self.assertIsNone(connection_builder.token)

        credential_manager.token.return_value = token_mock
        token = connection_builder._get_access_token()
        self.assertEqual(token, token_mock)

    def test_token_expired(self):
        """
        Test that when token is expired it will be refreshed.
        """
        credential_manager = MagicMock()

        # Create a token instance with a mock for the expired property
        expired_token = MagicMock(access_token="dummy_token")
        type(expired_token).expired = PropertyMock(return_value=True)

        connection_builder = ConnectionBuilder(credential_manager, self.db_config)
        connection_builder.token = expired_token
        self.assertTrue(connection_builder.token.expired)

        # Create a fresh token instance
        fresh_token = MagicMock(access_token="dummy_token")
        type(fresh_token).expired = PropertyMock(return_value=False)

        connection_builder.token = fresh_token
        token = connection_builder._get_access_token()
        self.assertFalse(token.expired)

    def test_token_valid_engine_present(self):
        """
        Test that when token is valid, and engine is present the _construct_conn_string is not called
        """
        credential_manager = MagicMock()
        valid_token = Token(access_token="dummy_token")
        type(valid_token).expired = PropertyMock(return_value=False)
        connection_builder = ConnectionBuilder(credential_manager, self.db_config)
        connection_builder.token = valid_token
        connection_builder.engine = MagicMock()

        with patch.object(
            connection_builder, "_construct_conn_string"
        ) as mock_construct_conn_string:
            connection_builder.get_engine()
            mock_construct_conn_string.assert_not_called()

    def test_get_token_error(self):
        """
        Test case with invalid configuration, when upstream module (Databricks SDK)
        returns ValueError when trying to fetch token.
        """
        credential_manager_invalid_config = MagicMock()
        connection_builder = ConnectionBuilder(
            credential_manager_invalid_config, self.db_config
        )

        with patch.object(connection_builder, "_get_access_token") as mock_get_token:
            example_error_message = "This message will different depending on Databricks SDK, this is just an example."
            mock_get_token.side_effect = ValueError(example_error_message)

            with self.assertRaises(ValueError) as engine_create_error:
                connection_builder.get_engine()

            self.assertEqual(str(engine_create_error.exception), example_error_message)

    def test_construct_conn_string(self):
        """
        Test connection string assembly
        """
        credential_manager = MagicMock()
        connection_builder = ConnectionBuilder(credential_manager, self.db_config)

        with patch.object(connection_builder, "_get_access_token") as mock_get_token:
            mock_token = MagicMock(access_token="dummy_token")
            mock_get_token.return_value = mock_token
            conn_string = connection_builder._construct_conn_string()
            expected_conn_string = "databricks://token:dummy_token@hostname/?http_path=http_path&catalog=db"
            self.assertEqual(conn_string, expected_conn_string)

            self.db_config.db = None
            conn_string_without_db = connection_builder._construct_conn_string()
            expected_conn_string_without_db = (
                "databricks://token:dummy_token@hostname/?http_path=http_path"
            )
            self.assertEqual(conn_string_without_db, expected_conn_string_without_db)

    def test_get_engine(self):
        """
        Test that engine is created and returned if it is initially None
        """
        credential_manager = MagicMock()
        connection_builder = ConnectionBuilder(credential_manager, self.db_config)
        connection_builder.engine = None
        self.assertIsNone(connection_builder.engine)

        with patch.object(connection_builder, "_get_access_token") as mock_get_token:
            mock_token = MagicMock(access_token="dummy_token")
            mock_get_token.return_value = mock_token
            connection_builder.get_engine()
            self.assertIsInstance(connection_builder.engine, Engine)

    def test_get_session(self):
        credential_manager = MagicMock()
        connection_builder = ConnectionBuilder(credential_manager, self.db_config)

        with patch.object(connection_builder, "_get_access_token") as mock_get_token:
            mock_token = MagicMock(access_token="dummy_token")
            mock_get_token.return_value = mock_token
            connection_builder.get_session()
            self.assertIsNotNone(connection_builder.session)
            self.assertIsInstance(connection_builder.session, Session)
