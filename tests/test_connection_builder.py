import unittest
from unittest.mock import MagicMock, patch
from databricks_sqlalchemy_oauth.connection_builder import ConnectionBuilder, Token

class ConnectionBuilderTest(unittest.TestCase):
    def setUp(self):
        self.credential_manager = MagicMock()
        self.db_config = MagicMock()
        self.db_config.hostname = "hostname"
        self.db_config.port = "port"
        self.db_config.http_path = "http_path"
        self.db_config.db = "db"

    def test_get_access_token(self):
        token_mock = MagicMock(spec=Token)
        token_mock.access_token = 'dummy_token'
        token_mock.expired = False
        connection_builder = ConnectionBuilder(self.credential_manager, self.db_config)
        self.credential_manager.token.return_value = token_mock
        token = connection_builder.get_access_token()
        self.assertEqual(token, token_mock)

    def test_construct_conn_string(self):
        self.connection_builder.get_access_token = MagicMock(return_value=Token(access_token='dummy_token'))
        conn_string = self.connection_builder.construct_conn_string()
        expected_conn_string = f"databricks://token:dummy_token@hostname/?http_path=http_path&catalog=db"
        self.assertEqual(conn_string, expected_conn_string)

    def test_engine_is_created(self):
        with patch('schema.schema.create_engine') as mock_create_engine:
            self.connection_builder.engine = None
            self.assertIsNone(self.connection_builder.engine)
            
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            self.connection_builder.get_engine()
            engine_after_create = self.connection_builder.engine
            
            self.assertIsNotNone(engine_after_create)

    def test_get_inspect_obj(self):
        # Mock the get_engine method to return a dummy engine
        self.connection_builder.get_engine = MagicMock(return_value=MagicMock())

        # Call the get_inspect_obj method
        inspect_obj = self.connection_builder.get_inspect_obj()

        # Assert that the inspect_obj is not None
        self.assertIsNotNone(inspect_obj)

if __name__ == '__main__':
    unittest.main()