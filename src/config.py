import os


class Settings:
    def __init__(self) -> None:
        self.sql_connection_string = os.getenv("SQL_CONNECTION_STRING", "").strip()
        self.api_auth_level = os.getenv("API_AUTH_LEVEL", "function").strip().lower()

    def validate(self) -> None:
        if not self.sql_connection_string:
            raise ValueError("SQL_CONNECTION_STRING is required.")


settings = Settings()
