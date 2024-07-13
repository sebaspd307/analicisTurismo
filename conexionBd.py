import pyodbc
import configparser

class Database:
    def __init__(self, config_file='config.ini'):
        # Leer la configuración desde el archivo
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        
        # Obtener los valores de configuración
        self.server = "SEBAS\\SQLEXPRESS"
        self.database = "twitterSentimientos"
        self.username = "sa"
        self.password = "123"
        
        # Crear la cadena de conexión
        self.connect_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}'
        
        # Intentar conectar al instanciar la clase
        self.connect()

    def connect(self):
        try:
            self.connection = pyodbc.connect(self.connect_string)
            print("Connected to database")
        except pyodbc.Error as e:
            print(f"Error connecting to database: {e}")

    def close(self):
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
            print("Connection closed")

    def execute_query(self, query, params=None):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or [])
            self.connection.commit()
            return cursor
        except pyodbc.Error as e:
            print(f"Error executing query: {e}")
            return None



