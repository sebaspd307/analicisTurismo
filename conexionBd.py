import pyodbc

class Database:
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.connection = None

    def connect(self):
        try:
            string_conexion = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}'
            self.connection = pyodbc.connect(string_conexion)
            print("Connected")
        except Exception as e:
            print(e)    

    def close(self):
        if self.connection:
            self.connection.close()
            print("Connection closed")
            
    def execute_query(self, query, params=None):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or [])
            self.connection.commit()
            return cursor
        except Exception as e:
            print(e)
            return None                        