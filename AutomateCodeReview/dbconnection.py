import pyodbc
from automate.credentials import db_info
from automate.logger import getLogger

class DBConnection:
    def __init__(self):
        self.logger = getLogger("DbConnection", Stream=False)
        self.credentials = db_info
        try:
            self.con = self.connect()
        except Exception as e:
            self.logger.error(f"Failed to establish database connection: {e}")
        


    def connect(self):
        try:
            credentials = self.credentials
            con = pyodbc.connect(f'Driver={credentials.get("Driver", "")};Server={credentials.get("Server", "")};Database={credentials.get("Database", "")};uid={credentials.get("uid", "")};pwd={credentials.get("pwd", "")}')
            self.logger.info("DB connection established successfully.")
            return con
        except Exception as e:
            self.logger.error(f"Error connecting to database: {e}")
            raise

    def executequery(self, query):
        try:
            with self.con:
                cur = self.con.cursor()
                self.logger.info(f"Executing query: {query}")
                res = cur.execute(query)
                results = res.fetchall()
                self.logger.info(f"Query executed successfully. Rows fetched: {len(results)}")
                return results
        except Exception as e:
            self.logger.error(f"Error executing query: {query}\n{e}")
            raise

    def updatequery(self, query):
        try:
            with self.con:
                cur = self.con.cursor()
                self.logger.info(f"Executing update query: {query}")
                res = cur.execute(query)
                rowcount = res.rowcount
                self.logger.info(f"Update query executed successfully. Rows affected: {rowcount}")
                return rowcount
        except Exception as e:
            self.logger.error(f"Error executing update query: {query}\n{e}")
            raise
