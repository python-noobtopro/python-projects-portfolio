import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import urllib
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from settings import DB_CONFIG, TABLE_CONFIG
# from ipdb import set_trace

class DBCon:
	" DataBase Connection and Query class "

	def __init__(self):
		self.table = TABLE_CONFIG['table']
		self.columns = TABLE_CONFIG['columns']
		self.connection = self.db_connect()

	def db_connect(self):
		params = f"DRIVER={DB_CONFIG['driver']};SERVER={DB_CONFIG['server']};PORT=1433;DATABASE={DB_CONFIG['database']};UID={DB_CONFIG['uid']};PWD={DB_CONFIG['pwd']}"
		params = urllib.parse.quote_plus(params)
		try:
			engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params), pool_timeout=DB_CONFIG['timeout'])
			print("Connecting with DB")
			return engine.connect()
		except SQLAlchemyError as e:
			print(f"An error occured while DB Connection: {e}")

	def query_table(self, table_name=None, columns=None):
		table = table_name or self.table
		column_names = ', '.join(columns) if columns is not None else self.columns
		with self.connection as connect:
			query = text(f"Select {column_names} FROM {table}")
			result = self.connection.execute(query).fetchall()
		df = pd.DataFrame(result)
		return df

	def custom_query(self):
		pass





