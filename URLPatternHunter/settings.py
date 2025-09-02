import os

PROJECT_DIR = os.path.join(os.getcwd())
BASE_DIR = os.getcwd()
DATABASE_DIR = os.path.join(BASE_DIR, 'database')
MAIN_DIR = os.path.join(BASE_DIR, 'main')

DB_CONFIG = {
                'driver': '{SQL Server}', 
                'server': 'jobsdbsql.jp.pdm.local',
                'database': 'CareerPages',
                'uid': 'pythonuser',
                'pwd': 'JHsUJ<97$jgn',
                'timeout' : 5
            }


TABLE_CONFIG = {
                    'table': '[CareerPages].[dbo].[CP_Patterns_Master]',
                    'columns': 'ID, PatternName, PyResource, Keyword'
	           }





	