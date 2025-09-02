from reqs import * 

class pdf2ExcelReqs:
	def __init__(self, masterexcelfile):
		self.utilslogger = self.getAppLogger('ExcelReader', Stream=True)
		#@@#

		self.data_folder_path = os.path.join(os.getcwd(), 'Data')
		self.final_folder_path = os.path.join(os.getcwd(), 'Final')
		self.excel_folder_path = os.path.join(os.getcwd(), 'Excels')
		self.masterexcelfile = os.path.join(self.data_folder_path, masterexcelfile)

	def get_column_list(self):
		try:
			df = pd.read_excel(self.masterexcelfile, engine="openpyxl")
			self.utilslogger.info(f"<<< Step-1 Passed (Read Master Excel) >>>")
		except Exception:
			error = traceback.format_exc()
			self.utilslogger.error(f"Step-1 Failed ---> {error}")
			return []
		columns = df.columns.to_list()
		return columns

	def get_master_df(self):
		return pd.DataFrame(columns=self.get_column_list())


	def read_excel(self, excelfile):
		excelfilepath = os.path.join(self.excel_folder_path, excelfile)
		try:
			df_sheets = pd.read_excel(excelfilepath, None, engine="openpyxl")
			self.utilslogger.info(f"<<< Step-2 Passed (Read Individual Excel) for {excelfile} >>>")
		except Exception:
			error = traceback.format_exc()
			self.utilslogger.error(f"Step-2 Failed ---> {excelfile} -- {error}")
		sheet_list = list(df_sheets.keys())
		for sheet in sheet_list:
			df = pd.read_excel(excelfilepath, engine="openpyxl", sheet_name=sheet)
			yield df, sheet


	def getAppLogger(self, log_name, Stream=False):
		os.makedirs(os.path.join(os.getcwd(), 'logs'), exist_ok=True)
		formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(message)s')
		log_level = logging.DEBUG
		handler = logging.StreamHandler(sys.stdout)
		LOG_FILE = os.path.join(os.getcwd(), 'logs', '{}.log'.format(log_name))
		ghandler = TimedRotatingFileHandler(LOG_FILE, when='D', interval=1, backupCount=10, encoding='utf-8')
		ghandler.setFormatter(formatter)
		glogger = logging.getLogger(log_name)
		glogger.addHandler(ghandler)
		glogger.setLevel(log_level)
		if Stream:
			glogger.addHandler(logging.StreamHandler(sys.stdout))
		return glogger