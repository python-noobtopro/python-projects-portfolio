
from utils import pdf2ExcelReqs
from reqs import *


class main(pdf2ExcelReqs):
	def __init__(self, masterexcelfile):
		super().__init__(masterexcelfile)
		self.columns = self.get_column_list()
		self.main_df = self.get_master_df()
		self.read_excel = self.read_excel
		self.final_folder_path = self.final_folder_path
		self.excel_folder_path = self.excel_folder_path
		self.logger = self.getAppLogger("logs", Stream=True)
		self.data = {}
		self.master_data = []
		self.found_column = set()
		self.missed_column = set()
		self.registered_column = set()
		self.value_pattern = r'^(?![\s:]*$).*[A-Za-z0-9].*$'      # match value : R, :R etc.

	def match_simple_column(self, excel):
		excluded_columns = [re.compile(r'.*[()\[\]{}].*'), re.compile(r"Organism Isolated", re.IGNORECASE), re.compile(r"INVESTIGATION", re.IGNORECASE)]
		ignore_column = set()
		for excluded_column in excluded_columns:
			if self.columns:
				for column in self.columns:
					match_object = re.match(excluded_column, column.strip())
					if match_object:
						ignore_column.add(column)
		
		for df, sheet in self.read_excel(excel):
			self.logger.info(f"<<< Step-3 (Simple matching) started for {excel} - {sheet} >>>")
			self.data["FileName (Test)"] = excel.replace(".xlsx", ".pdf")
			if self.columns:
				for column in self.columns:
					if column not in ignore_column:
						keyword = column.strip()
						mask = np.column_stack([df[col].astype(str).str.contains(re.compile(keyword, re.IGNORECASE), na=False) for col in df])
						find_result = np.where(mask==True)
						try:
							result = [find_result[0][0], find_result[1][0]]
							self.found_column.add(column)
						except Exception:
							self.missed_column.add(column)
							continue

						try:
							if not re.match(self.value_pattern, str(df.iloc[result[0], result[1] + 1])):
								self.data[column] = str(df.iloc[result[0], result[1] + 2]).strip(":")
								self.registered_column.add(column)
							elif re.match(self.value_pattern, str(df.iloc[result[0], result[1] + 1])):
								self.data[column] = str(df.iloc[result[0], result[1] + 1]).strip(":")
								self.registered_column.add(column)
						except:
							pass


	def match_medicine_column(self, excel):
		meds_column_pattern = re.compile(r'.*[()\[\]{}].*')
		meds_value_pattern = r'^\s*(:?\s*([SRI]|SDD)|([SRI]|SDD)\s*:\s*)\s*$'
		for df, sheet in self.read_excel(excel):
			self.logger.info(f"<<< Step-4.1 (Meds matching) started for {excel} - {sheet} >>>")
			if self.columns:
				for column in self.columns:
					match_object = re.match(meds_column_pattern, column.strip())
					if match_object:
						keyword = column.split("(")[0]
						mask = np.column_stack([df[col].astype(str).str.contains(re.compile(keyword, re.IGNORECASE), na=False) for col in df])
						find_result = np.where(mask==True)
						try:
							result = [find_result[0][0], find_result[1][0]]
							self.found_column.add(column)
						except Exception:
							self.missed_column.add(column)
							continue
						try:
							if not re.match(meds_value_pattern, str(df.iloc[result[0], result[1] + 1])):
								self.data[column] = str(df.iloc[result[0], result[1] + 2]).strip(":")
								self.registered_column.add(column)
							elif re.match(meds_value_pattern, df.iloc[result[0], result[1] + 1]):
								self.data[column] = str(df.iloc[result[0], result[1] + 1]).strip(":")
								self.registered_column.add(column)
						except:
							pass


	def match_organism_isolated_column(self, excel):
		organism_column_pattern = re.compile(r'Organism Isolated', re.IGNORECASE)
		for df, sheet in self.read_excel(excel):
			self.logger.info(f"<<< Step-4.2 (Organism isolated matching) started for {excel} - {sheet} >>>")
			if self.columns:
				for column in self.columns:
					match_object = re.match(organism_column_pattern, column.strip())
					if match_object:
						mask = np.column_stack([df[col].astype(str).str.contains(organism_column_pattern, na=False) for col in df])
						find_result = np.where(mask==True)
						try:
							result = [find_result[0][0], find_result[1][0]]
							self.found_column.add(column)
						except Exception:
							self.missed_column.add(column)
							continue

						try:
							if ":" not in str(df.iloc[result[0], result[1] + 0]):
								print(str(df.iloc[result[0], result[1] + 2].strip()), excel)
								self.data[column] = str(df.iloc[result[0], result[1] + 2].strip())
								self.registered_column.add(column)
							elif ":" in str(df.iloc[result[0], result[1] + 0]):
								self.data[column] = str(df.iloc[result[0], result[1] + 0].split(":")[1].strip())
								print("2", str(df.iloc[result[0], result[1] + 0].split(":")[1].strip()), excel)
								self.registered_column.add(column)
								
						except:
							pass

	def match_investigation_column(self, excel):
		investigation_column_pattern = re.compile(r"INVESTIGATION", re.IGNORECASE)
		for df, sheet in self.read_excel(excel):
			self.logger.info(f"<<< Step-4.3 (Investigation matching) started for {excel} - {sheet} >>>")
			if self.columns:
				for column in self.columns:
					match_object = re.match(investigation_column_pattern, column.strip())
					if match_object:
						mask = np.column_stack([df[col].astype(str).str.contains("PARAMETER NAME", na=False) for col in df])
						find_result = np.where(mask==True)
						try:
							result = [find_result[0][0], find_result[1][0]]
							self.found_column.add(column)
						except Exception:
							self.missed_column.add(column)
							continue
						try:
							self.data[column] = df.iloc[result[0] + 1, result[1] + 0]
							self.registered_column.add(column)
						except:
							pass

	def create_master_data_df(self):
		return pd.DataFrame.from_records(self.master_data)

	def create_consolidated_df(self):		
		final_df = pd.concat([self.main_df, self.create_master_data_df()], ignore_index=True)
		final_df = final_df.drop_duplicates()
		final_df = final_df.replace({'': np.nan, 'NA': np.nan, 'nan': np.nan})
		final_df = final_df.fillna('')
		return final_df

	def reset_data_points(self):
		self.data = {}
		self.found_column = set()
		self.missed_column = set()
		self.registered_column = set()

	def run(self):
		for files in os.listdir(self.excel_folder_path):
			self.match_simple_column(files)
			self.match_medicine_column(files)
			self.match_organism_isolated_column(files)
			self.match_investigation_column(files)
			self.master_data.append(self.data)
			self.logger.info(f"Column found for {files} --> {self.found_column}")
			self.logger.info(f"Column not found for {files} --> {self.missed_column}")
			if len(self.registered_column) != len(self.found_column):
				self.logger.error(f"Column not registered properly for {files} --> {self.found_column - self.registered_column}")
			self.reset_data_points()



		# Create df
		print(self.create_consolidated_df())
		self.create_consolidated_df().to_excel(os.path.join(self.final_folder_path, "Excels.xlsx"), index=False)


main("Main.xlsx").run()



