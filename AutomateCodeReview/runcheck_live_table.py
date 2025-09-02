import ast, os
from automate.dbconnection import DBConnection
from datetime import datetime
from automate.logger import getLogger


class CheckPyFiles:
    def __init__(self, localdirectory: str):
        self.logger = getLogger("CheckPyFiles", Stream=True)
        self.localdirectory = os.path.normpath(localdirectory)
        self.files = [file for file in os.listdir(self.localdirectory) if file.endswith('.py')]
        self.db = DBConnection()

    def getKwargsFromFile(self, filepath):
        with open(filepath, "r", encoding="utf-8") as file:
            try:
                tree = ast.parse(file.read(), filename=filepath)
            except Exception as e:
                self.logger.error(f"Cannot parse py file {os.path.basename(filepath)}: {str(e)}")
                return {}
            kwargs = {}
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                    for keyword in node.keywords:
                        # make dynamic
                        if keyword.arg not in ("CDMSID", "URLSNoList", "DailySpiderID", "PatternID"):
                            continue
                        try:
                            kwargs[keyword.arg] = ast.literal_eval(keyword.value)
                        except ValueError:
                            kwargs[keyword.arg] = keyword.value 
        if not kwargs:
            return {}
        return kwargs

    def getCompanyDetailsbyCDMSID(self, CDMSID):
        """
        returns company_details_dict
        """
        query = "Select Sno, jobURL, CDMSID, DailySpider, Active, PatternID From CP_Companies where CDMSID = {}".format(CDMSID)
        dbresponse = self.db.executequery(query)
        if dbresponse:
            company_details_dict = {
                "URLSNo": dbresponse[0][0],
                "JobURL": dbresponse[0][1],
                "CDMSID": dbresponse[0][2],
                "DailySpider": dbresponse[0][3],
                "Active": dbresponse[0][4],
                "PatternID": dbresponse[0][5]
            }
            self.logger.info(f"Company Details: {company_details_dict}")
            ### Check-1
            self.logger.info("Company present in CP_Companies. CHECK-1 PASSED.")
            return True, company_details_dict
        else:
            self.logger.warning(f"No data found for this CDMSID: {CDMSID}. Please update CP_Companies.")
            self.logger.warning("CHECK-1 FAILED.")
            return False, {}

    def mapJobURL(self, CDMSID):
        query_LUTemp = "Select Sno, jobURL, CDMSID, DailySpider, Active, PatternID From CP_Companies_LUTemp where CDMSID = {}".format(CDMSID)
        dbresponse = self.db.executequery(query)
        if dbresponse:
            JobURL_LUTemp = dbresponse[0][1]
        query_Live = "Select Sno, jobURL, CDMSID, DailySpider, Active, PatternID From CP_Companies where CDMSID = {}".format(CDMSID)
        dbresponse = self.db.executequery(query)
        if dbresponse:
            JobURL_Live = dbresponse[0][1]

        if JobURL_LUTemp == JobURL_Live:
            self.logger.info(f"mapJobURL Success for LUTemp and Live Table")
            return True
        else:
            self.logger.warning(f"mapJobURL failed for CDMSID : {CDMSID}. Please update CP_Companies with JobURL: {JobURL_LUTemp}.")
            return False

    def mapKwargsAndDB(self, companydetails: dict, kwargs: dict):
        # Check patternID == CDMSID, DailySpider, URLSNoList
        # returns updates dict

        updates = {}
        # CompanyDetails
        PatternID = companydetails['PatternID']
        CDMSID = companydetails['CDMSID']
        URLSNo = companydetails['URLSNo']
        DailySpider = companydetails['DailySpider']

        # FileKwargs
        patternid = kwargs.get('PatternID', '')
        dailyspider = kwargs.get('DailySpiderID', '')
        urlsnolist = kwargs.get('URLSNoList', [])
        cdmsid = kwargs.get('CDMSID', '')
        if cdmsid == CDMSID:
            if patternid==0 or (patternid and patternid != cdmsid):
                updates['PatternID'] = cdmsid
            if dailyspider==0 or (dailyspider and dailyspider != DailySpider):
                updates['DailySpiderID'] = DailySpider
            if urlsnolist[0]==0 or (urlsnolist and urlsnolist[0] != URLSNo):
                updates["URLSNoList"] = [URLSNo]

        return updates

    def updateKwargsInFile(self, filepath, updates: dict):
        ### Check-2
        self.logger.info(f"{os.path.basename(filepath)} needs kwargs updates: {updates}")
        try:
            with open(filepath, "r", encoding="utf-8") as file:
                lines = file.readlines()

            updated_lines = []
            for line in lines:
                for key, new_value in updates.items():
                    if f"{key}=" in line:
                        # Find the part of the line with the `key=value`
                        start_idx = line.index(f"{key}=") + len(f"{key}=")
                        end_idx = line.find(",", start_idx)
                        if end_idx == -1:  # Handle the case where there's no comma (e.g., last argument)
                            end_idx = line.find(")", start_idx)
                        
                        # Replace the old value with the new value
                        old_value = line[start_idx:end_idx].strip()
                        new_value_repr = f"'{new_value}'" if isinstance(new_value, str) else str(new_value)
                        line = line.replace(f"{key}={old_value}", f"{key}={new_value_repr}")
                updated_lines.append(line)

            if updates:
                with open(filepath, "w", encoding="utf-8") as file:
                    file.writelines(updated_lines)

                self.logger.info(f"Successfully updated {os.path.basename(filepath)}")
                self.logger.info("CHECK-2 PASSED.")
                return True
            else:
                print(f"No updates in file {os.path.basename(filepath)}")
        except Exception as e:
            self.logger.error(f"Error updating {filepath}: {str(e)}")
            self.logger.error("CHECK-2 FAILED.")
            return False

    def checkActiveLiveTable(self, companydetails: dict):
        ### Check-3
        Active = companydetails['Active']
        CDMSID = companydetails['CDMSID']
        URLSNo = companydetails['URLSNo']

        # Active CHECK
        query_active = f"Select Active from CP_Companies where CDMSID = {CDMSID}"
        dbresponse_active = self.db.executequery(query_active)
        active = dbresponse_active[0][0]
        self.logger.info(f"Active in DB: {active}")
        if not active:
            self.logger.warning(f"Active is NULL in DB {dbresponse_active}")
            updatequery_active = f"update CP_Companies set Active = 1 where Sno = {URLSNo}"
            self.db.updatequery(updatequery_active)
            self.logger.info(f"{updatequery_active} -----> EXECUTED")
            self.logger.info("CHECK-3 PASSED.")
        elif active != 1:
            self.logger.warning("Active is not 1. Updating.")
            updatequery_active = f"update CP_Companiesset Active = 1 where Sno = {URLSNo}"
            self.db.updatequery(updatequery_active)
            self.logger.info(f"{updatequery_active} -----> EXECUTED")
            self.logger.info("CHECK-3 PASSED.")
        else:
            self.logger.info("Active is already 1. CHECK-3 PASSED.")

    def checkPatternIDLiveTable(self, companydetails: dict):
        # if patternid NULL update patternid as cdmsid
        ### Check-4
        PatternID = companydetails['PatternID']
        CDMSID = companydetails['CDMSID']
        URLSNo = companydetails['URLSNo']

        # PatternID CHECK
        query_patternid = f"Select PatternID from CP_Companies where CDMSID = {CDMSID}"
        dbresponse = self.db.executequery(query_patternid)
        patternid = dbresponse[0][0]
        self.logger.info("PatternID in DB:", patternid)
        if not patternid:
            self.logger.warning("PatternID not present in DB", dbresponse)
            updatequery_patternid = f"update CP_Companies set PatternID = {CDMSID} where Sno = {URLSNo}"
            self.db.updatequery(updatequery_patternid)
            self.logger.info(f"{updatequery_patternid} -----> EXECUTED")
            self.logger.info("CHECK-4 PASSED.")
        elif patternid != CDMSID:
            self.logger.warning("PatternID <> CDMSID", dbresponse)
            updatequery_patternid = f"update CP_Companies set PatternID = {CDMSID} where Sno = {URLSNo}"
            self.db.updatequery(updatequery_patternid)
            self.logger.info(f"{updatequery_patternid} -----> EXECUTED")
            self.logger.info("CHECK-4 PASSED.")
        else:
            self.logger.info("PatternID == CDMSID.")
            self.logger.info("CHECK-4 PASSED.")


    def checkCrawlStatusLiveTable(self, companydetails: dict):
        # Check if CrawlStatus Table has Max Phase updated
        # Check if URLsno present in CrawlStatus Table

        ### Check-5
        DailySpider = companydetails['DailySpider']
        URLSNo = companydetails['URLSNo']
        table = "Tier1" if DailySpider == 4 else "NonTier1"

        # Map Max Phase
        maxphase_query = "SELECT max(TPhase) FROM CP_{}_Phase".format(table) 
        maxphase_response = self.db.executequery(maxphase_query)
        maxphase = int(maxphase_response[0][0])
        self.logger.info(f"DailySpider: {DailySpider}  MaxPhase: {maxphase}")

        # Map URLSno
        urlsno_crawlstatus_query = f"SELECT URLSNo, Phase, CrawlStatus FROM CP_{table}_CrawlStatus where URLSNo={URLSNo}"
        urlsno_crawlstatus_query_response = self.db.executequery(urlsno_crawlstatus_query)
        urlsno_is = urlsno_crawlstatus_query_response
        if not urlsno_is:
            self.logger.info(f"URLSNo {URLSNo} not present in CrawlStatus Table. Updating Table.")
            insert_phase_query = f"INSERT into CP_{table}_CrawlStatus (URLSNo,Phase,RecordDate,JobCounts,CrawlStatus) values('{URLSNo}', '{maxphase}', '{datetime.today().strftime('%Y-%m-%d')}', NULL, 0)"
            self.db.updatequery(insert_phase_query)
            self.logger.info(f"{insert_phase_query}---> EXECUTED")
            self.logger.info("CHECK-5 PASSED.")
        else:
            self.logger.error(f"Error in checkCrawlStatusLiveTable method., 'DailySpider': {DailySpider}, 'URLSNo': {URLSNo}")
            self.logger.error("CHECK-5 FAILED.")


    def run(self):
        self.logger.info(f"Starting processing for directory: {self.localdirectory}")
        
        all_company_details = [] 
        
        for file in self.files:
            details = {}
            self.logger.info(f"\nProcessing file: {file}")
            filepath = os.path.join(self.localdirectory, file)
            kwargs = self.getKwargsFromFile(filepath)
            if not kwargs:
                self.logger.warning(f"No valid kwargs found in file {file}. Skipping.")
                continue
            
            cdmsid = kwargs.get('CDMSID', '')
            patternid = kwargs.get('PatternID', '')
            urlsnolist = kwargs.get('URLSNoList', [])
            
            if not len(urlsnolist) == 1:
                self.logger.warning(f"Exception case for file {file}. More than one URLSNo.")
                continue
            try:
                status, companydetails = self.getCompanyDetailsbyCDMSID(cdmsid)
            except:
                print(f"{os.path.basename(file)} ---> SKIPPED")
                continue

            mapJobURL_status = self.mapJobURL(cdmsid)
            if mapJobURL_status:
                filedetails = {"FileName": file, "FilePath": filepath}
                companydetails.update(filedetails)
                all_company_details.append(companydetails)
                updates = self.mapKwargsAndDB(companydetails, kwargs)
                if updates:
                    self.updateKwargsInFile(filepath, updates)
                self.checkActiveLiveTable(companydetails)
                
                if patternid:
                    self.checkPatternIDLiveTable(companydetails)
                
                self.checkCrawlStatusLiveTable(companydetails)
            elif not status:
                filedetails = {"FileName": file, "Error": f"CDMSID {cdmsid} not having updated JobURL"}
                all_company_details.append(filedetails)
            self.logger.info(f"Finished processing file: {file}")
            self.logger.info("*" * 45)
            
            self.logger.info("Processing completed for all files.")

        return all_company_details


