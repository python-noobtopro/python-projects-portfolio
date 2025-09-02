"""
This is Application Database Repository Class And it containts All data base related CURD Operations on Selected Database.
"""

import sys
from Settings import  JA_TBL_CONFIG, DB_CONFIG, DEFAULT_DB_ENV, DAILY_SPIDER_CONFIG, JA_TBL_COLUMN_SIZE_CONFIG
from utils.CommonFunctions import getAppLogger, splitListByChunks, getMachineInfo
import traceback
import pyodbc
import time
import time


class GenericDBRepositoryClass():
    """Generic DB Repository class"""

    def __init__(self, DBEnv):
        try:
            self.DB_CONFIG = DB_CONFIG[DBEnv]
        except KeyError:
            self.DB_CONFIG = DB_CONFIG.get(DEFAULT_DB_ENV)
        self.driver = self.DB_CONFIG['driver']
        self.server = self.DB_CONFIG['server']
        self.uid = self.DB_CONFIG['uid']
        self.pwd = self.DB_CONFIG['pwd']
        self.database = self.DB_CONFIG['database']
        self.timeout = self.DB_CONFIG['timeout']
        self.MachineInfo = getMachineInfo()
        self.connection = self.connect()
        self.dblogger = getAppLogger("General", Stream=True)

    def connect(self):
        """ Connect to Database """
        try:
            connection = pyodbc.connect(
                driver=self.driver,
                server=self.server,
                uid=self.uid,
                pwd=self.pwd,
                database=self.database, timeout=self.timeout)
            return connection
        except pyodbc.Error as ex:
            print (ex)
            traceback.print_exc()
            self.exlogger = getAppLogger("General", Stream=True)
            ErrorLog = traceback.format_exc()
            self.exlogger.error("%s connect process failed. Exception: %s, Log: %s",self.__doc__, ex, ErrorLog)

    def fetchAll(self, query):
        """ Connect to Database """        
        try:
            print (query)
            db_cursor = self.connection.cursor()
            db_cursor.execute(query)
            self.dblogger.info(query)
            if db_cursor.rowcount:
                columns = [column[0] for column in db_cursor.description]
                records = [dict(zip(columns, row)) for row in db_cursor.fetchall()]
                db_cursor.close()
                return True, records
            else:
                db_cursor.close()
                return True, ()            
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()            
            self.dblogger.info("get process failed. Query: %s, Exception: %s, Log: %s", query, ex, ErrorLog)
            return False, ErrorLog
    def fetchOne(self, query):
        """ Connect to Database """        
        try:
            self.dblogger.info(query)
            db_cursor = self.connection.cursor()
            db_cursor.execute(query)
            if db_cursor.rowcount:
                columns = [column[0] for column in db_cursor.description]
                records = dict(zip(columns, db_cursor.fetchone()))
                return True, records
            else:
                return False, {}
            db_cursor.close()
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("fetchOne process failed. Query: %s, Exception: %s, Log: %s", query, ex, ErrorLog)
            return False, ErrorLog

    def save(self, query):
        """ Connect to Database """        
        try:
            db_cursor = self.connection.cursor()
            res = db_cursor.execute(query)
            db_cursor.commit()
            db_cursor.close()
            return True, db_cursor
        except pyodbc.Error as ex:
            db_cursor.rollback()
            ErrorLog = traceback.format_exc()
            self.dblogger.info("save process failed. Query: %s, Exception: %s, Log: %s",query, ex, ErrorLog)
            return False, ErrorLog

    def saveJob(self, DailySpiderName, PhaseNumber, URLSNo, **kwargs):
        """ Insert records into table 'Output Inserted.Sno'"""
        try:
            TblName =  JA_TBL_CONFIG[f"{DailySpiderName}_Jobs"].format(kwargs['OutputTableName'])
            statement = """ INSERT INTO {} ([URLSno], [JobTitle], [JobURL], 
            [PostDate], [Location], [ShortCode],[JobDesc], [Phase]) 
             VALUES  (N'{}',N'{}',N'{}',N'{}',N'{}',N'{}',N'{}',N'{}')
            """.format(TblName, URLSNo, kwargs['JobTitle'].replace("'","''")[:JA_TBL_COLUMN_SIZE_CONFIG['JobTitle']], 
                kwargs['JobURL'].replace("'","''")[:JA_TBL_COLUMN_SIZE_CONFIG['JobURL']],
                kwargs['JobPostDate'][:JA_TBL_COLUMN_SIZE_CONFIG['JobPostDate']], 
                kwargs['JobLocation'].replace("'","''")[:JA_TBL_COLUMN_SIZE_CONFIG['JobLocation']],
                kwargs.get('ShortCode', '')[:JA_TBL_COLUMN_SIZE_CONFIG['ShortCode']], 
                kwargs.get('JobDesc', ''), 
                PhaseNumber,
                )
            #self.dblogger.info(statement)
            return self.save(statement)
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("jobInsert process failed. Record:  %s, Exception: %s, Log: %s", kwargs, ex, ErrorLog)
            return False, ErrorLog

    def insertPhaseCrawlStatus(self, DailySpiderName, PhaseNumber, URLSNo,  **kwargs):
        """ Insert records into table """
        try:
            TblName =  JA_TBL_CONFIG[f"{DAILY_SPIDER_CONFIG[DailySpiderName]}_CRAWL_STATUS"]
            db_cursor = self.connection.cursor()
            statement = """ INSERT INTO {} ([URLSno], [Phase],[JobCounts], [FailedJobCounts], 
            [CrawlStatus], [IPAddress], [MachineName], [FilePath]) 
            values (N'{}',N'{}',N'{}',N'{}',N'{}',N'{}',N'{}',N'{}')
            """.format(TblName, URLSNo, PhaseNumber, 0, 0, 0, self.MachineInfo.get('IPAddress', 'NA'), self.MachineInfo.get('MachineName', 'NA'), kwargs.get('FilePath', ''))
            self.save(statement)
            statement = f"SELECT SNo FROM  {TblName} WHERE URLSno={URLSNo} And Phase={PhaseNumber}"
            return self.fetchOne(statement)
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("insertPhaseCrawlStatus process failed. Record:  %s, Exception: %s, Log: %s", kwargs, ex, ErrorLog)
            return False, ErrorLog

    def getURLCrawlStatusByPhaseNumberAndURLSNo(self,DailySpiderName, PhaseNumber, URLSNo):
        """ Insert records into table """
        try:
            TblName =  JA_TBL_CONFIG[f"{DailySpiderName}_CRAWL_STATUS"]
            statement = """ SELECT SNo, URLSno, Phase, JobCounts as SuccessTotal, FailedJobCounts as FailedTotal, CrawlStatus 
                FROM {} WHERE Phase={} AND URLSNo={}
            """.format(TblName, PhaseNumber, URLSNo)
            return self.fetchOne(statement)
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("getURLCrawlStatusByPhaseNumberAndURLSNo process failed. Record:  %s, Exception: %s, Log: %s", ex, ErrorLog)
            return False, ErrorLog

    def getPatternDetailsByPatterID(self,PatternID):
        try:
            TblName =  JA_TBL_CONFIG["PatternMaster"]
            statement = """ SELECT *  FROM {} WHERE ID={}
            """.format(TblName, PatternID)
            return self.fetchOne(statement)
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("getPatternDetailsByPatterID process failed. Record:  %s, Exception: %s, Log: %s",  ex, ErrorLog)
            return False, ErrorLog


    def updateURLPhaseCrawlStatusByURLSNo(self, DailySpiderName, PhaseNumber, URLSNo,**kwargs):
        """ Insert records into table """
        try:
            TblName =  JA_TBL_CONFIG[f"{DailySpiderName}_CRAWL_STATUS"]
            if kwargs['CrawlStatus'] == 4:
                statement = """ UPDATE {} SET JobCounts={}, FailedJobCounts={}, CrawlStatus='{}', ErrorLog='{}', 
                    HTMLText='{}', IPAddress='{}', MachineName='{}',  RunStartTime= getDate(), FilePath='{}'  WHERE URLSNo={} AND Phase={}
                """.format(TblName, kwargs['SuccessTotal'], kwargs['FailedTotal'], 
                    kwargs['CrawlStatus'], kwargs.get('ErrorLog', ''), kwargs.get('HTMLText', ''), self.MachineInfo.get('IPAddress', 'NA'), 
                    self.MachineInfo.get('MachineName', 'NA'),  kwargs.get('FilePath', ''),URLSNo, PhaseNumber
                    )
            else:
                statement = """ UPDATE {} SET JobCounts={}, FailedJobCounts={}, CrawlStatus='{}', ErrorLog='{}', 
                    HTMLText='{}', IPAddress='{}', MachineName='{}', RunEndTime= getDate(),FilePath='{}'  WHERE URLSNo={} AND Phase={}
                """.format(TblName, kwargs['SuccessTotal'], kwargs['FailedTotal'], 
                    kwargs['CrawlStatus'], kwargs.get('ErrorLog', ''), kwargs.get('HTMLText', ''), self.MachineInfo.get('IPAddress', 'NA'), 
                    self.MachineInfo.get('MachineName', 'NA'),  kwargs.get('FilePath', ''),URLSNo, PhaseNumber
                    )

            return self.save(statement)
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("updateURLPhaseCrawlStatusByURLSNo process failed. Record:  %s, Exception: %s, Log: %s", kwargs, ex, ErrorLog)
            return False, ErrorLog

    def updateJobsDescStatusByURLSNo(self, DailySpiderName, PhaseNumber, URLSNo,OutputTableName,**kwargs):
        """ Insert records into table """
        try:
            JobsTblName =  JA_TBL_CONFIG[f"{DailySpiderName}_Jobs"].format(OutputTableName)
            Consolidated_TblName = JA_TBL_CONFIG[f"{DailySpiderName}_ConsolidatedData"]            
            statement = f""" UPDATE t1 SET t1.isnew = 0 FROM {JobsTblName} t1 with(nolock)
                INNER JOIN {JA_TBL_CONFIG['Company_Registries']} t2 with(nolock) on t1.urlsno = t2.Sno
                INNER JOIN {Consolidated_TblName}  t3 with(nolock) on t2.CDMSID = t3.CDMSID and t1.joburl = t3.url  and t1.JobTitle=t3.job_title
                WHERE t1.phase = {PhaseNumber} and t2.sno = {URLSNo}
                """
            return self.save(statement)
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("updateJobsDescStatusByURLSNo process failed. Record:  %s, Exception: %s, Log: %s", kwargs, ex, ErrorLog)
            return False, ErrorLog
    def updateJobsDescByJobSNo(self, DailySpiderName, JObSNo, JobDesc, OutputTableName):
        """ Insert records into table """
        try:
            JobsTblName =  JA_TBL_CONFIG[f"{DailySpiderName}_Jobs"].format(OutputTableName)
            statement = f"""UPDATE {JobsTblName}
                            SET JobDesc = N'{JobDesc}', Processed = 1 WHERE sno = {JObSNo}
                        """
            return self.save(statement)
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("updateJobsDescByJobSNo process failed. Record:  %s, Exception: %s, Log: %s",  ex, ErrorLog)
            return False, ErrorLog
    def updateJobDescPostDateByJobSNo(self, DailySpiderName, JObSNo, JobDesc,JobPostDate, OutputTableName):
        """ Insert records into table """
        try:
            JobsTblName =  JA_TBL_CONFIG[f"{DailySpiderName}_Jobs"].format(OutputTableName)
            statement = f"""UPDATE {JobsTblName}
                            SET JobDesc = N'{JobDesc}',PostDate = '{JobPostDate}', Processed = 1 WHERE sno = {JObSNo}
                        """
            return self.save(statement)
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("updateJobDescPostDateByJobSNo process failed. Record:  %s, Exception: %s, Log: %s", ex, ErrorLog)
            return False, ErrorLog

    def updateJobDescJobLocationByJobSNo(self, DailySpiderName, JObSNo, JobDesc, JobLocation, OutputTableName):
        """ Insert records into table """
        try:
            JobsTblName =  JA_TBL_CONFIG[f"{DailySpiderName}_Jobs"].format(OutputTableName)
            statement = f"""UPDATE {JobsTblName}
                            SET JobDesc = N'{JobDesc}', Location = N'{JobLocation}', Processed = 1 WHERE sno = {JObSNo}
                        """
            return self.save(statement)
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("updateJobDescJobLocationByJobSNo process failed. Record:  %s, Exception: %s, Log: %s", ex, ErrorLog)
            return False, ErrorLog
    def updateJobDescJobLocationPostDateByJobSNo(self, DailySpiderName, JObSNo, JobDesc, JobLocation, JobPostDate, OutputTableName):
        """ Insert records into table """
        try:
            JobsTblName =  JA_TBL_CONFIG[f"{DailySpiderName}_Jobs"].format(OutputTableName)
            statement = f"""UPDATE {JobsTblName}
                            SET JobDesc = N'{JobDesc}',PostDate = '{JobPostDate}',Location = N'{JobLocation}', Processed = 1 WHERE sno = {JObSNo}
                        """
            return self.save(statement)
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("updateJobDescJobLocationPostDateByJobSNo process failed. Record:  %s, Exception: %s, Log: %s", ex, ErrorLog)
            return False, ErrorLog

    def getNewJobsOnNoDescByURLSNo(self, DailySpiderName, PhaseNumber, URLSNo, OutputTableName, **kwargs):
        """ Insert records into table """
        try:
            JobsTblName =  JA_TBL_CONFIG[f"{DailySpiderName}_Jobs"].format(OutputTableName)
            statement = f"""SELECT Sno, Joburl,ShortCode, URLSNo FROM {JobsTblName} t1 with(nolock)
                        WHERE URLSNo = {URLSNo} and isnew = 1 and Processed = 0 order by sno
                """
            return self.fetchAll(statement)
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("getNewJobsOnNoDescByURLSNo process failed. Record:  %s, Exception: %s, Log: %s", kwargs, ex, ErrorLog)
            return False, ErrorLog


    def getHTMLTagsByURLSNo(self, URLSNo):
        """ Connect to Database """        
        try:
            TblName =  JA_TBL_CONFIG[f"CompaniesTags"]
            statement = f"SELECT * FROM {TblName} WHERE [URLSno] = {URLSNo}"
            return self.fetchOne(statement)
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("getHTMLTagsBySNo process failed. Record:  %s, Exception: %s, Log: %s",  ex, ErrorLog)
            return False, ErrorLog

    def getHTMLTagsByPatternID(self, PatternID):
        """ Connect to Database """        
        try:
            TblName =  JA_TBL_CONFIG[f"CompaniesTags"]
            statement = f"SELECT * FROM {TblName} WHERE [PatternID] = {PatternID}"
            return self.fetchOne(statement)
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("getHTMLTagsBySNo process failed. Record:  %s, Exception: %s, Log: %s", ex, ErrorLog)
            return False, ErrorLog

    def getJobsByPhaseNumberAndURLSNo(self, DailySpiderName, PhaseNumber, URLSNo):
        try:
            TblName =  JA_TBL_CONFIG[f"{DailySpiderName}_Phase"]
            statement = f"""SELECT [JobTitle], [JobURL] FROM {TblName} WITH(nolock) 
                         WHERE [TPhase]={PhaseNumber} And [CompanyID]={URLSNo} And [JobDesc] IS NULL
                         """
            return self.fetchAll(statement)
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("getJobsByPhaseNumberAndSNo process failed. Record:  %s, Exception: %s, Log: %s",  ex, ErrorLog)
            return False, ErrorLog

    def insertGeneric(self, tbl_name, columns, record):
        """ Insert records into table """
        try:
            db_cursor = self.connection.cursor()
            column_names = ','.join(columns).strip()
            column_values = ("?," * len(columns))[:-1]
            statement = f"INSERT INTO {tbl_name}({column_names}) VALUES ({column_values})"
            self.dblogger.info(statement)
            db_cursor.execute(statement, tuple(record))
            db_cursor.commit()
            db_cursor.close()            
            return True
        except pyodbc.Error as ex:
            self.dblogger.info("Record insertion process failed. Record:  %s, Exception: %s, Log: %s", record, ex,
                        traceback.format_exc())
            return False

    def bulkInsert(self, tbl_name, columns, records, batch_size=100):
        """ Insert records into table """
        try:
            batch_list = splitListByChunks(records, batch_size)
            for each_batch in batch_list:
                db_cursor = self.connection.cursor()
                column_names = ','.join(columns).strip()
                column_values = ("?," * len(columns))[:-1].strip()
                statement = f"INSERT INTO {tbl_name}({column_names}) VALUES ({column_values})"
                self.dblogger.info(statement)
                db_cursor.executemany(statement, tuple(each_batch))
                res = db_cursor.commit()
                db_cursor.close()
            return True
        except pyodbc.Error as ex:
            self.dblogger.info("Bulk Record insertion process failed. Record:  %s, Exception: %s, Log: %s", records, ex,
                        traceback.format_exc())
            return False

    def update(self, db_con, tbl_name, ):
        pass

    def delete(self):
        pass

    def getMaxPhaseNumber(self, DailySpiderName):
        try:
            TblName =  JA_TBL_CONFIG[f"{DailySpiderName}_Phase"]
            query = f"SELECT MAX(TPhase) as PhaseNumber  FROM {TblName} WITH(nolock)"
            return self.fetchOne(query)

        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("getMaxPhaseNumber process failed. Exception: %s, Log: %s", ex, ErrorLog)
            return False, ErrorLog

    def getURLScenariosKeywordDetails(self):
        try:
            TblName =  JA_TBL_CONFIG["URL_Scenarios_Master"]
            query = f"SELECT CrawlStatusID, KeywordText FROM {TblName} WITH(nolock) WHERE KWStatus=1"
            return self.fetchAll(query)
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("getMaxPhaseNumber process failed. Exception: %s, Log: %s", ex, ErrorLog)
            return False, ErrorLog

    def getCompaniesByDailySpiderAndPatternID(self, DailySpiderName, PatternID, PhaseNumber):
        try:
            CrawlStatusTblName =  JA_TBL_CONFIG[f"{DAILY_SPIDER_CONFIG[DailySpiderName]}_CRAWL_STATUS"]
            query = f"""SELECT jobUrl,CDMSID,CompanyName,t1.Sno 
                        FROM {JA_TBL_CONFIG["Company_Registries"]} t1 with(nolock) 
                        inner join {CrawlStatusTblName} t2 with(nolock) on t1.Sno = t2.URLSNo
                        WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} 
                        and t2.Phase = {PhaseNumber} 
                        -- and t2.CrawlStatus in (0, 4)
                        and Active > 0
                        --and VMName= '{self.MachineInfo['MachineName']}' 
                        ORDER BY t1.Sno ASC"""
            CmpURLList = self.fetchAll(query)
            self.dblogger.info(query)
            if len(CmpURLList[1]) == 0:
                query = f"""SELECT jobUrl,CDMSID,CompanyName,t1.Sno 
                                        FROM {JA_TBL_CONFIG["Company_Registries"]} t1 with(nolock) 
                                        inner join {CrawlStatusTblName} t2 with(nolock) on t1.Sno = t2.URLSNo
                                        WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} 
                                        and t2.Phase = {PhaseNumber} 
                                        -- and t2.CrawlStatus in (0, 4)
                                        and Active > 0 
                                        ORDER BY t1.Sno ASC"""
                CmpURLList = self.fetchAll(query)
            self.dblogger.info(query)
            self.dblogger.info(CmpURLList)
            return CmpURLList
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.error("getCompaniesByDailySpiderAndPatternID process failed. Exception: %s, Log: %s", ex, ErrorLog)
            return False, ErrorLog

    def getCompaniesByDailySpiderAndPatternIDAndPyResource(self, DailySpiderName, PatternID, PyResource, PhaseNumber):
        CrawlStatusTblName =  JA_TBL_CONFIG[f"{DAILY_SPIDER_CONFIG[DailySpiderName]}_CRAWL_STATUS"]
        try:            
            # query = f"""SELECT jobUrl,CDMSID,CompanyName,t1.Sno 
            #             FROM {JA_TBL_CONFIG["Company_Registries"]} t1 with(nolock) 
            #             inner join {CrawlStatusTblName} t2 with(nolock) on t1.Sno = t2.URLSNo
            #             WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} and  PyResource={PyResource}
            #             and t2.Phase = {PhaseNumber} and t2.CrawlStatus = 0
            #             and Active>0
            #             and VMName= '{self.MachineInfo['MachineName']}' 
            #             ORDER BY Sno """
            query = f"""SELECT jobUrl,CDMSID,CompanyName,t1.Sno 
                        FROM {JA_TBL_CONFIG["Company_Registries"]} t1 with(nolock) 
                        inner join {CrawlStatusTblName} t2 with(nolock) on t1.Sno = t2.URLSNo
                        WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} and  PyResource={PyResource}
                        and t2.Phase = {PhaseNumber} 
                        -- and t2.CrawlStatus in (0, 4)
                        and Active > 0
                        --and VMName= '{self.MachineInfo['MachineName']}' 
                        ORDER BY t1.Sno ASC"""
            CmpURLList = self.fetchAll(query)
            if len(CmpURLList[1]) == 0:
                # query =  f"""SELECT jobUrl,CDMSID,CompanyName, Sno 
                #         FROM {JA_TBL_CONFIG["Company_Registries"]} with(nolock) 
                #         WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} And PyResource={PyResource}
                #         and Active>0
                #         ORDER BY Sno """
                query = f"""SELECT jobUrl,CDMSID,CompanyName,t1.Sno 
                        FROM {JA_TBL_CONFIG["Company_Registries"]} t1 with(nolock) 
                        inner join {CrawlStatusTblName} t2 with(nolock) on t1.Sno = t2.URLSNo
                        WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} and  PyResource={PyResource}
                        and t2.Phase = {PhaseNumber} and 
                        -- t2.CrawlStatus in (0, 4)
                        and Active > 0
                        ORDER BY t1.Sno ASC"""
                CmpURLList = self.fetchAll(query)
            self.dblogger.info(query)
            self.dblogger.info(CmpURLList)
            return CmpURLList
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("getCompaniesByDailySpiderAndPatternIDAndPyResource process failed. Exception: %s, Log: %s", ex, ErrorLog)
            return False, ErrorLog

    def getCompaniesByDailySpiderAndPatternIDAndCDMSID(self, DailySpiderName, PatternID, CDMSID, PhaseNumber):
        CrawlStatusTblName =  JA_TBL_CONFIG[f"{DAILY_SPIDER_CONFIG[DailySpiderName]}_CRAWL_STATUS"]
        try:            
            # query = f"""SELECT jobUrl,CDMSID,CompanyName, Sno 
            #             FROM {JA_TBL_CONFIG["Company_Registries"]} with(nolock) 
            #             WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} And CDMSID={CDMSID}
            #             and Active > 0 
            #             and VMName= '{self.MachineInfo['MachineName']}' 
            #             ORDER BY Sno """
            query = f"""SELECT jobUrl,CDMSID,CompanyName,t1.Sno 
                        FROM {JA_TBL_CONFIG["Company_Registries"]} t1 with(nolock) 
                        inner join {CrawlStatusTblName} t2 with(nolock) on t1.Sno = t2.URLSNo
                        WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} and  CDMSID={CDMSID}
                        and t2.Phase = {PhaseNumber} 
                        -- and t2.CrawlStatus in (0, 4)
                        and Active > 0 --and VMName= '{self.MachineInfo['MachineName']}' 
                        ORDER BY t1.Sno ASC"""
            # print(query)
            CmpURLList = self.fetchAll(query)
            if len(CmpURLList[1]) == 0:
                # query = f"""SELECT jobUrl,CDMSID,CompanyName, Sno 
                #         FROM {JA_TBL_CONFIG["Company_Registries"]} with(nolock) 
                #         WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} And CDMSID={CDMSID}
                #         and Active>0 
                #         ORDER BY Sno """
                query = f"""SELECT jobUrl,CDMSID,CompanyName,t1.Sno 
                        FROM {JA_TBL_CONFIG["Company_Registries"]} t1 with(nolock) 
                        inner join {CrawlStatusTblName} t2 with(nolock) on t1.Sno = t2.URLSNo
                        WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} and  CDMSID={CDMSID}
                        and t2.Phase = {PhaseNumber} 
                        -- and t2.CrawlStatus in (0, 4)
                        and Active > 0
                        ORDER BY t1.Sno ASC"""
                CmpURLList = self.fetchAll(query)
            self.dblogger.info(query)
            self.dblogger.info(CmpURLList)
            return  CmpURLList
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("getCompaniesByDailySpiderAndPatternIDandCDMSID process failed. Exception: %s, Log: %s", ex, ErrorLog)
            return False, ErrorLog

    def getCompaniesByDailySpiderAndPatternIDAndCDMSIDAndPyResource(self, DailySpiderName, PatternID, CDMSID,PyResource, PhaseNumber):
        CrawlStatusTblName =  JA_TBL_CONFIG[f"{DAILY_SPIDER_CONFIG[DailySpiderName]}_CRAWL_STATUS"]
        try:            
            # query = f"""SELECT jobUrl,CDMSID,CompanyName, Sno 
            #             FROM {JA_TBL_CONFIG["Company_Registries"]} with(nolock) 
            #             WHERE  DailySpider = {DailySpiderName} And 
            #             PatternID={PatternID} And CDMSID={CDMSID} And PyResource={PyResource}
            #             and VMName= '{self.MachineInfo['MachineName']}' 
            #             and Active>0 ORDER BY Sno """
            query = f"""SELECT jobUrl,CDMSID,CompanyName,t1.Sno 
                        FROM {JA_TBL_CONFIG["Company_Registries"]} t1 with(nolock) 
                        inner join {CrawlStatusTblName} t2 with(nolock) on t1.Sno = t2.URLSNo
                        WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} and  CDMSID={CDMSID} and PyResource={PyResource}
                        and t2.Phase = {PhaseNumber} 
                        -- and t2.CrawlStatus in (0, 4) and VMName= '{self.MachineInfo['MachineName']}' 
                        and Active > 0
                        ORDER BY t1.Sno ASC"""
            CmpURLList = self.fetchAll(query)
            if len(CmpURLList[1]) == 0:
                # query = f"""SELECT jobUrl,CDMSID,CompanyName, Sno 
                #         FROM {JA_TBL_CONFIG["Company_Registries"]} with(nolock) 
                #         WHERE  DailySpider = {DailySpiderName} And 
                #         PatternID={PatternID} And CDMSID={CDMSID} And PyResource={PyResource}
                #         and Active>0 ORDER BY Sno """

                query = f"""SELECT jobUrl,CDMSID,CompanyName,t1.Sno 
                        FROM {JA_TBL_CONFIG["Company_Registries"]} t1 with(nolock) 
                        inner join {CrawlStatusTblName} t2 with(nolock) on t1.Sno = t2.URLSNo
                        WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} and  CDMSID={CDMSID} and PyResource={PyResource}
                        and t2.Phase = {PhaseNumber} 
                        -- and t2.CrawlStatus in (0, 4)
                        and Active > 0
                        ORDER BY t1.Sno ASC"""

                CmpURLList = self.fetchAll(query)
            self.dblogger.info(query)
            self.dblogger.info(CmpURLList)
            return CmpURLList
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("getCompaniesByDailySpiderAndPatternIDAndCDMSIDAndPyResource process failed. Exception: %s, Log: %s", ex, ErrorLog)
            return False, ErrorLog

    def getCompaniesByDailySpiderAndCDMSIDAndPatternIDAndURLSNo(self, DailySpiderName, PatternID, CDMSID, URLSNoList, PhaseNumber):
        CrawlStatusTblName =  JA_TBL_CONFIG[f"{DAILY_SPIDER_CONFIG[DailySpiderName]}_CRAWL_STATUS"]

        try:
            if len(URLSNoList) ==1:
                URLSNoList ="({})".format(URLSNoList[0])
            # query = f"""SELECT jobUrl,CDMSID,CompanyName, Sno 
            #             FROM {JA_TBL_CONFIG["Company_Registries"]} with(nolock) 
            #             WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} And CDMSID={CDMSID} And Sno IN {URLSNoList}
            #             and VMName= '{self.MachineInfo['MachineName']}' 
            #             and Active > 0 ORDER BY Sno """

            query = f"""SELECT jobUrl,CDMSID,CompanyName,t1.Sno 
                        FROM {JA_TBL_CONFIG["Company_Registries"]} t1 with(nolock) 
                        inner join {CrawlStatusTblName} t2 with(nolock) on t1.Sno = t2.URLSNo
                        WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} and  CDMSID={CDMSID} and t1.Sno IN {URLSNoList}
                        and t2.Phase = {PhaseNumber} 
                        -- and t2.CrawlStatus in (0, 4) --and VMName= '{self.MachineInfo['MachineName']}' 
                        and Active > 0
                        ORDER BY t1.Sno ASC"""
            # print (query, "hello 446#"*10)
            CmpURLList = self.fetchAll(query)
            # print(CmpURLList,"hello 449#"*10)
            if len(CmpURLList[1]) == 0:
                # query = f"""SELECT jobUrl,CDMSID,CompanyName, Sno 
                #         FROM {JA_TBL_CONFIG["Company_Registries"]} with(nolock) 
                #         WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} And CDMSID={CDMSID} And Sno IN {URLSNoList}
                #         and Active>0 ORDER BY Sno """
                query = f"""SELECT jobUrl,CDMSID,CompanyName,t1.Sno 
                        FROM {JA_TBL_CONFIG["Company_Registries"]} t1 with(nolock) 
                        inner join {CrawlStatusTblName} t2 with(nolock) on t1.Sno = t2.URLSNo
                        WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} and  CDMSID={CDMSID} and t1.Sno IN {URLSNoList}
                        and t2.Phase = {PhaseNumber} 
                        -- and t2.CrawlStatus in (0, 4) 
                        and Active > 0
                        ORDER BY t1.Sno ASC"""
                CmpURLList = self.fetchAll(query)
            self.dblogger.info(query)
            self.dblogger.info(CmpURLList)
            return CmpURLList
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("getCompaniesByDailySpiderAndCDMSIDAndPatternIDAndURLSNo process failed. Exception: %s, Log: %s", ex, ErrorLog)
            return False, ErrorLog

    def getCompaniesByDailySpiderAndPatternIDAndURLSNoAndPyResource(self, DailySpiderName, PatternID, CDMSID, URLSNoList, PyResource, PhaseNumber):
        CrawlStatusTblName =  JA_TBL_CONFIG[f"{DAILY_SPIDER_CONFIG[DailySpiderName]}_CRAWL_STATUS"]
        try:            
            # query = f"""SELECT jobUrl,CDMSID,CompanyName, Sno 
            #             FROM {JA_TBL_CONFIG["Company_Registries"]} with(nolock) 
            #             WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} 
            #             And CDMSID={CDMSID} And Sno in {Sno} And PyResource={PyResource}
            #             and VMName= '{self.MachineInfo['MachineName']}' 
            #             and Active>0 ORDER BY Sno """
            query = f"""SELECT jobUrl,CDMSID,CompanyName,t1.Sno 
                        FROM {JA_TBL_CONFIG["Company_Registries"]} t1 with(nolock) 
                        inner join {CrawlStatusTblName} t2 with(nolock) on t1.Sno = t2.URLSNo
                        WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} and  CDMSID={CDMSID} and t1.Sno IN {URLSNoList} and PyResource={PyResource}
                        and t2.Phase = {PhaseNumber} and t2.CrawlStatus in (0, 4) --and VMName= '{self.MachineInfo['MachineName']}' 
                        and Active > 0
                        ORDER BY t1.Sno ASC"""
            CmpURLList = self.fetchAll(query)

            if len(CmpURLList[1]) == 0:
                # query = f"""SELECT jobUrl,CDMSID,CompanyName, Sno 
                #         FROM {JA_TBL_CONFIG["Company_Registries"]} with(nolock) 
                #         WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} 
                #         And CDMSID={CDMSID} And Sno in {Sno} And PyResource={PyResource}
                #         and Active>0 ORDER BY Sno """

                query = f"""SELECT jobUrl,CDMSID,CompanyName,t1.Sno 
                        FROM {JA_TBL_CONFIG["Company_Registries"]} t1 with(nolock) 
                        inner join {CrawlStatusTblName} t2 with(nolock) on t1.Sno = t2.URLSNo
                        WHERE  DailySpider = {DailySpiderName} And PatternID={PatternID} and  CDMSID={CDMSID} and t1.Sno IN {URLSNoList} and PyResource={PyResource}
                        and t2.Phase = {PhaseNumber} and t2.CrawlStatus in (0, 4)
                        and Active > 0
                        ORDER BY t1.Sno ASC"""
                CmpURLList = self.fetchAll(query)
            self.dblogger.info(query)
            self.dblogger.info(CmpURLList)
            return CmpURLList
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("getCompaniesByDailySpiderAndPatternIDAndURLSnoAndPyResource process failed. Exception: %s, Log: %s", ex, ErrorLog)
            return False, ErrorLog
    def getCompaniesByDailySpiderandScheduleNoandMachineName(self,DailySpiderID, ScheduleNo,MachineName):
        """ Insert records into table """
        try:
            DailySpiderName = DAILY_SPIDER_CONFIG[DailySpiderID]
            CrawlStatusTblName =  JA_TBL_CONFIG[f"{DAILY_SPIDER_CONFIG[DailySpiderID]}_CRAWL_STATUS"]
            TblName =  JA_TBL_CONFIG[f"{DailySpiderName}_CRAWL_STATUS"]
            query = f"""SELECT * FROM [CP_Companies] WHERE  Active > 0  and ScheduleNo = {ScheduleNo} """
            CmpURLList = self.fetchAll(query)
            return CmpURLList
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("getURLCrawlStatusByPhaseNumberAndURLSNo process failed. Record:  %s, Exception: %s, Log: %s", ex, ErrorLog)
            return False, ErrorLog

    def getCompaniesByDailySpiderScheduleNo(self,DailySpiderID, ScheduleNo):
            """ Insert records into table """
            try:
                DailySpiderName = DAILY_SPIDER_CONFIG[DailySpiderID]
                CrawlStatusTblName =  JA_TBL_CONFIG[f"{DAILY_SPIDER_CONFIG[DailySpiderID]}_CRAWL_STATUS"]
                TblName =  JA_TBL_CONFIG[f"{DailySpiderName}_CRAWL_STATUS"]
                query = f"""SELECT * FROM [CP_Companies] WHERE  Active > 0 and ScheduleNo = {ScheduleNo} """
                CmpURLList = self.fetchAll(query)
                return CmpURLList
            except pyodbc.Error as ex:
                ErrorLog = traceback.format_exc()
                self.dblogger.info("getCompaniesByDailySpiderScheduleNo process failed. Record:  %s, Exception: %s, Log: %s", ex, ErrorLog)
                return False, ErrorLog

    def updateSchedulerStartTime(self, ScheduleNo):
        """ Insert records into table """
        try:
            statement = """ Exec UpdateSchedulerStartTime @SchedulerSno = {} """.format(ScheduleNo)
            return self.save(statement)
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("updateSchedulerStartTime process failed. Record:  %s, Exception: %s, Log: %s", ex, ErrorLog)
            return False, ErrorLog

    def updateSchedulerEndTime(self, ScheduleNo):
        """ Insert records into table """
        try:
            statement = """ Exec UpdateSchedulerEndTime @SchedulerSno = {} """.format(ScheduleNo)
            return self.save(statement)
        except pyodbc.Error as ex:
            ErrorLog = traceback.format_exc()
            self.dblogger.info("updateSchedulerStartTime process failed. Record:  %s, Exception: %s, Log: %s", ex, ErrorLog)
            return False, ErrorLog