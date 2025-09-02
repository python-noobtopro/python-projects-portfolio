'''
Contains main business logic
'''
import os, csv, time
from automate.downloader import S3Downloader
from automate.runcheck import CheckPyFiles
from automate.runworkers import RunThreads
from automate.logger import getLogger

# s3foldername: 'Output/12_12_2024'
# localdir: local folder path (architecture)


class AutomateReview:
    def __init__(self, s3foldername: str, localdir: str, resourcename: str):
        self.PyResourceName = resourcename
        self.logger = getLogger("AutomateReview", Stream=True)
        self.s3foldername = s3foldername
        self.localdir = localdir
        self.base_folder = os.path.basename(self.s3foldername)
        self.downloaded_folder = os.path.join(self.localdir, self.base_folder)
        
    def download(self):
        # Step-1 --> Start with folder name in s3 bucket to download in architecture template itself
        self.logger.info(f"Step-1 --> Running downloader for {self.s3foldername} in {self.localdir}")
        S3Downloader().downloads3folder(s3folder=self.s3foldername, localdirectory=self.localdir)

    def checkfiles(self):
        # Step-2 --> Check file sanity and make it ready for run (files in downloaded folder)
        self.logger.info(f"Step-2 --> Checking all py files in folder {os.path.basename(self.downloaded_folder)} and getting company details.")
        all_company_details = CheckPyFiles(self.downloaded_folder).run()
        additional_details = {"s3folder": self.s3foldername,
                              "PyResource": self.PyResourceName
                            }
        for details in all_company_details:
            details.update(additional_details)
        return all_company_details

    def executefiles(self):
        # Step-3 --> Run all py files in the downloaded folder on worker threads and capture output
        self.logger.info(f"Step-3 --> Running all py files in folder {os.path.basename(self.downloaded_folder)} in threads.")
        execution_summary = RunThreads(self.downloaded_folder).run()
        return execution_summary    

    def createreport(self, companydetails, exec_summary, csvfilename: str):
        merged_data = []
        for exec_item in exec_summary:
            file_name = exec_item.get('FileName')
            matching_job = next((job for job in companydetails if job['FileName'] == file_name), None)
            merged_entry = exec_item.copy()  # Start with execution summary details
            if matching_job:
                merged_entry.update(matching_job)  # Add matching job details
            merged_data.append(merged_entry)

        # Write to CSV
        output_csv_file = csvfilename
        csv_headers = set(key for entry in merged_data for key in entry.keys())  # Collect all unique headers

        with open(output_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=sorted(csv_headers))
            writer.writeheader()
            writer.writerows(merged_data)

        self.logger.info(f"Merged data has been written to {output_csv_file}")


    def run(self):
        start_time = time.time()
        # self.download()
        all_company_details = self.checkfiles()
        execution_summary = self.executefiles()
        self.logger.info("*******  Automate Review process End  *********")
        self.logger.info("  *****  Please check details  *******  ")
        self.logger.info(all_company_details)
        self.logger.info(execution_summary)
        failed_files = [(status["FileName"], status) for status in execution_summary if status["Status"] != "Success"]
        self.createreport(all_company_details, execution_summary, 'report-19022025.csv')
        end_time = time.time()
        execution_time = (end_time - start_time)/60
        self.logger.info(f"***** Execution Time: {execution_time} mins ******")

if __name__ == '__main__':
    # Template's local dir path
    localdir='C:\\Users\\Rupesh.Ranjan\\OneDrive - GlobalData PLC\\Desktop\\Nexgile_Tasks\\nexgilereviewtemplate-new'
    resourcename="Rupesh"
    s3foldername='Output/19_02_2025'

    AutomateReview(s3foldername=s3foldername,
                    localdir=localdir,
                    resourcename=resourcename
                    ).run()
        