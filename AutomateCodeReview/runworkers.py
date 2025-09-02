import os
import subprocess
import time
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import threading
from logger import getLogger

class RunThreads:
    def __init__(self, folderpath: str, threads=4):
        self.logger = getLogger("RunThreads", Stream=True)
        self.threads = threads
        self.folderpath = os.path.normpath(folderpath)
        self.lock = threading.Lock()

    def triggerCompaniesByFilePath(self, filepath):
        """Function to execute a Python file."""
        try:
            self.logger.info(f"Subprocess Started for file {os.path.basename(filepath)}")
            start_time = time.time()
            result = subprocess.run(
                ['python', filepath],
                capture_output=False,  # 
                text=True,  # Decode output as string
                check=True,  # Raise an exception if the script fails                
                shell=False
                )
            end_time = time.time()
            elapsed_time = (end_time - start_time) / 60
            self.logger.info(f"Subprocess Success for file {os.path.basename(filepath)}. Time: {elapsed_time} mins")
            return True, {f"Subprocess Success for file {os.path.basename(filepath)}"}
        except Exception as e:
            self.logger.error(f"{filepath} execution Failed: {str(e)}")
            return False, {str(e) or 'No error message captured'}


    def run(self):
        execution_summary = []
        processed_files = set()
        threads = self.threads
        folderpath = self.folderpath
        files = [os.path.join(folderpath, file) for file in os.listdir(folderpath) if file.endswith(".py")]
        if files:
            with ThreadPoolExecutor(max_workers=threads) as pool:
                futures = {pool.submit(self.triggerCompaniesByFilePath, file): file for file in files}
            for future in futures:
                file = futures[future]
                try:
                    file_summary = {}
                    with self.lock:
                        if file in processed_files:
                            self.logger.info(f"Skipping already processed file: {os.path.basename(file)}")
                            continue
                    self.logger.info(f"Started execution of {os.path.basename(file)} in thread successfully")
                    start_time = time.time()
                    time_stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    file_summary["FileName"] = os.path.basename(file)
                    file_summary["StartTime"] = time_stamp
                    try:
                        success, output = future.result()
                    except Exception as e:
                        success, output = False, f"Future Exception: {str(e)}"
                    if not success:
                        file_summary["Status"] = "Failed"
                        file_summary["SubprocessError"] = output
                        execution_summary.append(file_summary)
                        continue

                    file_summary["Status"] = "Success"
                    end_time = time.time()
                    execution_time = (end_time - start_time)/60
                    # self.logger.info(f"Ended execution of {os.path.basename(file)} in thread successfully. \nExecution Time: {execution_time} minutes")
                    # file_summary["ExecutionTime"] = f"{execution_time} mins"
                    with self.lock:
                        processed_files.add(file)
                    execution_summary.append(file_summary)
                except Exception as e:
                    self.logger.error(f"Error in executing Thread for {os.path.basename(file)}: {e}")
                    error_details = {
                        "Thread Status": "Thread Failed",
                        "ErrorType": type(e).__name__,  
                        "ErrorMessage": str(e),  
                        "FileName": os.path.basename(file),
                        "StackTrace": traceback.format_exc()  
                    }
                    file_summary.update(error_details)
                    execution_summary.append(file_summary)
        self.logger.info(f"Run complete for folder {os.path.basename(folderpath)}. Check Execution Summary.")
        self.logger.info(execution_summary)
        failed_files = [(status["FileName"], status) for status in execution_summary if status["Status"] != "Success"]
        for file, error in failed_files:
            self.logger.error(f"Execution Failed for {file}, {error}")
        return execution_summary


directory = "./23_04_2025"
RunThreads(directory).run()




