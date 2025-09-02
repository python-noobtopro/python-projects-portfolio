__author__ = 'Rupesh Ranjan'

"""
This file takes folder path, number of threads, excluded files as a parameter
and runs all the files except excluded files in a folder using Multithreading and queue
"""

import logging
import os
import subprocess
import sys
import time
from datetime import date
from queue import Queue
from threading import Thread
from win10toast import ToastNotifier
from logging import handlers

# Import all parameters set by developer
import config

from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# create a logs' directory if not already exists
try:
    os.makedirs(os.path.join(config.BASE_DIR, "logs"))
except FileExistsError:
    pass


def getAppLogger(name):
    LOG_ROTATE = 'midnight'
    BASE_DIR = config.BASE_DIR
    LOG_FORMATTER = logging.Formatter('[%(asctime)s] %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    LOG_LEVEL = logging.DEBUG
    LOG_FILE = os.path.join(BASE_DIR, 'logs', '{}_{}.log'.format(name, date.today().strftime("%Y-%B-%d")))
    handler = handlers.TimedRotatingFileHandler(LOG_FILE, when=LOG_ROTATE)
    handler.setFormatter(LOG_FORMATTER)
    logger = logging.getLogger(str(name))
    logger.addHandler(handler)
    logger.setLevel(LOG_LEVEL)
    return logger


glogger = getAppLogger('general')
apscheduler_logger = getAppLogger('apscheduler')

# Give name of logger file


# not executed files list
error_check = []


# main execution function
def execute_file(file):
    try:
        ret = subprocess.call([sys.executable, os.path.join(config.BASE_DIR, file)])
        if ret != 0:
            glogger.error(f'Did not execute {file}, Exit code --> {ret}')
            error_check.append(file)
        else:
            glogger.info(f'Successfully executed --> {file}')
    except Exception as e:
        glogger.error(f"Did not execute {file}, EXCEPTION: {e}", exc_info=True)
        error_check.append(file)
        pass


class RunThread:
    def __init__(self):
        self.start = time.time()
        self.path = config.BASE_DIR
        self.file_list = []
        files = os.listdir(self.path)
        # Excluding 'config.py' by default
        config.EXCLUDED_FILES.append('config.py')
        for file in files:
            if file.endswith('.py'):
                if not any(x in file for x in config.EXCLUDED_FILES):
                    self.file_list.append(file)
        print(self.file_list)
        glogger.info(f"All files to be executed ---> {self.file_list}")

    # Create a queue of files and provide it to worker threads
    def create_queue(self):
        q = Queue(maxsize=0)
        no_thread = config.NUMBER_OF_THREADS
        for i in range(no_thread):
            thread = Thread(target=self.run_thread, args=(q,), daemon=True)
            thread.start()

        for x in range(len(self.file_list)):
            q.put(x)
        q.join()

    def run_thread(self, q):
        while True:
            c = q.get()
            file = self.file_list[c]
            execute_file(file)
            q.task_done()
            break

    def run(self):
        self.create_queue()
        toaster = ToastNotifier()
        toaster.show_toast("SEC Scheduler", "SEC NextSteps Scheduler is Running",
                           icon_path=None, duration=5,
                           threaded=True)
        glogger.info("Execution Time: --- %s minutes ---" % int((time.time() - self.start) / 60))
        glogger.info(f"Please check following files --> {error_check}")
        print("Execution Time: --- %s minutes ---" % int((time.time() - self.start) / 60))
        print(f"Please check following files --> {error_check}")


def main():
    scheduler = BlockingScheduler(logger=apscheduler_logger)
    trigger = CronTrigger(
        year="*", month="*", day="*", hour="*", minute="*", second="5"
    )

    scheduler.add_job(
        RunThread().run,
        # trigger=trigger,
        'interval',
        seconds=5)
    # logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG,
    #                     filename="1.log", filemode='w', encoding='utf-8')
    # logging.getLogger('apscheduler').setLevel(logging.DEBUG)
    scheduler.start()

    # print(dir(scheduler))


if __name__ == '__main__':
    # RunThread().run()
    main()

