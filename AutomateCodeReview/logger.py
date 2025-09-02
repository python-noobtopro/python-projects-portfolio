import logging
import os, sys
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
LOG_ROTATE = 'D'
IS_LOGGER_DISABLED = False


base_directory = os.getcwd()
logs_dir = os.path.join(base_directory, 'logs')
os.makedirs(logs_dir, exist_ok=True)

def getLogger(log_name, Stream=False):
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(message)s')
    log_level = logging.DEBUG
    handler = logging.StreamHandler(sys.stdout)
    LOG_FILE = os.path.join(logs_dir, '{}.log'.format(log_name))
    ghandler = TimedRotatingFileHandler(LOG_FILE, when=LOG_ROTATE, interval=1, backupCount=10, encoding='utf-8')
    ghandler.setFormatter(formatter)
    glogger = logging.getLogger(log_name)
    glogger.addHandler(ghandler)
    if Stream:
        glogger.addHandler(logging.StreamHandler(sys.stdout))
    glogger.setLevel(log_level)
    if IS_LOGGER_DISABLED:
        glogger.disabled = IS_LOGGER_DISABLED
    return glogger
