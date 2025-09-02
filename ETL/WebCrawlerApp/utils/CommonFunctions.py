"""
This is Application Utils Component And it containts All resuable functions.
"""
import requests
import warnings
import traceback
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from abc import ABC, abstractmethod
from dateutil.parser import (parse as dateparser)
from datetime import date, datetime
from scrapy.selector import Selector
from bs4 import BeautifulSoup
from Settings import BASE_DIR, LOG_ROTATE, IS_LOGGER_DISABLED
from logging.handlers import TimedRotatingFileHandler
import subprocess
import socket
import hashlib
import logging
import os
import json
import sys


def getHash(text):
    """ SHA256 hash of a String in Python"""
    return hashlib.sha256(text.encode('UTF-8').hexdigest())

def cleanText(InputTxt, Items):
    for item in Items:
        try:
            InputTxt = InputTxt.replace(item, '')
        except Exception:
            pass
    return InputTxt

def getAppLogger(log_name, Stream=False):
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(message)s')
    log_level = logging.DEBUG
    handler = logging.StreamHandler(sys.stdout)
    LOG_FILE = os.path.join(BASE_DIR, 'logs', '{}.log'.format(log_name))
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

def getMachineInfo():
    MachineInfo = {}
    MachineInfo['MachineName'] = socket.gethostname()
    p = subprocess.Popen(["ipconfig"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output, errors = p.communicate()
    for each_line in output.rstrip().split('\n'):
        if "IPv4 Address" in each_line:
            MachineInfo['IPAddress'] = each_line.split(":")[1].strip()
            break
    return MachineInfo

def splitListByChunks(inputList, n):
    n = max(1, n)
    return (inputList[i:i + n] for i in range(0, len(inputList), n))

class CustomLogHandler():
    """docstring for CustomLogHandler"""
    def __init__(self, name, message, data={}):
        self.Name = name
        self.Message = message
        self.Data = data
    def __repr__(self):
        return json.dumps(self.__dict__)