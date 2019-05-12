import sys
import time
import mysql.connector
import configparser
import subprocess
import logging
import datetime
import os

config = configparser.ConfigParser()
config.read('config.ini')
ENV = config['DEFAULT']['ENV']
prefix_file = str(datetime.date.today())
file_log = "%s/monitor/%s-%s" % (config[ENV]['PATH_LOG'],prefix_file,"convert.log")
logging.basicConfig(filename=file_log,level=logging.DEBUG, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
if(not os.path.ismount('/var/www/html/gStorage/RECORD_FILE')):
    message = "Mount has issue try recover first time"
    logging.info(message)
    subprocess.call('python','/opt/monitorNewFileConvertToMP4/mount.sh')
    message = "Done for running command mount back"
    logging.info(message)
    if(not os.path.ismount('/var/www/html/gStorage/RECORD_FILE')):
        message = "Mount has issue after mount"
        logging.info(message)
        #send line alert

else:
    message = "Mount status is ok "
    logging.info(message)
