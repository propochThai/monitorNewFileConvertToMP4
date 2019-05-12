import sys
import time
import mysql.connector
import configparser
import subprocess
import logging
import datetime
import os

def connectDB():
    config = configparser.ConfigParser()
    config.read('config.ini')
    ENV = config['DEFAULT']['ENV']
    try:
        mydb = mysql.connector.connect(
                    host=config[ENV]["DB_HOST"],
                    user=config[ENV]["DB_USER"],
                    password=config[ENV]["DB_PASS"],
                    database=config[ENV]["DB_DATABASE"])
        return True, mydb
    except:
        t, v, tb = sys.exc_info()
        message = " Error cannot connect DB:%s" % v
        logging.error(message)
        return False, "error"
def KillJob(slot,logging):
    pidFile = "./process_%s.pid" % (slot)
    if  os.path.isfile(pidFile):
        #Have file then kill the job
        f = open(pidFile, "r")
        pid_infile = f.read()
        f.close()
        subprocess.call(["kill","-9",pid_infile])
        message="Kill process %s" % pid_infile
        os.remove(pidFile)
        logging.info(message)
    
    #logging.info(message)
    #subprocess.call(["python","/opt/monitorNewFileConvertToMP4/monitor_convert_file.py",slot,"&"])
    #run process
        

def CheckLastFoundJob(slot):
    config = configparser.ConfigParser()
    config.read('config.ini')
    ENV = config['DEFAULT']['ENV']
    status_connect, mydb = connectDB()
    if(status_connect):
        prefix_file = str(datetime.date.today())
        file_log = "%s/monitor/%s-%s" % (config[ENV]['PATH_LOG'],prefix_file,"convert.log")
        logging.basicConfig(filename=file_log,level=logging.DEBUG, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
        sql = "select ifnull(watchID),0)  from tbl_watch where watch_status = 'Found' and DATE_ADD(watch_created,  INTERVAL %s MINUTE) < NOW() and slot = %d order by watch_created asc  " % (config[ENV]["TIMEOUT_MINUTE_CONVERT"],slot)
        mycursor = mydb.cursor()
        mycursor.execute(sql)
        myresult = mycursor.fetchone()
        if(int(myresult[0]) ==0):
            message = "Not found died process at slot %d" % (slot)
            logging.info(message)
        else:
            watchID = int(myresult[0])
            message = "found died process at slot %d Watch_ID  %d " % (slot,watchID)
            logging.info(message)
            sql = "update tbl_watch set watch_status = 'Error' where watchID = %d" % watchID
            mycursor.execute(sql)
            mydb.commit()
            KillJob(slot,logging)

    mydb.close()
if __name__ == '__main__': 
    for slot in range(10):
        CheckLastFoundJob(slot)
   