import sys
import time
import mysql.connector
import configparser
import subprocess
import logging
import datetime
if __name__ == '__main__':
    slot =  int(sys.argv[1])
    config = configparser.ConfigParser()
    config.read('config.ini')
    ENV = config['DEFAULT']['ENV']
    try:
        mydb = mysql.connector.connect(
                    host=config[ENV]["DB_HOST"],
                    user=config[ENV]["DB_USER"],
                    password=config[ENV]["DB_PASS"],
                    database=config[ENV]["DB_DATABASE"])
    except:
        t, v, tb = sys.exc_info()
        message = " Error cannot connect DB:%s" % v
        logging.error(message)
    sql = "select count(*) as N  from tbl_watch where watch_status = 'Converting' and slot = %d" % (slot)
    mycursor = mydb.cursor()
    mycursor.execute(sql)
    myresult = mycursor.fetchone()
    if(int(myresult[0]) ==0):
        #ready to check new file to convert
        sql = "select * from tbl_watch where watch_status = 'Found' and DATE_ADD(watch_created,  INTERVAL %s MINUTE) < NOW() and slot = %d order by watch_created asc  " % (config[ENV]["DELAY_MINUTE_CONVERT"],slot)
        mycursor.execute(sql)
        rows = mycursor.fetchall()
        
        for row in rows:
            prefix_file = str(datetime.date.today())
            file_log = "%s/monitor/%s-%s" % (config[ENV]['PATH_LOG'],prefix_file,"convert.log")
            logging.basicConfig(filename=file_log,level=logging.DEBUG, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
            
            path_file =  row[1]
            watchID = row[0]

            message = "Found new file waiting convert at slot :%s" % (path_file,slot)
            logging.info(message)
            try:
                sql ="update tbl_watch set watch_status ='Converting',watch_converted=now() where watchID = %s " %(watchID)
                mycursor.execute(sql)
                mydb.commit()
                destination_path = path_file.replace(".avi",".mp4")
                message = "converting from %s to %s" % (path_file,destination_path)
                logging.info(message)
                subprocess.call(["ffmpeg","-y","-i", path_file,"-threads","6","-codec","copy","-preset","ultrafast",destination_path])

                sql ="update tbl_watch set watch_status ='Done',watch_completed=now() where watchID = %s " %(watchID)
                mycursor.execute(sql)
                mydb.commit()
                message = "Convert completed: %s" % (destination_path)
                logging.info(message)
            except:
                t, v, tb = sys.exc_info()
                message = " Error cannot update data :%s %s %s" % (v,path_file,watchID)
                logging.error(message)
    mydb.close()