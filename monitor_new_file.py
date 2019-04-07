import sys
import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import mysql.connector
import configparser
import logging

class MyHandler(PatternMatchingEventHandler):
    patterns=["*.avi"]
    def process(self, event):
        if(event.event_type == "created"):
            prefix_file = str(datetime.date.today())
            file_log = "%s%s-%s" % (config[ENV]['PATH_LOG'],prefix_file,"watch.log")
            logging.basicConfig(filename=file_log, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
            message = "Found new file:%s" % event.src_path
            logging.info(message)
            try:
                mydb = mysql.connector.connect(
                    host=config[ENV]["DB_HOST"],
                    user=config[ENV]["DB_USER"],
                    password=config[ENV]["DB_PASS"],
                    database=config[ENV]["DB_DATABASE"])
                try:
                    mycursor = mydb.cursor()
                    path_file = event.src_path
                    sql = "INSERT INTO tbl_watch (watch_src_file, watch_hash,watch_created,watch_status) VALUES ('%s', '%s',now(),'Found')" % (path_file,hash(path_file))
                    mycursor.execute(sql)
                    mydb.commit()
                    mydb.close()
                    message = "Success add new file into convert Queue:%s" % event.src_path
                    logging.info(message)
                except:
                    t, v, tb = sys.exc_info()
                    message = " Error cannot insert data:%s" % v
                    logging.error(message)
            except:
                t, v, tb = sys.exc_info()
                message = " Error cannot connect DB:%s" % v
                logging.error(message)  
            

    def on_modified(self, event):
        self.process(event)

    def on_created(self, event):
        self.process(event)


if __name__ == '__main__':
    
    config = configparser.ConfigParser()
    

    config.read('config.ini')
    ENV = config['DEFAULT']['ENV']
    observer = Observer()
    observer.schedule(MyHandler(), path=config[ENV]['PATH_MONITOR'],recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()