import sys
import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import mysql.connector
import configparser




class MyHandler(PatternMatchingEventHandler):
    patterns=["*.avi"]
    def process(self, event):
        if(event.event_type == "created"):
            mydb = mysql.connector.connect(
                host=config['STAGING']["DB_HOST"],
                user=config["STAGING"]["DB_USER"],
                password=config["STAGING"]["DB_PASS"],
                database=config["STAGING"]["DB_DATABASE"])
            mycursor = mydb.cursor()
            path_file = event.src_path
            sql = "INSERT INTO tbl_watch (watch_src_file, watch_hash,watch_created,watch_status) VALUES ('%s', '%s',now(),'Found')" % (path_file,hash(path_file))
            mycursor.execute(sql)
            mydb.commit()
            mydb.close()




    def on_modified(self, event):
        self.process(event)

    def on_created(self, event):
        self.process(event)


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    observer = Observer()
    observer.schedule(MyHandler(), path=config['STAGING']['PATH_MONITOR'],recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()