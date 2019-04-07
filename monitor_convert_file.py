import sys
import time
import mysql.connector
import configparser
import subprocess

if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.read('config.ini')
    mydb = mysql.connector.connect(
                host=config['STAGING']["DB_HOST"],
                user=config["STAGING"]["DB_USER"],
                password=config["STAGING"]["DB_PASS"],
                database=config["STAGING"]["DB_DATABASE"])
    sql = "select count(*) as N  from tbl_watch where watch_status = 'Converting'"
    mycursor = mydb.cursor()
    mycursor.execute(sql)
    myresult = mycursor.fetchone()
    if(int(myresult[0]) ==0):
        #ready to check new file to convert
        sql = "select * from tbl_watch where watch_status = 'Found' and DATE_ADD(watch_created,  INTERVAL %s MINUTE) < NOW() order by watch_created asc LIMIT 0,1 " % config["STAGING"]["DELAY_MINUTE_CONVERT"]
        mycursor.execute(sql)
        myresult = mycursor.fetchone()
        if(myresult is not None):
            path_file =  myresult[1]
            watchID = myresult[0]
            sql ="update tbl_watch set watch_status ='Converting',watch_converted=now() where watchID = %s " %(watchID)
            mycursor.execute(sql)
            mydb.commit()
            destination_path = path_file.replace(".avi",".mp4")
            subprocess.call(["ffmpeg", "-i", path_file, destination_path])
            
            sql ="update tbl_watch set watch_status ='Done',watch_completed=now() where watchID = %s " %(watchID)
            mycursor.execute(sql)
            mydb.commit()
    mydb.close()