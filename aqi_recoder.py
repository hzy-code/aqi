#!/usr/bin/python3
# -*- coding: utf8 -*-
import urllib
import urllib.parse
import urllib.request
import time
import sys
import os

import json

#db basic var
import db

# parser command line argument
def parse_cmd_argu():
    if len(sys.argv)==1:
        return time.localtime()
    return time.strptime(sys.argv[1],'%y-%m-%d %H:%M:%S')


# read all station id from db(aqi.station)
def read_all_station_id():
    sql="SELECT id FROM station"
    db.cur.execute(sql)
    id=list()
    for x in db.cur.fetchall():
        id.append(x[0])
    return id;

# return url for station_id
def get_url(station_id):
    path="www.zzemc.cn/em_aw/Services/DataCenter.aspx"
    time_tu=time.localtime()
    strtime=time.strftime('%Y-%m-%d %H:00:00',time.localtime())
    params=urllib.parse.urlencode({'type':'getPointHourData',
        'code':station_id,
        'time':strtime})
    url="http://"+path+"?"+params
    return url;

# get the json for station
def get_json(station_id):
    url=get_url(station_id)
    res=urllib.request.urlopen(url)
    return res.readall()

# parse json and return aqi data
def parse_json(txt):
    aqi_data=dict()
    obj=json.loads(txt)
    data=obj['Head'][0]
    aqi_data['aqi']=int(data['AQI'])
    aqi_data['date']=data['CREATE_DATE']
    aqi_data['pm25']=float(data['PM25'])
    aqi_data['pm25aqi']=int(data['PM25IAQI'])
    return aqi_data

# get the last recoder time from db 
def get_last_time(station_id):
    sql="select max(rec_time) from aqi_data where station_id="+str(station_id)
    count=db.cur.execute(sql)
    if count==0:
        return None
    res=db.cur.fetchall()
    if res[0][0] is None:
        return None
    return res[0][0].timestamp()

# write to db
def write_to_mysql(station_id,aqi_data):
    last_time=get_last_time(station_id)
    new_time=time.strptime(aqi_data['date'],'%Y-%m-%d %H:%M:%S')
    if last_time is not None and time.mktime(new_time) <= last_time:
        return 0
    sql="INSERT INTO aqi_data(rec_time,station_id,aqi,pm25aqi,pm25) "
    values=" VALUES ('%s',%d,%d,%d,%f)" %(aqi_data['date'],station_id,aqi_data['aqi'],aqi_data['pm25aqi'],aqi_data['pm25'])
    sql=sql+values
    db.cur.execute(sql)
    db.conn.commit()
    return 1


# add a recoder for a station
def add_recoder_station(station_id):
    url=get_url(station_id)
    try:
        data=get_json(station_id)
        aqi_data=parse_json(data.decode('utf-8'))
        write_to_mysql(station_id,aqi_data)
    finally:
        return 0
    
# query station name
def query_station_name(station_id):
    sql='SELECT name FROM station WHERE id=%d' %(station_id)
    db.cur.execute(sql)
    res=db.cur.fetchall()
    return res[0][0]

# create datafile for gnuplot
def create_datafile(station_id):
    #gen sql
    innertime=48*3600
    mintime=time.time()-innertime
    mintime=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(mintime))
    sql='SELECT rec_time,station_name,aqi,pm25aqi FROM st_aqi WHERE rec_time >= \'%s\' AND station_id=%d' %(mintime,station_id)
    # read data
    db.cur.execute(sql)
    res=db.cur.fetchall()
    # write file
    path=os.path.join('/home/hzy/share/aqi',query_station_name(station_id)+'.txt')
    fobj=open(path,'w')
    for line in res:
        date=time.strftime('%Y-%m-%d %H:%M:%S',line[0].timetuple())
        name=line[1]
        aqi=line[2]
        pm25aqi=line[3]
        f_line="%s %s %d %d\n" %(date,name,aqi,pm25aqi)
        fobj.write(f_line)
    fobj.close()
    
def main():
    ids=read_all_station_id()
    [ add_recoder_station(x) for x in ids ]
    [ create_datafile(x) for x in ids ]
    
if __name__=="__main__":
    main()
