#!/usr/bin/python3
# -*- coding: utf8 -*-
import urllib
import urllib.parse
import urllib.request
import time
import sys
import os
import subprocess
import json

#db basic var
import db

import conf
import plot

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
def get_url(station_id,t=time.time()):
    path="www.zzemc.cn/em_aw/Services/DataCenter.aspx"
    time_tu=time.localtime()
    strtime=time.strftime('%Y-%m-%d %H:00:00',time.localtime(t))
    params=urllib.parse.urlencode({'type':'getPointHourData',
        'code':station_id,
        'time':strtime})
    url="http://"+path+"?"+params
    return url;

# get the json for station
def get_json(url):
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

# write fail (station,id) info to db(aqi_download_fail)
def log_fail(station_id,t):
    if station_id==0:
        return
    cur_t=time.time()
    local_t=time.localtime(cur_t)
    if cur_t - t < local_t.tm_min*60+local_t.tm_sec:
        return;
    # write basic info (station_id ,time)
    strtime=time.strftime('%y-%m-%d %H:00:00',time.localtime(t))
    try:
        sql="INSERT INTO aqi_download_fail(station_id,rec_time) VALUES(%d,'%s')" %(station_id,strtime)
        res=db.cur.execute(sql)
        db.conn.commit()
    except:
        # add fail count
        sql="UPDATE aqi_download_fail SET fail_count=fail_count+1 WHERE station_id=%d AND rec_time='%s'" %(station_id,strtime)
        db.cur.execute(sql)
        db.conn.commit()

# add a recoder for a station
def add_recoder_station(station_id):
    rectime=get_recoder_time(station_id)
    for t in rectime:
        url=get_url(station_id,t)
        try:
            data=get_json(url)
            aqi_data=parse_json(data.decode('utf-8'))
            write_to_mysql(station_id,aqi_data)
        except:
            log_fail(station_id,t)        
            continue
    
# query station name
def query_station_name(station_id):
    sql='SELECT name FROM station WHERE id=%d' %(station_id)
    db.cur.execute(sql)
    res=db.cur.fetchall()
    return res[0][0]

# create datafile for gnuplot
def create_datafile(station_id):
    #gen sql
    innertime=24*3600
    mintime=time.time()-innertime
    mintime=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(mintime))
    sql='SELECT rec_time,station_name,aqi,pm25aqi FROM st_aqi WHERE rec_time >= \'%s\' AND station_id=%d' %(mintime,station_id)
    # read data
    db.cur.execute(sql)
    res=db.cur.fetchall()
    # write file
    path=os.path.join(conf.DATA_DIR,query_station_name(station_id)+'.txt')
    fobj=open(path,'w')
    for line in res:
        date=time.strftime('%Y-%m-%d %H:%M:%S',line[0].timetuple())
        name=line[1]
        aqi=line[2]
        pm25aqi=line[3]
        f_line="%s %s %d %d\n" %(date,name,aqi,pm25aqi)
        fobj.write(f_line)
    fobj.close()
    
# get skip fail time
def get_fail_time(station_id):
    sql="SELECT max(rec_time) FROM aqi_download_fail WHERE station_id=%d AND fail_count>=%d" %(station_id,3)
    if 0==db.cur.execute(sql):
        return None
    res=db.cur.fetchall()
    if len(res)==0:
        return None
    rec_time=res[0][0]
    if rec_time is None:
        return None
    return rec_time.timestamp()


# get the time need to recoder aqi
def get_recoder_time(station_id):
    rec_times=list()
    lasttime=get_last_time(station_id)
    if lasttime is None:
        rec_times.append(time.time())
        return rec_times
    fail_time=get_fail_time(station_id)
    if fail_time is not None:
        if lasttime < fail_time:
            lasttime = fail_time

    # subscration the mins and sec
    fun=lambda x : int(time.mktime(x)-x.tm_sec-x.tm_min*60)
    lasttime=time.localtime(lasttime)
    lasttime=fun(lasttime)
    curtime=time.localtime()
    curtime=fun(curtime)
    # gen list
    max=5;
    endtime=lasttime+max*3600
    endtime=int(min(endtime,curtime))
    return list(range(lasttime+3600,endtime+1,3600))

def clean_aqi_download_fail(id):
    pass

# 初始化函数 
def init():
    cmd=('/usr/bin/vmhgfs-fuse .host:/share '+conf.BASE_DIR).split(' ')
    if not os.path.exists(conf.DATA_DIR):
        if subprocess.call(cmd) != 0:
            print('mount .host:/share error')

# plot the graph for id
def plot_id(id):
    if id==0:
        return
    title='AQI'
    sql='SELECT name FROM station WHERE id=%s' %(id)
    if db.cur.execute(sql)==0:
        return
    res=db.cur.fetchall()
    try:
        station=res[0][0]
        direct=plot.gen_plot_direct(title,station,station)
        plot.plot(direct)
    except:
        return

def test():
    init()
    
def main():
    init()
    ids=read_all_station_id()
    [ add_recoder_station(x) for x in ids ]
    [ create_datafile(x) for x in ids ]
    [ plot_id(x) for x in ids] 
    
if __name__=="__main__":
    main()
