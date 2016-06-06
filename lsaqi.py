#!/usr/bin/env python3
# -*- encoding: utf8 -*-

import time
import datetime
import urllib
import urllib.request
import json

import db

def get_station_id_list():
    ids=list()
    sql="SELECT id FROM station ORDER BY id"
    db.cur.execute(sql)
    res=db.cur.fetchall()
    [ ids.append(x[0]) for x in res ]
    return ids

def get_last_time(station_id):
    sql="SELECT MAX(rec_time) FROM st_aqi WHERE station_id="+str(station_id)
    db.cur.execute(sql)
    res=db.cur.fetchall()
    try:
        return res[0][0].timetuple()
    except:
        return None

# is lastest time
def is_latest(t):
    if type(t)==datetime.datetime:
        stamp=t.timestamp()
    elif type(t)==time.struct_time:
        stamp=time.mktime(t)
    elif type(t)==int:
        stamp=t
    else:
        raise TypeError('the timetype must be time.struct_time timestamp or datetime.datetimr')
    now=time.time()
    st=time.localtime(now)
    if now-stamp > st.tm_min*60+st.tm_sec:
        return False
    return True

# get the json for station
def get_json(url):
    res=urllib.request.urlopen(url)
    return res.readall()

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

# parse json and return aqi data
def parse_json(txt):
    try:
        aqi_info=dict()
        obj=json.loads(txt)
        data=obj['Head'][0]
        aqi_info['aqi']=int(data['AQI'])
        aqi_info['time']=data['CREATE_DATE']
        aqi_info['pm25']=float(data['PM25'])
        aqi_info['pm25aqi']=int(data['PM25IAQI'])
        return aqi_info
    except:
        return None

# read aqi info from www.zzemc.cn
def download_aqiinfo(station_id):
    url=get_url(station_id)
    data=get_json(url)
    aqi_info=parse_json(data.decode('utf-8'))
    return aqi_info

# read aqi info from db
def get_aqi_info(station_id):
    last_time=get_last_time(station_id)
    if last_time is None:
        return None
    str_time=time.strftime('%Y-%m-%d %H:%M:%S',last_time)
    sql='SELECT rec_time,station_name,aqi,pm25aqi,pm25 FROM st_aqi WHERE station_id=%d AND rec_time=\'%s\'' %(station_id,str_time)
    db.cur.execute(sql)
    res=db.cur.fetchall()
    t=res[0][0]
    if not is_latest(t):
        aqi_info=download_aqiinfo(station_id)
        if aqi_info is not None:
            aqi_info['name']=res[0][1]
            return aqi_info
    aqi_info=dict()
    aqi_info['name']=res[0][1]
    aqi_info['time']=time.strftime('%Y-%m-%d %H:%M:%S',res[0][0].timetuple())
    aqi_info['aqi']=res[0][2]
    aqi_info['pm25aqi']=res[0][3]
    aqi_info['pm25']=res[0][4]
    return aqi_info

def ls_aqi(station_id):
    aqi_info=get_aqi_info(station_id)
    if aqi_info is None:
        return
    line='%-20s%10s%4s%5d%5d%8.4f' %(aqi_info['time'],aqi_info['name'],get_aqi_level(aqi_info['aqi']),aqi_info['aqi'],aqi_info['pm25aqi'],aqi_info['pm25'])
    print(line)

def get_aqi_level(aqi):
    if aqi<=50 :
        return '优'
    elif aqi<=100:
        return '良'
    elif aqi<=150:
        return '轻'
    elif aqi<=200:
        return '中'
    elif aqi<=300:
        return '重'
    else:
        return '严'

def ls_head():
    head='%-20s%10s%4s%5s%5s%8s' %('日期','探测站','等级','AQI','Pm25','Pm25')
    print(head)
    print('---------------------------------------------------------')

def main():
    ids=get_station_id_list()
    ls_head()
    [ ls_aqi(x) for x in ids]

if __name__=="__main__":
    main()

