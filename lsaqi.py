#!/usr/bin/env python3
# -*- encoding: utf8 -*-

import time

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


# read aqi info from db
def get_aqi_info(station_id):
    last_time=get_last_time(station_id)
    if last_time is None:
        return None
    str_time=time.strftime('%Y-%m-%d %H:%M:%S',last_time)
    sql='SELECT rec_time,station_name,aqi,pm25aqi,pm25 FROM st_aqi WHERE station_id=%d AND rec_time=\'%s\'' %(station_id,str_time)
    db.cur.execute(sql)
    res=db.cur.fetchall()
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
    ls_head()
    ids=get_station_id_list()
    [ ls_aqi(x) for x in ids]

if __name__=="__main__":
    main()

