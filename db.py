#!/usr/bin/python3
# -*- coding: utf8 -*-

import MySQLdb

conn=MySQLdb.connect(host='localhost',user='hzy',passwd='hzyzxj',charset='utf8',db='aqi')
cur=conn.cursor()

if __name__=='__main__':
    testsql='select * from station order by id'
    cur.execute(testsql)
    res=cur.fetchall()
    print("station:")
    print("=================")
    [print(x) for x in res]
