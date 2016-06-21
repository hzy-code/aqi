#!/usr/bin/python3
# -*- encoding : utf-8 -*-

import os
import conf
import subprocess

def gen_plot_direct(title,fname,dname):
    template= '''set term png size 1800,600
set grid
set title '%s'
set output '%s.png'
set timefmt '%%Y-%%m-%%d %%H:%%M:%%S'
set xdata time
set format x '%%m/%%d %%H:%%M'
plot '%s.txt' u 1:4 title 'aqi' with lines, '%s.txt' u 1:5 title 'pm25' with lines
    '''
    fname=os.path.join(conf.DATA_DIR,fname)
    dname=os.path.join(conf.DATA_DIR,dname)
    return template %(title,fname,dname,dname)
    


def plot(direct):
    popen=subprocess.Popen(['gnuplot'],stdin=subprocess.PIPE,bufsize=1,
            universal_newlines=True)
    popen.stdin.write(direct)
    popen.stdin.close()
    return popen.wait()==0

def main():
    plot(gen_plot_direct('aqi','烟厂','烟厂'))

if __name__ == '__main__':
    main()
