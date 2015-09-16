#!/usr/bin/env python

import math as mt
import sys
import time
import gc
idd
from help import back2num,fore2num,\
                 fore2num2,back2num2,rich_string_wrap

import locale
from mx.BeeBase import BeeDict as bd
import argparse
import copy as copy_mod

#-------------------------
#-------------------------
def num2comma2(num):
    
    locale.setlocale(locale.LC_ALL, 'en_US')
    s = locale.format("%d", num, grouping=True)
    return s
#-----------------------------------------------------------
#----------------------------
def main():
    
    
    rx = bd.BeeStringDict('RX', keysize = 9, readonly=1)    
    rx_set = bd.BeeStringDict('RXset', keysize = 9, readonly=0)    
    
    count =-1
    empty = 0
    
    keys = sorted(rx.keys())
    print len(keys)

    for key in keys:
        
        line = rx.get(key,'')
        
        if len(line) > 9:
        
       
            subs = line.split('|')
            codes= []
            for sub in subs:
                code = sub.split(':')[0]
                if len(code) > 4:
                    codes.append(code)
                
            #print codes
            rx_set[key] = set(codes)
            rx_set.commit()
        else:
            empty += 1
        
        
        count+=1
        if count % 10000 == 0:
            print  rich_string_wrap(num2comma2(count),'y',0,'k',0), \
            rich_string_wrap(num2comma2(empty),'r',0,'k',0), ('%4.3f ' % (float(empty+0.5)/float(count+0.5)))

if __name__ == '__main__':
    main()
