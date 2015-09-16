#!/usr/bin/python
# -*- coding: utf-8 -*-

import multiprocessing

num_procs = 24


from pylab import *
import random as rn
import pandas as pd
import math
import scipy.stats.distributions
import scipy.optimize
from scipy.stats import hypergeom
import scipy.stats
import cProfile
import math
import copy as copy_mod
import numpy as np
from scipy.stats import chi2
from help import back2num,fore2num,fore2num2,\
                 back2num2,rich_string_wrap
            
from os import system
import locale

age_edges = [4,10,30,50]
bug = 0
global fipscodes
global first_pass
global name2set 
global clean

import time

clean = False

name2set = {}

first_pass = True
fipscodes = {}
q = multiprocessing.JoinableQueue()
procs = []

import argparse


#----------------------------------
def do_work((message,num)):

    print "work",rich_string_wrap(message +'_'+ str(num),'r',1,'k',0),"started"
    get_cases_phe(name2set, message, num)
    print "work",rich_string_wrap(message +'_'+ str(num),'r',1,'k',0),"completed"

#-----------------------------------
def worker():
    for item in iter( q.get, None ):
        
        do_work(item)          
        q.task_done()
    q.task_done()


#------------------------------------
def age2str(age):
    
    greater = ''
    less = ''
    maxa = -1
    mina = 1000
    for a in age_edges:
        
        if age > a and a > maxa:
            maxa = a
            greater = '>'+str(maxa)+'_'
        if age <= a and a < mina:
            mina = a
            less = '<='+str(mina)
    return greater+less
    
#-----------------------------------
# convert a number to a string with commas
def num2comma2(num):
    
    locale.setlocale(locale.LC_ALL, 'en_US')
    s = locale.format("%d", num, grouping=True)
    return s
#===================================================
def read_new_phenotypes():
    
    global name2set2
    global names
    name2set2 = {}
    names = []
    num = 0
    
    data = pd.read_table('FinalPhenotypes_new.txt',sep='\t')
#    print data
        
    for i in range(len(data['Disease_Summary_Name'])):
                
        tmp = set([])
        
        name = data['Disease_Summary_Name'][i]
        codes = data['ICD9 Codes'][i].split('|')
        names.append(name)
                #print codes
        for j in range(len(codes)):
            tmp = tmp | set([codes[j]])
                 
        name2set2[name]=copy_mod.deepcopy(tmp)
    
    return name2set2
#===================================================
#=================================================
def get_cases_phe(name2set,phe,num):
    
    t00 = time.time()	
    t0 = time.time()
            
    
    pat = 0
    saved = 0
    
    tmp = copy_mod.deepcopy(age_edges)
    tmp.append(75)
    #print tmp
    
    get_fipscodes()
    tmp.insert(0, -1)
    tmp.append(80)
        
         
    infile = open('CASES_DX.txt', 'r')
    bug = 0
    
    files = {}
    buffers = {}
    counters = {}
    maxbuff = 10000000
    
    files[phe] = open(phe+'_ids.txt','w')
    counters[phe] = 0
    tmp = np.zeros([maxbuff,1],int)
    buffers[phe] = tmp
    saved = 0
    
    #^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    while infile:
        
        #***********  EACH LINE IS A PATIENT ***********
        slices = infile.readline().split('^')
        
        if len(slices[0]) == 0:
            break
        if pat % 100000 == 0:
            t1 = time.time()
            
            print rich_string_wrap(str(phe)+'_'+str(num),'w',1,'k',0), saved,\
                  rich_string_wrap(str(num2comma2(pat)), 'r', 1, 'k', 0), \
                  rich_string_wrap(num2comma2(t1-t00)+' s','r',1,'k',0), \
                  rich_string_wrap('%0.1f' % ((t1-t00)/60.)+' m','c',1,'k',0), \
                  rich_string_wrap('%0.1f' % ((t1-t00)/3600.)+' h','m',1,'k',0), \
                  rich_string_wrap(num2comma2(t1-t0)+' ds','y',1,'k',0)
            t0 = time.time()

        #*****************************
        pat_sick = 0
        #*****************************
        
        
        if len(slices) > 1:
            
            pat+=1            
            line = slices[0].split('|')
             
            if len(line) > 5:
                
                try:
                    ids = line[0].rstrip()
                    fips = line[4].split(',')[0].rstrip()
                    sex = line[3][0].rstrip()
                    tmpstr = line[1].lstrip()
                    if len(tmpstr) > 0:
                        init_age = int(tmpstr)
                    else:
                        init_age = 0
                    first_year = int(line[5].rstrip())
                    
                    #print init_age, sex, fips, first_year
                except:
                    print "Unexpected error:", sys.exc_info()[0]
                    #raise               
                    fips = '0000'
                    bug +=1
                    #print line, slices[0]
                
                line1 = slices[1].rstrip()
                if len(line1) > 0 and line1[len(line1)-1] == '|':
                    line1 = line1[0:len(line1)-1]
                elif len(line1) > 0 and line1[len(line1)-2:len(line1)] == '|\\':
                    line1 = line1[0:len(line1)-1]
                    slices = infile.readline().rstrip()
                    if slices[len(slices)-1] == '|':
                        slices = slices[0:len(slices)-1]
                    line1 += slices
                elif len(line1) == 0:
                    fips = '0000'
                    
                line = line1.split('|')
            
                if len(line) > 0:
                
                        
                     for j in range(len(line)):
                                
                        code = line[j].split(':')
                        failed = 0
                        err = ''
                        
                        if len(code) ==3:
                                 
                            try:
                                icode = code[0].rstrip()
                                
                                if len(icode) > 0 and icode[len(icode)-1] == '.':
                                    icode = code[0][0:len(icode)-1].lstrip()
                                    
                            except:
                                print "Unexpected error:", sys.exc_info()[0]
                                
                                icode = 'xxx'
                                print '+'+code[0]+'+'
                                failed = 1
                                err += '**icode**'
                            try:
                                stmp = code[1].rstrip().lstrip()
                                if len(stmp) > 0:
                                    iage = int(stmp)
                            except:
                                print "Unexpected error:", sys.exc_info()[0]
                                raise               
                                
                                iage = 1
                                err += 'iage '
                            
                            if pat_sick == 0 and len(set([icode]) & name2set[phe]) > 0:
                                    
                                    
                                pat_sick = 1
                                if counters[phe] == maxbuff:
                                    for i in range(counters[phe]):
                                        files[phe].write('%09d\n' % buffers[phe][i])
                                    counters[phe] = 0
                                         
                                counters[phe] += 1
                                buffers[phe][counters[phe]-1] = int(ids)
                                saved+=1
                                    

                        else:
                            bug += 1
                                                          
        else:
            print rich_string_wrap(str(pat),'r',1,'k',1), slices
            
    if counters[phe] != 0:
        for i in range(counters[phe]):
            files[phe].write('%09d\n' % buffers[phe][i])
            
    files[phe].close()
    
    infile.close()
        
    return
# -------------------------------------------------


#==================================================
def get_fipscodes():
    
    global fipscodes
    fipscodes = {}
    from arf_dict import county
    
    
    for key in county.keys():
        fipscodes[key[0]] = 1
    
    return
    
# -------------------------------------------------
def main():

    global name2set
    global clean
    global names
    clean = False
    
    if len(sys.argv) < 1:
        print rich_string_wrap(
        'Usage: python mp_index_only.py  [-c]', 'r', 1, 'k', 0)
        print '!!!!!!'
        sys.exit(0)

    else:
        
        #system('clear')
        
        parser = argparse.ArgumentParser(description='Codes')
        parser.add_argument('-c','--clean', help=rich_string_wrap('Clean [False/True]','y',1,'k',0),required=False)
        args = parser.parse_args()
        if args.clean is not None:
            clean = bool(args.clean)
            print ("Clean: %s" % clean)               
        
        name2set = read_new_phenotypes()
        
        ndis = len(name2set.keys())
        #names = name2set.keys()
        
        
        for i in range(num_procs):
            procs.append( multiprocessing.Process(target=worker) )
            procs[-1].daemon = True
            procs[-1].start()

            num = 0
            for item in names:
                if num < ndis:
                    num+=1
                    q.put((item,num))
                else:
                    break

        if num < ndis:
            q.join()

        for p in procs:
            q.put( None )

        q.join()

        for p in procs:
            p.join()

        print "Finished everything...."
        print "num active children:", multiprocessing.active_children()
                
if __name__ == '__main__':
    main()
