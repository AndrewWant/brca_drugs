#!/usr/bin/python
# -*- coding: utf-8 -*-

import multiprocessing
import time

num_procs = 200


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
global drug2set
global clean

import time

clean = False

name2set = {}

first_pass = True
fipscodes = {}
q = multiprocessing.JoinableQueue()
procs = []

import argparse

#------------------------------------------
def nonEmptyIntersection(A, B):
    """
    Returns true if set A intersects set B.
    """
    smaller, bigger = A, B
    if len(B) < len(A):
        smaller, bigger = bigger, smaller
    for e in smaller:
        if e in bigger:
            return True
    return False
#----------------------------------
def do_work((pat_set,num,idd,alle)):
    
    global drug2set
    
    print "work",rich_string_wrap(num2comma2(len(pat_set)) +'*****'+ num2comma2(num),'r',1,'k',0),('%4.3f' % (100.*float(num)/alle)),"started"
    get_cases_drug(drug2set, pat_set, num, idd)
    print "work",rich_string_wrap(num2comma2(len(pat_set)) +'*******'+ num2comma2(num),'r',1,'k',0),"completed"

#-----------------------------------
def worker():
    for item in iter( q.get, None ):
        
        do_work(item)          
        q.task_done()
    q.task_done()


#-----------------------------------
def num2comma2(num):
    
    locale.setlocale(locale.LC_ALL, 'en_US')
    s = locale.format("%d", num, grouping=True)
    return s
    
#=================================================
def read_drug_sets():
    global drug2set
    
    drug2set = {}
    
    import pickle
    drug2set = pickle.load( open( "ndc.p", "rb" ) )
    
    #print sorted(drug2set.keys())
    #raw_input('?')

    return drug2set
    
    
#=================================================
def clean_name(name):
    
    s = name.replace(',','_')
    s = s.replace('(','_')
    s = s.replace(' ','_')
    s = s.replace(')','_')
    s = s.replace('\\','_')
    s = s.replace('/','_')
    s = s.replace(':','_')
    s = s.replace('*','_')
    s = s.replace('\"','_')
    s = s.replace('\'','_')
    s = s.replace('<','_')
    s = s.replace('>','_')
    s = s.replace('|','_')
    clean_name = s
    
    return clean_name
#=================================================
def get_cases_drug(drug2set,pat_set,num,idd):
    
    t00 = time.time()	
    t0 = time.time()
     
    for drug in drug2set.keys():
        d_set = drug2set[drug]
        
        if nonEmptyIntersection(d_set, pat_set):
            with open('drug_'+clean_name(drug)+'.txt','a') as f:
                f.write(idd+'\n')
                f.close()
        

        
    return
# -------------------------------------------------


    
# -------------------------------------------------
def main():

    global drug2set
    
    drug2set = read_drug_sets()
    clean = False
    
    names = []
    for name in sorted(drug2set.keys()):
        imya = name.replace(" ","_")
        names.append(imya)
    
    system('clear')
        
    from mx.BeeBase import BeeDict as bd
    rx_set = bd.BeeStringDict('../RXset', keysize = 9, readonly=1)
    all_pats = sorted(rx_set.keys())
        
    print num2comma2(len(all_pats))
            
if __name__ == '__main__':
    main()
