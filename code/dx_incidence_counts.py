from mx.BeeBase import BeeDict as bd
import pandas as pd
import numpy as np
import pickle
from itertools import chain
import Queue
import os
import sys
import time

#home = os.path.expanduser('~') 
#data = home + '/wrk/data/'
#data = '/Volumes/melamed/wrk/data/'
marketscan = '/Users/data/'
import pdb

import multiprocessing

### age brackets..
agebins = np.array([0,20] + range(25,80,5) + range(80,100,10) )


def time_ranges(timear, age):
    return [min(timear[0],age),max(timear[1],age)]

def make_outdict(icd_codes):
    return {d:{a:0 for a in agebins}
            for d in set(chain.from_iterable(icd_codes.values())) | set(['total'])}

def do_county(dx, county_ids, code2dx):
    out = make_outdict(code2dx)
    age_bin_counts = {a:0 for a in agebins}

    for person in county_ids:

        person_str = '{:0>9}'.format(int(person))
        agerange = [float('inf'),0]
        dx_min_max = {}   
        alldx = dx[person_str]
         
        ### People with no DX info are not counted toward anything (Age or Total)
        if len(alldx) == 0: continue
        for dx_visit in alldx.strip('|').split('|'):
            if len(dx_visit)==0: continue
            [code, age,week] = [ri.strip() for ri in dx_visit.split(":")]
            agerange = time_ranges(agerange, int(age))
            try:
                for phe in code2dx[code]:
                    disease_ages = dx_min_max.setdefault(phe,
                                                        [float('inf'),0])
                    dx_min_max[phe] = time_ranges(disease_ages, int(age))
            except KeyError:
                continue
        ## for each DX, record min & max age.  Assume person has DX for all ages betwen
        #   so a DX made at age 18 and 28 will count towards 0-20,20-25, and 25-30
        for dx_age in dx_min_max:
            entrybin = agebins[np.max(np.where(dx_min_max[dx_age][0] >= agebins)):(np.max(np.where(dx_min_max[dx_age][1] >= agebins)) + 1)] 
            for ab in entrybin:
                out[dx_age][ab] += 1
                
        ## and record overall min & max age.  Same coding rule as for DX ages.
        entrybin = agebins[np.max(np.where(agerange[0] >= agebins)):(np.max(np.where(agerange[1] >= agebins)) + 1)]
        for ab in entrybin:
            out['total'][ab] += 1
    return out

def process_wrapper(in_queue, out_queue, plock, disease_file, ndo=0):
    dx = bd.BeeStringDict(marketscan+'DX', keysize = 9, readonly=True)
    idx = bd.BeeStringDict(marketscan+'indices', keysize = 51, readonly=True)
    code2dx = pickle.load(open(disease_file))

    for (county, index, tot_cty) in iter( in_queue.get, None ):
        if index % 50000 == 0:
            plock.acquire()
            print 'AT {:1.2f} %\n'.format(index/float(tot_cty)*100)
            sys.stdout.flush()
            plock.release()
        ret = {}

        ## Do DX for M and F separately
        for gender in ['M','F']:
            county_ids = set()
            try:
                county_ids = idx[gender + ' ' + county]
            except KeyError:
                print 'indices does not have key: F ' + county
                continue 
            if ndo > 0:
                county_ids = set(list(county_ids)[:ndo])
            ret[gender] = do_county(dx, county_ids, code2dx)
        if len(ret.keys()) > 0:
            out_queue.put((county, ret))
    out_queue.put(None)
    
def fill_counties(nprocs, in_queue, plock, ndo=0):
    from arf_dict import county
    county = pd.DataFrame(county,index=['value']).stack(level=0).reset_index(level=0,drop=True).index.values
    if ndo==0:
        ndo = len(county)
    plock.acquire()
    print 'fill_counties: tot = ' + str(ndo)
    plock.release()
    i= 0
    while True:
        try:
            in_queue.put((county[i], i, ndo), block=False)
            i += 1
            if i > ndo: break
        except Queue.Full:
            time.sleep(5)
    for p in range(nprocs):
        in_queue.put( None )

def wrap_main(args):
    import traceback
    try:
        main(*args)
    except:
        print "FATAL: reader({0}) exited while multiprocessing".format(*args) 
        traceback.print_exc()

            
def main(disease_file, ndo=0):
    
    import subprocess

    t0 = time.time()
    #rxset = bd.BeeStringDict(marketscan+'RXSet', keysize = 9, readonly=True)

    num_procs = 25
    in_queue = multiprocessing.Queue()
    out_queue = multiprocessing.Queue()
    plock = multiprocessing.Lock()
    
    procs = [multiprocessing.Process(target=fill_counties,
                                              args=(num_procs, in_queue, plock, ndo))]
    procs[0].start()
    for i in range(num_procs - 1):
        procs.append( multiprocessing.Process(target=process_wrapper,
                                              args=(in_queue, out_queue, 
                                                    plock, disease_file, ndo))) 
        procs[-1].start()
        
    fsave = 'dx_prevalences.' + disease_file.split('.')[0] + '.' + str(ndo)
    ## Create header for output file
    code2dx = pickle.load(open(disease_file))    
    od = make_outdict(code2dx)
    tmp = pd.DataFrame(od).unstack().reset_index().transpose().rename(index={'level_0':'dx','level_1':'age'}).iloc[:2,:]
    hdr = pd.concat((tmp,tmp),axis=1)
    hdr.loc['gender',:] = ['F']*tmp.shape[1] + ['M']*tmp.shape[1]
    hdr.to_csv(fsave,sep='\t', header=False)

    ### now get ready to write a line for each county
    fh = open(fsave,'a')
    done_signals = 0
    while done_signals < num_procs - 1: #len(multiprocessing.active_children())>0 and
        out = out_queue.get(timeout=3000)
        if out is None:
            done_signals += 1
            continue
        [county, counts] = out
        counts = list(pd.DataFrame(counts['F']).unstack()) + list(pd.DataFrame(counts['M']).unstack())
        fh.write(county + '\t' + '\t'.join([str(x) for x in counts]) + '\n')
            
    fh.close()
    plock.acquire()
    print '!!!!! GOT DONES = '
    plock.release()

    t1 = time.time()
    print "Finished everything.... SEC=" + '{:1.2f}'.format(t1 - t0)


if __name__ == '__main__':
    ndo = 0
    if len(sys.argv) > 1: ndo = int(sys.argv[2])
    wrap_main(sys.argv[1], # match file
            ndo)
