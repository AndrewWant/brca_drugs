import subprocess
from mx.BeeBase import BeeDict as bd


import multiprocessing
import pdb
import time
import Queue
import os
import sys

home = os.path.expanduser('~') 
data = home + '/wrk/data/'
#data = '/Volumes/melamed/wrk/data/'
marketscan = data + 'marketscan/'
annot_path = data + 'med_annotations/'

def process_pat_worker(in_queue, out_queue, plock,
                       process_function, db_list, **kwargs):
    for db in db_list:
        kwargs[db] = bd.BeeStringDict(db_list[db][0] + db,
                                      keysize = db_list[db][1], readonly=True)
    #people_dx = bd.BeeStringDict('kanix_matches_F_Breast_Cancer.txt_demo_bd',
    #                          keysize = 9, readonly=True)
    #kwargs['person_dx_info'] = people_dx ### BADDD
    #rx = bd.BeeStringDict(marketscan+'RX', keysize = 9, readonly=True)
    #dx = bd.BeeStringDict(marketscan+'DX', keysize = 9, readonly=True)
    #demo = bd.BeeStringDict(marketscan+'ID_demographics', keysize = 9, readonly=True)
    #rxset = bd.BeeStringDict(marketscan+'RXSet', keysize = 9, readonly=True)
    #dx = bd.BeeStringDict(marketscan+'DX', keysize = 9, readonly=True)

    ## iter = reads until get "None"
    for (patline, index, tot_pat) in iter( in_queue.get, None ):
        if index % 50000 == 0:
            plock.acquire()
            print 'AT {:1.2f} %\n'.format(index/float(tot_pat)*100)
            sys.stdout.flush()
            plock.release()

        result = process_function(patline, **kwargs)
        if not result is None:
            out_queue.put(result)
            
    out_queue.put( None )
    plock.acquire()
    print 'child: is in q empty? ' + str(in_queue.empty())
    plock.release()
    #return 0

def fill_worker(to_iter, in_queue, out_queue, nprocs, ndo, plock):
    plock.acquire()
    print 'fill_worker: file= doing' + str(ndo)
    plock.release()

    #
    #match_case_control = open(match_file)
    i = 0
    #line = match_case_control.readline()
    #while line:
    for line in to_iter:
        if not line or (i > ndo and ndo > 0): break
        try:
            in_queue.put((line, i, ndo), block=False)
            #line = match_case_control.readline()
            i += 1
            #print 'line =' + line.strip() + ' line?' + str(line==True)
        except Queue.Full:
            time.sleep(5)
    for p in range(nprocs):
        in_queue.put( None )
    out_queue.put( None )
    plock.acquire()
    print 'exit filler'

    plock.release()
    

def start_workers(num_procs, process_function, pat_file, ndo, dblist, **kwargs):
        ### Now 
    plock = multiprocessing.Lock()
    in_queue = multiprocessing.Queue()

    out_queue = multiprocessing.Queue()


    # start queue-filling job
    procs = [multiprocessing.Process(target = fill_worker,
                                     args = (pat_file, in_queue, out_queue,
                                              num_procs - 1, ndo, plock))]
    procs[0].start()

    t0 = time.time()

    # start queue-emptying jobs
    for i in range(num_procs - 1):
        procs.append( multiprocessing.Process(target=process_pat_worker,
                                              args=(in_queue, out_queue,
                                                    plock, process_function, dblist),
                                            kwargs=kwargs)) 
        #**kwargs) ))
        procs[-1].start()

    return in_queue, out_queue, plock
