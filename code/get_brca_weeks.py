from mx.BeeBase import BeeDict as bd
import pandas as pd
from itertools import chain
data = '/Volumes/Macintosh HD-1/Users/Data/'
data = '/Users/Data/'
px = bd.BeeStringDict(data+'PX', keysize = 9, readonly=True)
rx = bd.BeeStringDict(data+'RXset', keysize = 9, readonly=True)
dx = bd.BeeStringDict(data+'DX', keysize = 9, readonly=True)
demo = bd.BeeStringDict(data+'ID_demographics', keysize = 9, readonly=True)


from pat_weeks import *

import billing_codes
[icd9, cpt] = billing_codes.load_icd9_cpt()
import time
t0 = time.time()
bcpat = open('ab').read().strip().split('\n')
ndo = 0 #100000
import numpy as np
lens = np.zeros((len(bcpat), 5))
for (i, pat) in enumerate(bcpat):
    wkinfo = get_pat_weeks(pat, dx, px)
    wnums = [int(w) for w in wkinfo['d'].keys()]
    brca_diagnos_wk = [int(w) for w in get_diagnosis_weeks(wkinfo, ('174','239.3','238.3','233'))]
    lens[i,:] = [len(wkinfo['d']), len(wkinfo['p']), max(wnums) - min(wnums), max(wnums), sum(np.array(wnums) >= min(brca_diagnos_wk))]
    if i % 50000 == 0:
        print 'done ' + '{:1.2f}'.format(100*(i/float(len(bcpat)))) #+ ' @ ' + str(i)
    if ndo > 0 and i > ndo - 2: break

if ndo > 0:
    lens = lens[:ndo,:]    
import pickle
pkl = open('brca_week_diagnosiswks.pkl','w')
pickle.dump((bcpat, lens), pkl)
pkl.close()
t1 = time.time()
