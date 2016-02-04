import pdb
import os
import pandas as pd
import numpy as np
import pickle
def string_id(intid):
    return '{:0>9}'.format(int(intid))

def get_pat_weeks(pid, dx, rx, px=0):
    hb_dx = dx[pid].strip('|').split('|')
    week_to_diags = {}
    for d in hb_dx:
        dinfo = d.split(':')
        week_to_diags.setdefault(dinfo[2].strip(), set()).add(dinfo[0].strip())
    ret = {'d': week_to_diags}
    if px != 0:
        procedures = px[pid].strip('|').split('|')
        week_to_proc = {}
        for d in procedures:
            dinfo = d.split(':')
            if len(dinfo[0].strip()) > 0:
                week_to_proc.setdefault(dinfo[2].strip(), set()).add(dinfo[0].strip())
        ret['p'] = week_to_proc
    return ret

def pat_week(pat_week_info, week, icd9, cpt):
    print week
    if 'p' in pat_week_info:
        print 'Procedures:' + str(len(pat_week_info['p']))
        print [(d, cpt.loc[d, 'short_description']) for d in pat_week_info['p'][week]]
    print 'Diagnoses:'
    print [(d, icd9.loc[d]) for d in pat_week_info['d'][week]]
    
def get_diagnosis_weeks(pat_week_info, diagnosis_prefix_list):
    #hi
    return [week for week in pat_week_info['d'] if 
            len([wd for wd in d.strip('|').split('|')
                if wd.startswith(diagnosis_prefix_list)]) > 0]


def pat_dx_age_info(pat_dx,diagnosis_prefixes):
    ## list of tuple
    if type(diagnosis_prefixes) == tuple:
        diagnosis_prefix_list = [diagnosis_prefix_list]

    #print pat_dx
    #pdb.set_trace()
    youngest_year = float('inf')
    oldest_year = 0
    youngest_week = float('inf')
    oldest_week = 0

    dx_infos = dict([(d, [float('inf')]*2 + [0]*2) for d in diagnosis_prefixes])
    first_diag_week = float('inf')
    dx_yr = float('inf')
    if len(pat_dx) > 0: 
        for d in pat_dx.strip('|').split('|'):
            dinfo = d.split(':')
            youngest_year = min(int(dinfo[1]), youngest_year)
            oldest_year = max(int(dinfo[1]), oldest_year)
            youngest_week = min(int(dinfo[2]), youngest_week)
            oldest_week = max(int(dinfo[2]), oldest_week)

            for dx_name in diagnosis_prefixes:
                if dinfo[0].startswith(diagnosis_prefixes[dx_name]):
                    # [0 = year, 1 = week]

                    dx_infos[dx_name][0] = min(int(dinfo[1]), dx_infos[dx_name][0]) # young age
                    dx_infos[dx_name][1] = min(int(dinfo[2]), dx_infos[dx_name][1]) # young week
                    dx_infos[dx_name][2] = max(int(dinfo[1]), dx_infos[dx_name][2]) # old age
                    dx_infos[dx_name][3] = max(int(dinfo[2]), dx_infos[dx_name][3]) # old week
    #pdb.set_trace()
    return [dx_infos, youngest_year, oldest_year, youngest_week, oldest_week]

def rx2dataframe(pat_rx):
    pat_rx = pat_rx.strip('|').split('|')
    #pdb.set_trace()
    rx_df = pd.DataFrame([[ri.strip() for ri in r.split(":")]
                          for r in pat_rx if len(r)>0],
                         columns = ['rx','amt','supply','age','week'])
    if rx_df.shape[0] > 0:
        rx_df['rx'] = rx_df['rx'].str.replace("\x00","00")
        rx_df['week'] = rx_df['week'].map(int)
        rx_df['age'] = rx_df['age'].map(int)
        rx_df['supply'] = rx_df['supply'].map(float)
    return rx_df


    
def rx_df(pat, rx, redbook_dict):
    drugs = set()

    try:
        rx_df = rx2dataframe(rx[pat])
        if rx_df.shape[0] == 0:
            return rx_df


        #print pat + ' w ' + str(len(y))
        rx_df['gennme'] = ['NA' if drug not in redbook_dict else redbook_dict[drug][0]
                           for drug in rx_df['rx'] ]
        rx_df.loc[rx_df['supply'] > 180, 'supply'] = 180  ## as per documentation
        rx_df = rx_df.loc[rx_df['gennme']!='NA',:]
        return rx_df
    except KeyError:
        print 'rx_df: rx has no key ' + pat

def drugs_before_given_age(pat, rx, old_age_cut,
                           redbook_dict, young_age_cut):
    drugs = set()
    #youngest_age = float('inf')
    #print '> drugs_before_given_age: ' + pat + ' ' + str(age)
    try:
        for r in rx[pat].strip('|').split('|'):
            if len(r) == 0: continue
            dinfo = r.split(":")
            #youngest_age = min(int(dinfo[3]), youngest_age)
            if int(dinfo[3]) < old_age_cut:
                if young_age_cut == 0 or int(dinfo[3]) >= young_age_cut:
                    drug = dinfo[0].strip().replace("\x00","00")
                    if drug in redbook_dict:
                        drugs.add(redbook_dict[drug][0])
    except KeyError:
        print "!!! drugs_before_given_age: " + pat

    #rint '< drugs_before_given_age: ' + pat                
    return drugs

    
def drug_pat_ages(pat_id_path, marketscan, redbook, diagnosis_prefix_list, fname, ndo=0):
    #marketscan + '/DX_indices/dis_Hereditary_Breast_Cancer_ids.txt'
    home = os.path.expanduser('~') 
    data = home + '/wrk/data/'

    patlist = open(pat_id_path).read().strip().split('\n')
    rx = bd.BeeStringDict(marketscan+'RX', keysize = 9, readonly=True)
    rxset = bd.BeeStringDict(marketscan+'RXSet', keysize = 9, readonly=True)
    dx = bd.BeeStringDict(marketscan+'DX', keysize = 9, readonly=True)
    demo = bd.BeeStringDict(marketscan+'ID_demographics', keysize = 9, readonly=True)
    
    #ndo =  10 #1000 #100000
    if ndo > 0:
        patlist = patlist[:ndo]

    rep = open(fname + 'missing_drug','w')
    gennme = redbook['gennme'].unique()
    
    rx_age = pd.DataFrame(np.full((len(patlist),len(gennme)),float('inf')),
                          columns = gennme, index = patlist)
    dx_age = np.zeros((len(patlist), 3))
    for (i, pat) in enumerate(patlist):
        if i % 50000 == 0:
            print 'done ' + '{:1.2f}'.format(100*(i/float(len(patlist))))
        [first_diag_week, diag_yr, youngest_year, oldest_year] = \
          pat_dx_age_info(pat, dx, diagnosis_prefix_list)

        #pdb.set_trace()
        for r in rx[pat].strip('|').split('|'):
            if len(r) == 0: continue
            dinfo = r.split(":")
            youngest_year = min(int(dinfo[3]), youngest_year)
            oldest_year = max(int(dinfo[3]), oldest_year)

            week = int(dinfo[4])
            if week < first_diag_week:
                if dinfo[0] in redbook.index:
                    drug_info = redbook.loc[dinfo[0].strip().replace("\x00","00"),:]
                    if drug_info['mstfmds']!="Ointment":
                        rx_age.loc[pat, drug_info['gennme']] = min(rx_age.loc[pat,drug_info['gennme']],
                                                                   int(dinfo[3]))
                else:
                    rep.write( dinfo[0] + '\n')
        dx_age[i,:] = [brca_year, youngest_year, oldest_year]
    pkl = open(fname + '.drug_mats.pkl','w')
    pickle.dump((rx_age, dx_age) , pkl)
    pkl.close()
    rx_age = rx_age.loc[:,(rx_age < float('inf')).sum(axis=0) > 50]
    pkl = open(fname + '.filter_drug_mats.pkl','w')
    pickle.dump((rx_age, dx_age) , pkl)
    pkl.close()

def get_rx_BD_AD(pid, dx, rx, diagnosis_prefix_list, redbook, rep):
    first_diag_week = float('inf')
    #pdb.set_trace()
    youngest_year = float('inf')
    oldest_year = 0
    brca_year = float('inf')
    for d in dx[pid].strip('|').split('|'):
        dinfo = d.split(':')
        if dinfo[0].startswith(diagnosis_prefix_list):
        #if dinfo[0] in diagnosis_prefix_list:
            first_diag_week = min(int(dinfo[2]), first_diag_week)
            brca_year = min(int(dinfo[1]), brca_year)
        youngest_year = min(int(dinfo[1]), youngest_year)
        oldest_year = max(int(dinfo[1]), oldest_year)
    bd = {}
    ad = {}
    for r in rx[pid].strip('|').split('|'):
        if len(r) == 0: continue
        dinfo = r.split(":")
        youngest_year = min(int(dinfo[3]), youngest_year)
        oldest_year = max(int(dinfo[3]), oldest_year)

        week = int(dinfo[4])
        toadd = bd
        if week >= first_diag_week:
            toadd = ad
        if dinfo[0] in redbook.index:
            drug_info = redbook.loc[dinfo[0].strip().replace("\x00","00"),:]
            if drug_info['mstfmds']!="Ointment":
                toadd.setdefault(drug_info['gennme'], []).append(dinfo[1].strip())
        else:
            rep.write( dinfo[0] + '\n')
    return first_diag_week, bd, ad, youngest_year, oldest_year, brca_year

