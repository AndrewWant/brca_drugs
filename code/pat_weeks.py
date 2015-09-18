import pdb

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
            len([wd for wd in pat_week_info['d'][week] 
                if wd.startswith(diagnosis_prefix_list)]) > 0]


def get_rx_BD_AD(pid, dx, rx, diagnosis_prefix_list, redbook, rep):
    first_diag_week = float('inf')
    #pdb.set_trace()
    for d in dx[pid].strip('|').split('|'):
        dinfo = d.split(':')
        if dinfo[0].startswith(diagnosis_prefix_list):
        #if dinfo[0] in diagnosis_prefix_list:
            first_diag_week = min(int(dinfo[2]), first_diag_week)
    bd = {}
    ad = {}
    for r in rx[pid].strip('|').split('|'):
        if len(r) == 0: continue
        dinfo = r.split(":")
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
    return first_diag_week, bd, ad

