import pandas as pd
import pickle

def load_icd9_cpt():
    icd9 = pd.read_table('marketscan/cms_tab/icd9_code_lookup.csv',sep=',',dtype=str)
    icd9dd = icd9.drop_duplicates(subset='icd9_code_id', take_last=True)
    icd9dd.index = icd9dd['icd9_code_id']
    icd9dd = icd9dd.loc[:,'description']
    icd9 = icd9dd

    cpt_v = pd.read_table('marketscan/cms_tab/hcpc_code_lookup.csv',sep=',',dtype=str)
    cptdd = cpt_v.loc[:,['hcpc_code_id','short_description','long_description']].drop_duplicates(subset='hcpc_code_id', take_last=True)
    cptdd.index = cptdd['hcpc_code_id']
    cpt = cptdd.drop(['hcpc_code_id'],1)

    pkl = open('icd9_cpt.pkl','w')
    pickle.dump((icd9,cpt), pkl);
    pkl.close()
    return icd9, cpt

def open_icd9_cpt():
    return pickle.load(open('icd9_cpt.pkl'))
