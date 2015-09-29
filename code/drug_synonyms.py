import pickle
import pdb
import os

def load_for_synonyms(list_pkl):  #fdadrugs
    list_of_lists, target = pickle.load(open(list_pkl))
    get_target_synonyms(list_of_lists, target, list_pkl.split('.')[0])
    
def get_target_synonyms(dict_of_syn_lists, target, pklname=''):  #fdadrugs
    dlists_upper = dict([(dlist, set([d.upper() for d in dict_of_syn_lists[dlist]]))
                    for dlist in dict_of_syn_lists])
    drug_syns = dict([(dlist, dict(zip(dlists_upper[dlist],
                          [set() for x in range(len(dlists_upper[dlist]))])))
                 for dlist in dlists_upper])
    #drug_syns = [(set(lists_upper[i]),
    #             dict(zip(lists_upper[i], [set() for x in range(len(lists_upper[i]))])))
    #             for i in range(len(lists_upper))]
    pubchem = open(os.path.expanduser('~') + '/wrk/data/med_annotations/drugs/CID-Synonym-filtered')

    
    id = '0'
    curlist = []
    i = 1
    report = open(pklname + 'rep','w')
    for line in pubchem:
        num_name = line.strip().split('\t')
        if num_name[0] != id:
            targlist = set(curlist) & target
            if len(targlist) > 0:
                #print 'checking' + id
                for listname in drug_syns:
                    for ixn in set(curlist) & dlists_upper[listname]:
                        drug_syns[listname][ixn] |= set(targlist)
                        report.write( 'adding ' + str(targlist) + ' to ' + str(ixn) + '\n')
                '''
                for (keyset, syndict) in drug_syns:
                    for ixn in set(curlist) & keyset:
                        #pdb.set_trace()
                        syndict[ixn] |= set(targlist)
                        report.write( 'adding ' + str(targlist) + ' to ' + str(ixn) + '\n')
                '''
            curlist = [num_name[1].upper()]
            id = num_name[0]
        else:
            curlist += [num_name[1].upper()]
        #i += 1
        #if i > 100000:
        #    break
    report.close()

    syns = open(pklname + 'synonyms.pkl','w')
    pickle.dump(drug_syns, syns)
    syns.close()
    return drug_syns

if __name__ == '__main__':
    import sys
    load_for_synonyms(sys.argv[1])
