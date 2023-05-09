#!/usr/bin/env python3

import sys

path_output_mapper='Progetto1/Progetto_1/Job2/MR/test/output_mapper.txt'
def create_map_col(words):
    map_col_index={}
    i=0
    for w in words:
        map_col_index[w]=i
        i+=1
    return map_col_index


index_line=0    #skip the headers

userId_col='UserId'
helpfulnessDenominator_col='HelpfulnessDenominator'
helpfulnessNumerator_col='HelpfulnessNumerator'
map_col_index={}
with open(path_output_mapper,'w') as f:

    for line in sys.stdin:
        

        line=line.strip()
        

        words=line.split(",")
        
        #contorllo se la riga ha esattamente 10 valori
        if len(words)==10:
        
            #se siamo negli headers allora creo una mappa delle colonne 'Nome colonn->indice' per avere accesso diretto
            
            if index_line==0:
                map_col_index=create_map_col(words)
                
                index_line+=1
            else:
                
                user_id=words[map_col_index[userId_col]]  #userID
                hd=words[map_col_index[helpfulnessDenominator_col]]#denominatore
                hn=words[map_col_index[helpfulnessNumerator_col]]  #nominatore
                try:
                    hd=float(hd)
                    hn=float(hn)
                except ValueError:
                    continue
                

                
                #dall'esplorazione dati emerche che se hd==0 allora lo è anche hn
                #se hd==0 allora value=0
                f.write(str(words)+'\n')
                f.write("ID\t\tVALUES\tNN\tND+\n")
                if hd==0:
                    value=0
                else:
                    value=hn/hd
                

                print("%s\t%s"%(user_id,str(value)))
                f.write("%s\t%s\t%s\t%s\n" %(user_id,str(value),str(hn),str(hd)))
                    