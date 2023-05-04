#!/usr/bin/env python3

import sys





index_line=0    #skip the headers

userId_col='UserId'
helpfulnessDenominator_col='HelpfulnessDenominator'
helpfulnessNumerator_col='HelpfulnessNumerator'
map_col_index={}
def create_map_col(words):
    map_col_index={}
    i=0
    for w in words:
        map_col_index[w]=i
        i+=1
    return map_col_index

for line in sys.stdin:
        

    line=line.strip()
        

    words=line.split(",")
        
        #contorllo se la riga ha esattamente 10 valori
    if len(words)==10:
        
            #se siamo negli headers allora creo una mappa delle colonne 'Nome colonn->indice' per avere accesso diretto
            
            if index_line==0:
                map_col_index=create_map_col(words)
                print("%s"%(str(map_col_index)))
                
                index_line+=1
            else:
                
                user_id=words[2]  #userID
                hd=words[5]#denominatore
                hn=words[4]  #nominatore
                try:
                    hd=float(hd)
                    hn=float(hn)
                except ValueError:
                    continue
                

                
                #dall'esplorazione dati emerche che se hd==0 allora lo è anche hn
                #se hd==0 allora value=0
               
                if hd==0:
                    value=0
                else:
                    value=hn/hd
                

                print("%s\t%f" %(user_id,value))
                
                    