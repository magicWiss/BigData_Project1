#!/usr/bin/env python3

import sys


#Id,ProductId,UserId,ProfileName,HelpfulnessNumerator,HelpfulnessDenominator,Score,Time,Summary,Text,TimeCreation


path_output_mapper="/home/matteowissel/Universita/Bigdata/Progetto1_git/BigData_Project1/Progetto1/Progetto_1/Job3/MR_test/mapper_test.txt"
index_line=0    #skip the headers
USER_ID_COL=2
PRODUCT_COL=1
SCORE_COL=6

with open(path_output_mapper,'w') as f:
    for line in sys.stdin:
            

        line=line.strip()
            

        words=line.split(",")
            
            #contorllo se la riga ha esattamente 10 valori
        if len(words)>1:
            
                #se siamo negli headers allora creo una mappa delle colonne 'Nome colonn->indice' per avere accesso diretto
                
                if index_line==0:
                    index_line+=1
                else:
                    
                    user_id=words[USER_ID_COL]  #userID
                    product_id=words[PRODUCT_COL]#denominatore
                    score=words[SCORE_COL]  #nominatore
                    try:
                        score=float(score)
                    except ValueError:
                        continue
                    

                    
                    #dall'esplorazione dati emerche che se hd==0 allora lo è anche hn
                    #se hd==0 allora value=0
                
                    if score>=4:
                        print("%s\t%s" %(user_id,product_id))
                        f.write("%s\t%s\n" %(user_id,product_id))
                
                    