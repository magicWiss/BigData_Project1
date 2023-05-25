#!/usr/bin/env python3

import sys


#Id,ProductId,UserId,ProfileName,HelpfulnessNumerator,HelpfulnessDenominator,Score,Time,Summary,Text,TimeCreation


path_output_mapper="/home/matteowissel/Universita/Bigdata/Progetto1_git/BigData_Project1/Progetto1/Progetto_1/Job3/MR_test/mapper2_test.txt"
index_line=0    #skip the headers
USER_ID_COL=2
PRODUCT_COL=1
SCORE_COL=6
conta_uguali=0
conta_diversi=0
with open(path_output_mapper,'w') as f:
    for line in sys.stdin:
            

        line=line.strip()
            

        words=line.split("\t")
            
            #contorllo se la riga ha esattamente 10 valori
        if len(words)>1:
            
                #se siamo negli headers allora creo una mappa delle colonne 'Nome colonn->indice' per avere accesso diretto
                
                
                    
                    user_id=words[0]
                    product_string_list=words[1]
                    product_set=eval(product_string_list)
                    
                    
                   


                    
                    
                    

                    
                    #dall'esplorazione dati emerche che se hd==0 allora lo è anche hn
                    #se hd==0 allora value=0
                
                    
                    f.write("%s\t%s\n" %(str(product_set),user_id))

                
                    