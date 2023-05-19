#!/usr/bin/env python3

import sys
import random
group_to_products={}
path_out_reducer="/home/matteowissel/Universita/Bigdata/Progetto1_git/BigData_Project1/Progetto1/Progetto_1/Job3/MR/reducer2_test.txt"

    
for line in sys.stdin:

        line=line.strip()
        
        values=line.split('\t')
        user_id=values[1]
        products=eval(values[0])  
        #trasformo in un frozen_set per essere utilizzato come key della mappa temporanea
        products_fs=frozenset(products)

        
            

        #se è il primo elemento
        if len(group_to_products)==0:
            group_to_products[products_fs]=[user_id] 
        
        else:
            inserted=0
            #logica di inserimento
            #Dato il nuovo elemento N
            #Data una chiave K
            #Inserisco uN in uK se: N==K or N contiene K 
            #Se non esiste un elemento uguale a N va inserita la entry
            for k in group_to_products.keys():

                if products_fs==k:
                    inserted=1
                    group_to_products[k].append(user_id)
                
                if len(products_fs)>len(k):
                    intersection=products_fs & k
                    if len(intersection)>=3:
                        group_to_products[k].append(user_id)
                

            if inserted==0:
                group_to_products[products_fs]=[user_id]
        
for k in group_to_products:
            
        
            print("%s\t  %s" %(str(group_to_products[k]),str(list(k))))

    



