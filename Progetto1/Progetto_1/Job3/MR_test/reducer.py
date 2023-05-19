#!/usr/bin/env python3

import sys
import random
user_to_prduct={}
path_out_reducer="/home/matteowissel/Universita/Bigdata/Progetto1_git/BigData_Project1/Progetto1/Progetto_1/Job3/MR_test/reducer_test.txt"
with open(path_out_reducer,'w') as f:
    
    for line in sys.stdin:

        line=line.strip()
        
        values=line.split('\t')
        user_id=values[0]
        products=values[1]  

        random_numboer=random.randint(0,10)
        if random_numboer==0:
            print("riga",line)
            print("user-id",user_id)
            print("products",products) 
            

        
        
        
        
                
                

                
        
        if user_id not in user_to_prduct:
            user_to_prduct[user_id]=set()         #si è notato come utenti hanno recensito più volte lo stesso prodott. Utilizzo un set per effettuare gia adesso il filtraggio

        user_to_prduct[user_id].add(products)       

    

    
    for k in user_to_prduct:
        
        if len(user_to_prduct[k])>=3:
            f.write("%s\t  %s\n" %(k,str(user_to_prduct[k])))

    



