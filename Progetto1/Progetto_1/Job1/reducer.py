#!/usr/bin/env python3

import sys
import ast
user_to_value={}
path_out_reducer='Job1/output_reducer.txt'

    
for line in sys.stdin:

        line=line.strip()
        
        values=line.split('\t')
        if len(values)==2:

            user_id=values[0]
            utility_str=values[1]
        
        

        
            val=0
            try:
                    val=float(utility_str)
                    

            except ValueError:
                    continue
                    
            
            if user_id not in user_to_value:
                user_to_value[user_id]=[]

            user_to_value[user_id].append(val)

    

    
for k in user_to_value:
        somma=sum(user_to_value[k])
        N=len(user_to_value[k])
        mean_utility=somma/N
        print("%s\t%s" %(k,str(mean_utility)))





