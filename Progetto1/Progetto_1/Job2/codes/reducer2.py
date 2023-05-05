#!/usr/bin/env python3

import sys
import ast
user_to_value={}
path_out_reducer='Job2/output_reducer.txt'

    
for line in sys.stdin:

        line=line.strip()
        
        values=line.split('\t')

        
  
        if len(values)==2:
            user_id=values[0]
            mean_utility=values[1]   

            try:
                    val=float(mean_utility)
                    

            except ValueError:
                    continue
                    
            
            if user_id not in user_to_value:
                user_to_value[user_id]=0

            user_to_value[user_id]=val

user_to_value=dict(sorted(user_to_value.items(),key=lambda x: x[1],reverse=True))

    
for k in user_to_value.keys():
        
        print("%s\t%s" %(k,str(user_to_value[k])))


