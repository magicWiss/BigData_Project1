#! /usr/bin/bash

scp -i ~/.ssh/pem/fb_bd_2023.pem -r Job1/* hadoop@ec2-3-216-31-6.compute-1.amazonaws.com:~/progetto1/job1
scp -i ~/.ssh/pem/fb_bd_2023.pem -r Job2/*.sh hadoop@ec2-3-216-31-6.compute-1.amazonaws.com:~/progetto1/job2
scp -i ~/.ssh/pem/fb_bd_2023.pem -r Job2/HIVE/comandi.hql hadoop@ec2-3-216-31-6.compute-1.amazonaws.com:~/progetto1/job2/HIVE/comandi.hql
scp -i ~/.ssh/pem/fb_bd_2023.pem -r Job2/MR/codes/* hadoop@ec2-3-216-31-6.compute-1.amazonaws.com:~/progetto1/job2/MR
scp -i ~/.ssh/pem/fb_bd_2023.pem -r Job2/SPARK/* hadoop@ec2-3-216-31-6.compute-1.amazonaws.com:~/progetto1/job2/SPARK