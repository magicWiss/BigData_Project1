#! /usr/bin/bash
echo 'Rev_Parsed'
{ time(hive --f "HIVE/comandi.hql" --hiveconf input_path file:///home/hadoop/prog1/Dataset/Rev_Parsed.csv); } 2> output/hiveoutputRP.txt
echo '1'
{ time(hive --f "HIVE/comandi.hql" --hiveconf input_path file:///home/hadoop/prog1/Dataset/Test_k_1.csv ); } 2> output/hiveoutput1.txt 
echo '2'
{ time(hive --f "HIVE/comandi.hql" --hiveconf input_path file:///home/hadoop/prog1/Dataset/Test_k_2.csv ); } 2> output/hiveoutput2.txt 
echo '01'
{ time(hive --f "HIVE/comandi.hql" --hiveconf input_path file:///home/hadoop/prog1/Dataset/Test_k_01.csv ); } 2> output/hiveoutput01.txt 
echo '03'
{ time(hive --f "HIVE/comandi.hql" --hiveconf input_path file:///home/hadoop/prog1/Dataset/Test_k_03.csv ); } 2> output/hiveoutput03.txt 
echo '05'
{ time(hive --f "HIVE/comandi.hql" --hiveconf input_path file:///home/hadoop/prog1/Dataset/Test_k_05.csv ); } 2> output/hiveoutput05.txt 
echo '07'
{ time(hive --f "HIVE/comandi.hql" --hiveconf input_path file:///home/hadoop/prog1/Dataset/Test_k_07.csv ); } 2> output/hiveoutput07.txt 
echo '13'
{ time(hive --f "HIVE/comandi.hql" --hiveconf input_path file:///home/hadoop/prog1/Dataset/Test_k_13.csv ); } 2> output/hiveoutput13.txt 
echo '15'
{ time(hive --f "HIVE/comandi.hql" --hiveconf input_path file:///home/hadoop/prog1/Dataset/Test_k_15.csv ); } 2> output/hiveoutput15.txt 
echo '17'
{ time(hive --f "HIVE/comandi.hql" --hiveconf input_path file:///home/hadoop/prog1/Dataset/Test_k_17.csv ); } 2> output/hiveoutput17.txt 