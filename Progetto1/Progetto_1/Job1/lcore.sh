#! /usr/bin/bash
echo 'Rev_Parsed'
{ time(spark-submit --master local codes/SPARK/job1.py --input_path file:///home/hadoop/prog1/Dataset/Rev_Parsed.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/coreoutputRP.txt && rm -r SPARK/output.csv
echo '1'
{ time(spark-submit --master local codes/SPARK/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_1.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/coreoutput1.txt && rm -r SPARK/output.csv
echo '2'
{ time(spark-submit --master local codes/SPARK/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_2.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/coreoutput2.txt && rm -r SPARK/output.csv
echo '01'
{ time(spark-submit --master local codes/SPARK/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_01.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/coreoutput01.txt && rm -r SPARK/output.csv
echo '03'
{ time(spark-submit --master local codes/SPARK/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_03.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/coreoutput03.txt && rm -r SPARK/output.csv
echo '05'
{ time(spark-submit --master local codes/SPARK/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_05.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/coreoutput05.txt && rm -r SPARK/output.csv
echo '07'
{ time(spark-submit --master local codes/SPARK/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_07.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/coreoutput07.txt && rm -r SPARK/output.csv
echo '13'
{ time(spark-submit --master local codes/SPARK/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_13.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/coreoutput13.txt && rm -r SPARK/output.csv
echo '15'
{ time(spark-submit --master local codes/SPARK/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_15.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/coreoutput15.txt && rm -r SPARK/output.csv
echo '17'
{ time(spark-submit --master local codes/SPARK/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_17.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/coreoutput17.txt && rm -r SPARK/output.csv