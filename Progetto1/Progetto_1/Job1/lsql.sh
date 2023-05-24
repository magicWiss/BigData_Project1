#! /usr/bin/bash
echo 'Rev_Parsed'
{ time(spark-submit --master local codes/SPARKSQL/job1.py --input_path file:///home/hadoop/prog1/Dataset/Rev_Parsed.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/sqloutputRP.txt && rm -r SPARK/output.csv
echo '1'
{ time(spark-submit --master local codes/SPARKSQL/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_1.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/sqloutput1.txt && rm -r SPARK/output.csv
echo '2'
{ time(spark-submit --master local codes/SPARKSQL/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_2.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/sqloutput2.txt && rm -r SPARK/output.csv
echo '01'
{ time(spark-submit --master local codes/SPARKSQL/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_01.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/sqloutput01.txt && rm -r SPARK/output.csv
echo '03'
{ time(spark-submit --master local codes/SPARKSQL/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_03.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/sqloutput03.txt && rm -r SPARK/output.csv
echo '05'
{ time(spark-submit --master local codes/SPARKSQL/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_05.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/sqloutput05.txt && rm -r SPARK/output.csv
echo '07'
{ time(spark-submit --master local codes/SPARKSQL/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_07.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/sqloutput07.txt && rm -r SPARK/output.csv
echo '13'
{ time(spark-submit --master local codes/SPARKSQL/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_13.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/sqloutput13.txt && rm -r SPARK/output.csv
echo '15'
{ time(spark-submit --master local codes/SPARKSQL/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_15.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/sqloutput15.txt && rm -r SPARK/output.csv
echo '17'
{ time(spark-submit --master local codes/SPARKSQL/job1.py --input_path file:///home/hadoop/prog1/Dataset/Test_k_17.csv --output_path file:///home/hadoop/prog1/job2/SPARK/output.csv); } 2> output/sqloutput17.txt && rm -r SPARK/output.csv