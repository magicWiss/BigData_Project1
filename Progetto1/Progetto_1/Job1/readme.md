# JOB 1
## Consegna 
Un job che sia in grado di generare, per ciascun anno, i 10 prodotti che hanno ricevuto il maggior numero di recensioni e, per ciascuno di essi, le 5 parole con almeno 4 caratteri più frequentemente usate nelle recensioni (campo text), indicando, per ogni parola, il numero di occorrenze della parola.

## Comandi
 -  ## Map Reduce
        $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.3.4.jar \
        -mapper codes/MR/mapper.py \
        -reducer codes/MR/reducer.py \
        -input /prog1/Dataset/Rev_Parsed.csv \
        -output /prog1/job1/output/MR/MR1 && \
        $HADOOP_HOME/bin/hdfs dfs -cat /prog1/job1/output/MR/MR1/* > output.txt && \
        $HADOOP_HOME/bin/hdfs dfs -put output.txt /prog1/job1/output/MR/MR1/ && \
        $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.3.4.jar \
        -reducer codes/MR/reducer2.py \
        -input /prog1/Dataset/Rev_Parsed.csv \
        -output /prog1/job1/output/MR/MR2 \
        -mapper codes/MR/mapper2.py \
        --cacheFile '/prog1/job1/output/MR/MR1/output.txt#output' && \
        $HADOOP_HOME/bin/hdfs dfs -cat /prog1/job1/output/MR/MR2/* > output_finale.txt

    Vengono eseguiti due job MapReduce in maniera sequenziale. Il primo produce una lista di ProductId - year che il secondo job utilizzerà per filtrare le review di interesse.

 -  ## Hive
        hive --f 'path/to/file/'
       verificare il path del file csv nello script hql

 -  ## Spark SQL
       $SPARK_HOME/bin/spark-submit \
              --master local \
              codes/SPARKSQL/job1.py \
              --input_path file:///home/federico/BD/BigData_Project1/Progetto1/Progetto_1/Dataset/Rev_Parsed.csv

 -  ## Spark Core
       $SPARK_HOME/bin/spark-submit \
              --master local \
              codes/SPARK/job1.py \
              --input_path hdfs:///prog1/Dataset/Rev_Parsed.csv \
              --output_path hdfs:///prog1/job1/output/SPARK