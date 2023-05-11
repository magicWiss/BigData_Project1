# JOB 1
## Consegna 
Un job che sia in grado di generare, per ciascun anno, i 10 prodotti che hanno ricevuto il maggior numero di recensioni e, per ciascuno di essi, le 5 parole con almeno 4 caratteri più frequentemente usate nelle recensioni (campo text), indicando, per ogni parola, il numero di occorrenze della parola.

## Comandi
 -  ## Map Reduce
        $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.3.4.jar \
        -mapper codes/MR/mapper.py \
        -reducer codes/MR/reducer.py \
        -input /prog1/Dataset/Rev_Parsed.csv \
        -output /prog1/job1/output/mr1 && \
        $HADOOP_HOME/bin/hdfs dfs -cat /prog1/job1/output/mr1/* > output.txt && \
        $HADOOP_HOME/bin/hdfs dfs -put output.txt /prog1/job1/output/mr1/ && \
        $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.3.4.jar \
        -reducer codes/MR/reducer2.py \
        -input /prog1/Dataset/Rev_Parsed.csv \
        -output /prog1/job1/output/mr2 \
        -mapper codes/MR/mapper2.py \
        --cacheFile '/prog1/job1/output/mr1/output.txt#output' && \
        $HADOOP_HOME/bin/hdfs dfs -cat /prog1/job1/output/mr2/* > output_finale.txt

    Vengono eseguiti due job MapReduce in maniera sequenziale. Il primo produce una lista di ProductId - year che il secondo job utilizzerà per filtrare le review di interesse.

 -  ## Hive
        hive --f 'path/to/file/'