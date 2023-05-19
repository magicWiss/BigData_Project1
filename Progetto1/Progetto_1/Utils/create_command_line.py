#! /usr/bin/env python3
import json

path_input="BigData_Project1/Progetto1/Progetto_1/Utils/input_paths.json"

base_commands={
    "putLoc2Hdfs":"$HADOOP_HOME/bin/hdfs dfs -put ",
    "getHdfs2Loc":"$HADOOP_HOME/bin/hdfs dfs -get ",
    "chmodMapp":"chmod a+x",
    "run":"$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.3.4.jar",
    "remove":"$HADOOP_HOME/bin/hdfs dfs -rm -r ",
    "move": "$HADOOP_HOME/bin/hdfs dfs -mv " 
    }

# Opening JSON file
f = open(path_input)
  
# returns JSON object as 
# a dictionary
data = json.load(f)
  


run_rutine1=base_commands['run']+" -mapper "+data["mapper1"]+" -reducer "+data["reducer1"]+" -input "+data["hdfs_input1"]+" -output "+data["hdfs_output1"]

move_output=base_commands['move'] + " " + data['move_out_file'] + " " +data["hdfs_input2"]

remove_out=base_commands["remove"]+ " "+data["hdfs_output1"]


run_rutine2=base_commands['run']+" -mapper "+data["mapper2"]+" -reducer "+data["reducer2"]+" -input "+data["hdfs_input2"]+" -output "+data["hdfs_output2"]


commands_series=[run_rutine1,move_output,remove_out,run_rutine2]



path_commnads=data["output_commands_path"]

with open(path_commnads,"w+") as f:
    f.write("time")
    for i in commands_series:
        f.write(i+" | ")
