#! /usr/bin/env python3
import json

path_input="Progetto1/Progetto_1/Utils/input_paths.json"

base_commands={
    "putLoc2Hdfs":"$HADOOP_HOME/bin/hdfs dfs -put ",
    "getHdfs2Loc":"$HADOOP_HOME/bin/hdfs dfs -get ",
    "chmodMapp":"chmod a+x",
    "run":"$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.3.4.jar",
    "remove":"$HADOOP_HOME/bin/hdfs dfs -rm -r "
    }

# Opening JSON file
f = open(path_input)
  
# returns JSON object as 
# a dictionary
data = json.load(f)
  
put_local_file_2_HDFS=base_commands["putLoc2Hdfs"]+data["local_input"]+" "+data["hdfs_input"]

get_output_file_2_local=base_commands['getHdfs2Loc']+data['hdfs_output']+" "+data["local_output"]

run_rutine=base_commands['run']+" -mapper "+data["mapper"]+" -reducer "+data["reducer"]+" -input "+data["hdfs_input"]+" -output "+data["hdfs_output"]

change_vis_mapper=base_commands["chmodMapp"]+" "+data["mapper"]

change_vis_reducer=base_commands["chmodMapp"]+" "+data["reducer"]

remove_out=base_commands["remove"]+ " "+data["hdfs_output"]


commands_series=[put_local_file_2_HDFS,change_vis_mapper,change_vis_reducer,run_rutine,get_output_file_2_local,remove_out]



path_commnads=data["output_commands_path"]

with open(path_commnads,"w+") as f:
    for i in commands_series:
        f.write(i+"\n")
