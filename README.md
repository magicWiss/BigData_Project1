# BigData_Project1

# STRUTTURA HDFS
root
    |__prog1
            |_Dataset
                    |_Reviews.csv (originale)
                    |_Rev_Parsed.csv (parsato)
            |
            |_job1
                |_____output1: output del primo blocco mr
                |_____ output2: output del seconod blocco mr
            |
            |
            |_job2
                |
                |_____output


# STRUTTURA REPO
BigData_Project1|
                |
                __Progetto1
                           |
                           |__Progetto_1
                                        |
                                        |_Dataset
                                                |
                                                |_Review.csv (oringinal dataset)
                                                |
                                                |_Rev_Parsed.csv (parsed dataset)
                                        |
                                        |__Exploration
                                                    |
                                                    |_exploration.ipynb (esplorazione del dataset)
                                                    |
                                                    |__testing.ipynb (test del parsing)
                                        |
                                        |__Job1_____
                                                    |
                                                    |__codes (mappers and reducers)
                                                            |
                                                            |_output1:output prima coppia map-reduce
                                                            |_output2: output seconda coppia map-reduce
                                                    |        
                                                    |__test  (test locale dei mapper e reducers)
                                        |
                                        |__Utils____
                                                    |
                                                    |__command.txt (genera i comandi linux per l'esecuzione del job)
                                                    |__input_paths.txt (vengono definiti i path di base in formato json)
                                                    |__create_command_line.py (crea i comandi utilizzando l'input 'input_paths.txt' scrivendo su 'command.txt')
