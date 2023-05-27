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
                                                |_TestDataset|
                                                             |_Test_k_[0.1,0.3,0.5,0.7,1,1.3,1.5,1.7,2].csv (dataset per test di scalabilità)
                                                |
                                                |_Review.csv (oringinal dataset)
                                                |
                                                |_Rev_Parsed.csv (parsed dataset)
                                        |
                                        |__Enrichment
                                                    |
                                                    |_enrich_data.ipynb (notebook per la creazione di dataset aventi grandezze distinte rispetto all'originale)
                                        
                                        
                                        |__Exploration
                                                    |
                                                    |_exploration.ipynb (esplorazione del dataset)
                                                    |
                                                    |__testing.ipynb (test del parsing)
                                        |
                                        |__Job2_____
                                                    |__HIVE
                                                           |__output: cartella di memorizzazione della tabella output del job Hive
                                                           |
                                                           |__comandi.hql: interrogazoni hql usate nel job
                                                           |
                                                           |__inptu_test.txt: file di test
                                                           |
                                                           |__test.ipynb: notebook di tes

                                                    |__MR__|
                                                           |_codes: codici map e reduce usati nel job
                                                           |
                                                            |_output1: output prima coppia map-reduce
                                                            |_output2: output seconda coppia map-reduce
                                                            |        
                                                            |__test  (test locale dei mapper e reducers)
                                                    |
                                                    |__plots: cartella per i plot sull'analisi del risultato
                                        |
                                        |
                                        |__Job 3_
                                                |
                                                |__MR__: 
                                                        |
                                                        |_Analytics: analisi dei risultati e greazione grafo
                                                        |
                                                        |_codici MR
                                                |__MR_test: codici di test MR
                                        |
                                        |
                                        |__Tempi_
                                                |
                                                |_Logica per l'analisi dei tempi di esecuzione
                                        |
                                        |__Utils____
                                                    |
                                                    |__command.txt (genera i comandi linux per l'esecuzione del job)
                                                    |__input_paths.txt (vengono definiti i path di base in formato json)
                                                    |__create_command_line.py (crea i comandi utilizzando l'input 'input_paths.txt' scrivendo su 'command.txt')
