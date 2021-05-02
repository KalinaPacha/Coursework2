hdfs dfs -mkdir /mnt
hdfs dfs -mkdir /mnt/data

hdfs dfs -copyFromLocal reviews.csv /mnt/data/.
hdfs dfs -copyFromLocal users.csv /mnt/data/.
hdfs dfs -copyFromLocal places.csv /mnt/data/.
