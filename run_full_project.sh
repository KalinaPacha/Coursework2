echo "Running HDFS Load"
sh hdfs_data_load.sh

echo "Running HIVE table load"
hive -f hive_data_load.hql

echo "Running Functionality - 1"
sh functionality1.sh 5.0

echo "Running Functionality - 2"
sh functionality2.sh

echo "Running Functionality - 3"
sh functionality3.sh

echo "Running Functionality - 4"
cat func-4.scala | $SPARK_HOME/bin/spark-shell

echo "Running Functionality - 5"
sh functionality5.sh

echo "Running Functionality - 6"
sh functionality6.sh

echo "Running Functionality - 7"
cat func-7.scala | $SPARK_HOME/bin/spark-shell

echo "Running Functionality - 8"
cat func-8.scala | $SPARK_HOME/bin/spark-shell

echo "Running Functionality - 9"
cat func-9.scala | $SPARK_HOME/bin/spark-shell

echo "Running Functionality - 10"
cat func-10.scala | $SPARK_HOME/bin/spark-shell

echo "Running Functionality - 11"
cat func-11.scala | $SPARK_HOME/bin/spark-shell

echo "Running Functionality - 12"
cat func-12.scala | $SPARK_HOME/bin/spark-shell

echo "Running Functionality - 13"
cat func-13.scala | $SPARK_HOME/bin/spark-shell

echo "Running Functionality - 14"
cat func-14.scala | $SPARK_HOME/bin/spark-shell

echo "Running Functionality - 15"
cat func-15.scala | $SPARK_HOME/bin/spark-shell
