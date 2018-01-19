# spark_prac
Practice learning Apache Spark

#EMR Cluster

When creating a new cluster on emr, make sure to check security groups (need inbound rules to point to your ip address on port 22)

#Start movie similarity on cluster need 1g memory
spark-submit --executor-memory 1g MovieSimilarities1M.py 260

