# ELT Data Processing Project 

## In this project, I will use Apache Spark to process my data. Apache Spark is a in-memory processing engine for big data processing which is 10 times faster than traditional Hadoop MapReduce proecessing because it provides high-level operators such as 'map' to process data on run-time instead of read/write from disk.

## Spark supports distributed computing. I will create cluster using Amazon EMR by running follow command.
        aws emr create-cluster --name mySpark \
        --use-default-roles --release-label emr-5.28.0  \
        --instance-count 3 --applications Name=Spark Name=Zeppelin  \
        --bootstrap-actions Path="s3://bootstrap.sh" \
        --ec2-attributes KeyName=SparkEMR \
        --instance-type m5.xlarge --log-uri s3:///emrlogs/

## With instance count of 3, I will have one master cluster and two worker clusters. 

