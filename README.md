# ELT Data Processing Project 

## In this project, data will be downloaded from S3 buckets and load into the staging tables. All copied data will then tranform and load into the Star Schema tables created on previous step. By combining the datasets, data analysts can perform the queries to get insightful data in a more effectively.

## I will use Apache Spark to process my data. Apache Spark is a in-memory processing engine for big data processing which is 10 times faster than traditional Hadoop MapReduce proecessing because it provides high-level operators such as 'map' to process data on run-time instead of read/write from disk.

## Spark supports distributed computing. I will create cluster using Amazon EMR by running follow command.
        aws emr create-cluster --name mySpark \
        --use-default-roles --release-label emr-5.28.0  \
        --instance-count 3 --applications Name=Spark Name=Zeppelin  \
        --bootstrap-actions Path="s3://bootstrap.sh" \
        --ec2-attributes KeyName=SparkEMR \
        --instance-type m5.xlarge --log-uri s3:///emrlogs/

## With instance count of 3, I will have one master cluster and two worker clusters. 

# Submit python script the Apache Spark EMR clusters

1. Create and download the IAM user crediential with s3 and Spark EMR permission

2. Submit the script to spark to run it
        spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.0 etl.py

# Song Dataset
 * Song data: s3://udacity-dend/song_data 
        {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

 * Log data: s3://udacity-dend/log_data 
        {"artist": null,"auth": "Logged In","firstName": "Walter","gender": "M","itemInSession": 0,"lastName": "Frye","length": null,"level": "free","location": "San Francisco-Oakland-Hayward, CA","method": "GET","page": "Home","registration": 1540919166796.0,
        "sessionId": 38,"song": null,"status": 200,"ts": 1541105830796,"userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId": "39"}

# ETL Steps

1. Download  dataset to a local machine.
2. Upload a file into an S3 location using the AWS S3 console, or you can use the AWS CLI command, like `aws s3 cp <your current file location>/<filename> s3://<bucket_name>`.
3. Create an EMR instance.
4. Copy the python script to the home directory of EMR instance.
5. Execute the file using `spark-submit <filename>.py`.


# Star table schema

Fact Table Reference

    songplays - records in event data associated with song plays i.e. records with page NextSong

        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables Reference

    users - users in the app

        user_id, first_name, last_name, gender, level

    songs - songs in music database

        song_id, title, artist_id, year, duration

    artists - artists in music database

        artist_id, name, location, lattitude, longitude

    time - timestamps of records in songplays broken down into specific units

        start_time, hour, day, week, month, year, weekday

