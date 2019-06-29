# kafka-twitter-project

After you are done with your kafka installation, follow below 4 steps in your terminal to get started with your kafka-twitter-project.

Step 1: First of all you need to start server using below command inside your kafak bin  directory, in my case it is  kafka_2.12-2.2.0/bin

~/Aman/kafka/kafka_2.12-2.2.0$ kafka-server-start.sh config/server.properties 

Step 2: Then you need to start zookeeper server inside your kafka bin directory as shown below

~/Aman/kafka/kafka_2.12-2.2.0$ bin/zookeeper-server-start.sh config/zookeeper.properties  

Step 3: Create a kafka-topic (twitter-tweets) to which producer is going to write the tweets as shown below

kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic twitter_tweets --partitions 3 --replication-factor 1

Verify the creation of the topics with the below command:

kafka-topics.sh --zookeeper localhost:2181 --topic twitter-tweets --describe

Step 4: Create a kafka-topic (important-tweets) to which kafka-stream application is going to write the filtered tweets as shown below

kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic important_tweets --partitions 3 --replication-factor 1

Verify the creation of the topics with the below command:

kafka-topics.sh --zookeeper localhost:2181 --topic important-tweets --describe




This maven project has 3 main modules:

1) kafka-twitter-producer

This module has TwitterProducer.java file, which has a kafka client producer which reads data from a twitter api and push the data to a kafka topic(twitter-tweets) inside a kafka-cluster having brokers.


2) kafka-streams

This modules has KafkaStream.java file which reads tweets in real-time that is being written by kafka-producer in the topic (twitter-tweets), filters out the tweets of those authors who have more than 1000 followers (this filteration can be customized according to use case needs) and the write the filtered tweets back to a new kafka topic (important-tweets).


3) kafka-twitter-consumer

This module has ElasticSearchConsumer.java file, which has kafka client consumer which reads tweets from kafka topic (important-tweets) and send them to an elastics search database (bonsai.io).


Topics:

-> Main topic to which twitter producer is writting tweets is : twitter-tweets
-> Kafka topic to which filtered records are being written and those tweets are being send to Elastic search by Kafka consumer is : important-tweets


Consumer Group ID : kafka-elastic-search
kafka-streams Application ID : kafka-streams-twitter-1


In this project I am dumping the filtered tweets into an open source Elastic Search DB called Bonsai.io

Since in this project I am using kafka standalone cluster on my local machine so by defalut it comes with just a single Broker inside the kafka cluster.
So all the topics that we create and their resepct parition will reside on the same single broker with the defalut replication factor for the topics set to 1.
So this kind of a system is good for learning and skill enhancement purpose but it is not used in a real world scenario, since it in not fault tolerant.







 


