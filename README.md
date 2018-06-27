# Project 2: Twitter Emerging Topic Detection using Apache Spark

COEN 242 - Big Data 

Santa Clara University 

Spring 2018


### Authors 

Immanuel Amirtharaj, Jackson Beadle


Last Edited: June 13, 2018


### Project Description

Spark Streaming project to parse Twitter streams, run semantic analysis using 
Stanford's Core NLP library, and detect emerging topics. 

Topics are defined as hashtags. An emerging topic is the topic with the greatest 
net positive increase of mentions between two windows. The code can be easily 
reconfigured to use a different window duration or sliding duration. Sample output
is provided for emerging topics as detected mid-June 2018. 

The report refers to Spark applications for querying movie data. More information, 
as well as the code, can be found [here](https://github.com/beadlejack/bigdata_spark).
