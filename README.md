storm-samples
=============

This repository holds a collection of storm topologies, we have made experience with at 
[Dr. Krusche & Partner](http://www.dr-kruscheundpartner.de). We share the respective Spout & Bolt 
definitions with the hope that they are useful. Actually our main interest is in real-time text analysis using libraries 
such as Storm and Spark.

**Storm**

Storm is an open source real-time computation framework, which was developed at Twitter and is sometimes referred 
to as "real-time Hadoop." Whereas Hadoop relies on batch processing, Storm is a real-time, distributed, fault-tolerant system. 

Like Hadoop, it can process huge amount of data, but does so in real time with guaranteed reliability. This means, that 
every incoming message will be processed. 

Storm also offers features such as fault tolerance and distributed computation, which make it suitable for processing 
huge amounts of data on different machines.

**Shark**

Spark is an open source cluster computing system that aims to make data analytics fast — both fast to run and fast to 
write. To this end, Spark provides primitives for in-memory cluster computing. A job can load data into memory and 
query it repeatedly much more quickly than with disk-based systems like Hadoop MapReduce.

This repository here focuses on Storm.

### Natural Language Processing
This project references the Apache OpenNLP library for the processing of natural language text. Actually it supports
* Sentence Segmentation
* Part of Speech Tagging

_to be continued_

### Named Entity Recognition
This project uses the GATE text mining system for real-time (supervised) Named Entity Recognition. To this end a specific 
NERBolt is defined that connects to the GATE system through a socket connection.

_to be continued_

### FileSpout
This spout operates on a single text file and emits each line for further processing. It is a simple example of how 
to acquire data for a storm network (or topology).

_to be continued_

### ZMQPullSpout
This spout retrieves messages from a ZeroMQ PUSH server. We use the ZMQPullSpout in combination with a web server 
(which is the mentioned PUSH server) to process postings of a certain forum in real-time. The main objective associated 
with a forum entry is Named Entity Recognition to detect referenced electronic products.

![ZMQPullSpout](https://raw.github.com/skrusche63/storm-samples/master/src/main/resources/ZMQPullSpout.png)

_to be continued_

### MySQLBolt

_to be continued_

### POSTaggerBolt
This bolt uses the Apache Open NLP library to annotate a text line (sentence) with part of speech tags. The result is a 
list of TaggedWords describing a word associated with its detected POS tag.

The bolt may be configured by a set of POS tags to be excluded from the result list.

_to be continued_

### NERBolt
This bolt uses the GATE text mining system for Named Entity Recognition. GATE is invoked by using a socket connetion to 
send raw text and receive XML documents from GATE describing the entities found. The image below illustrates the idea behind 
this bolt.

![NER Bolt Overview](https://raw.github.com/skrusche63/storm-samples/master/src/main/resources/NERBolt.png)

