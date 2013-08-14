storm-samples
=============

This repository holds a collection of storm topologies, we have made experience with at 
[Dr. Krusche & Partner](http://www.dr-kruscheundpartner.de). We share the respective Spout & Bolt 
definitions with the hope that they are useful.

Storm is an open source real-time computation framework, which was developed at Twitter and is sometimes referred 
to as "real-time Hadoop." Whereas Hadoop relies on batch processing, Storm is a real-time, distributed, fault-tolerant system. 

Like Hadoop, it can process huge amount of data, but does so in real time with guaranteed reliability. This means, that 
every incoming message will be processed. 

Storm also offers features such as fault tolerance and distributed computation, which make it suitable for processing 
huge amounts of data on different machines.

### Natural Language Processing
This project references the Apache OpenNLP library for the processing of natural language text. Actually it supports
* Sentence Segmentation
* Part of Speech Tagging

_to be continued_

### FileSpout
This spout operates on a single text file and emits each line for further processing. It is a simple example of how 
to acquire data for a storm network (or topology).

_to be continued_

### MySQLBolt

_to be continued_

### POSTaggerBolt

_to be continued_

### NERBolt
This bolt uses the GATE text mining system for Named Entity Recognition. GATE is invoked by using a socket connetion to 
send raw text and receive XML documents from GATE describing the entities found.

