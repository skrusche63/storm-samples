package de.kp.storm.spout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileSpout extends BaseRichSpout {

	private static final long serialVersionUID = -4400263505149897785L;

	private SpoutOutputCollector collector;
	private String fname;

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {

		fname = (String) conf.get("file");
		this.collector = collector;		

	}

	public void nextTuple() {
		
		BufferedReader reader;
		String line = null;

		try {
			
			reader = new BufferedReader(new FileReader(fname));
			while ((line = reader.readLine()) != null)  {
              collector.emit(new Values(line));         
			}

			reader.close();

		} catch (Exception e) {
			e.printStackTrace();
			
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

}