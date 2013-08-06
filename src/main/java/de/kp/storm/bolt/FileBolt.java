package de.kp.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class FileBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 192532070443534548L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		
		String line = (String)input.getValueByField("line");
		/*
		 * Do anything with the respective line of processed file
		 */
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
