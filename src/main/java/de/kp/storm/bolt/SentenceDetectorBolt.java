package de.kp.storm.bolt;

import java.util.Map;

import de.kp.storm.nlp.SentenceDetector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SentenceDetectorBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -1156496754182154906L;
	private SentenceDetector detector;

	private OutputCollector collector;
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		if (input == null) return;
		
		/*
		 * detect sentences from incoming text
		 */
		String text = (String)input.getValueByField("message");
		String[] sentences = detector.getSentences(text);

		/*
		 * emit sentences for further processing
		 */
		int len = sentences.length;
		if (len == 0) return;
		
		for (int i=0; i < len; i++) {
			this.collector.emit(new Values(sentences[i]));
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {   
		
		String modelFile = (String)conf.get("model");
		
		/*
		 * initialize sentence detector
		 */
		SentenceDetector.init(modelFile);
		detector = SentenceDetector.getInstance();
		
		this.collector = collector;
		
	}

}
