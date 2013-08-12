package de.kp.storm.bolt;

import java.io.StringReader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import de.kp.storm.nlp.POSTagger;
import de.kp.storm.nlp.TaggedWord;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class POSTaggerBolt extends BaseBasicBolt{

	private static final long serialVersionUID = 1035222015958967593L;
	private static POSTagger tagger;
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		if (input == null) return;
		
		String sentence = (String)input.getValueByField("sentence");
		StringReader reader = new StringReader(sentence);

		List<TaggedWord> taggedWords = tagger.getTaggedWords(reader);
		
		/*
		 * do further processing with tagged words
		 */

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {   
		
		String modelFile = (String)conf.get("model");
		
		@SuppressWarnings("unchecked")
		HashSet<String> excludedTags = (HashSet<String>)conf.get("excluded");
		
		/*
		 * initialize tagger
		 */
		POSTagger.init(modelFile, excludedTags);
		tagger = POSTagger.getInstance();
		
	}


}
