package de.kp.storm.nlp;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

public class POSTagger {

	private static POSTagger instance;
	private HashSet<String> excludedTags;

	private POSTaggerME tagger;
	
	private POSTagger(String modelFile, HashSet<String> excludedTags) {

		InputStream modelIn = null;

		try {

			modelIn = new FileInputStream(modelFile);
			POSModel model = new POSModel(modelIn);
		
			tagger = new POSTaggerME(model);
			if (excludedTags != null) this.excludedTags = excludedTags;
			
		} catch (Exception e) {
			e.printStackTrace();

		} finally {

			if (modelIn != null) {
				try {
					modelIn.close();
				}
				catch (IOException e) {
				}
			}
		
		}

	}

	public static void init(String modelFile) {
		init(modelFile, null);
	}
	
	public static void init(String modelFile, HashSet<String> excludedPOS) {
		instance = new POSTagger(modelFile, excludedPOS);
	}
	
	public static POSTagger getInstance() {
		return instance;
	}

	public String[] getTags(String[] words) {
		return tagger.tag(words);
	}

	public List<TaggedWord> getTaggedWords(StringReader reader) {

		List<TaggedWord> taggedWords = new ArrayList<TaggedWord>();
		
		StreamTokenizer streamTokenizer = null;
		ArrayList<String> rawWords = new ArrayList<String>();

		try {

			streamTokenizer = new StreamTokenizer(reader);
			while (streamTokenizer.nextToken() != StreamTokenizer.TT_EOF) {

				if (streamTokenizer.ttype == StreamTokenizer.TT_WORD)
					rawWords.add(streamTokenizer.sval);

			}

		} catch (IOException ex) {
			ex.printStackTrace();
		}
		
		String[] tags  = tagger.tag(rawWords.toArray(new String[rawWords.size()]));
		for (int i=0; i < tags.length; i++) {
			
			String tag = tags[i];
			if ((excludedTags != null) && excludedTags.contains(tag))
				continue; 
		
			taggedWords.add(new TaggedWord(rawWords.get(i), tags[i]));
		
		}
		
		return taggedWords;
		
	}

}
