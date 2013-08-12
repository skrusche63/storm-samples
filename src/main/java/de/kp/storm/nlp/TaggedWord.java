package de.kp.storm.nlp;

public class TaggedWord {

	private String word;
	private String tag;

	public TaggedWord(String word, String tag) {
		this.word = word;
		this.tag  = tag;
	}
	
	public String getWord() {
		return word;
	}
	
	public String getTag() {
		return tag;
	}
}
