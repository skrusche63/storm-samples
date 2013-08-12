package de.kp.storm.nlp;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;

public class SentenceDetector {

	private static SentenceDetector instance;
	private SentenceDetectorME detector;
	
	private SentenceDetector(String modelFile) {

		InputStream modelIS = null;

		try {
			modelIS = new FileInputStream(modelFile);
			SentenceModel model = new SentenceModel(modelIS);

			detector = new SentenceDetectorME(model);

		} catch (IOException e) {
			e.printStackTrace();

		} finally {

			if (modelIS != null) {
				try {
					modelIS.close();
				}
				catch (IOException e) {
				}
			}

		}		

	}

	public static void init(String modelFile) {
		instance = new SentenceDetector(modelFile);
	}
	
	public static SentenceDetector getInstance() {
		return instance;
	}

	/**
	 * This method retrieves a String Array, each item
	 * representing a detected sentence
	 * 
	 * @param text
	 * @return
	 */
	public String[] getSentences(String text) {
		return detector.sentDetect(text);		
	}

}
