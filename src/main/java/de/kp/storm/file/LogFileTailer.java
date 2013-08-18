package de.kp.storm.file;

import java.io.File;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;

public class LogFileTailer {

    /*
     * The amount of time to wait for the file to be updated.
     */
    private long delay;

    private String fname;
    private Tailer tailer;
    
    /*
     * Socket port
     */
    private int port;
    
    public LogFileTailer(String fname) {
    	this(fname, 500, 5555);
    }
    
    public LogFileTailer(String fname, int port) {
    	this(fname, 500, port);
    }
    
    public LogFileTailer(String fname, long delay, int port) {
    	this.fname = fname;
    	this.delay = delay;
    }
    
	public void run() {

		File file = new File(this.fname);
		TailerListener listener = new LogFileListener(port);
		
		tailer = Tailer.create(file, listener, delay);
		
		// Alternative method using executor:
		// tailer = new Tailer(file, listener, delay);
		
		// Executor executor ...
		// executor.execute(tailer);
		 
		 
		// Alternative method if you want to handle the threading yourself:
		// tailer = new Tailer(file, listener, delay);
		// Thread thread = new Thread(tailer);
		// thread.setDaemon(true); // optional
		// thread.start();

		

	}

	public void stop() {
		tailer.stop();
	}

}
