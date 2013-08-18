package de.kp.storm.file;

import org.apache.commons.io.input.TailerListenerAdapter;
import org.zeromq.ZMQ;

/*
 * A TailerListener that sends a detected file line
 * to a 0MQ pull client
 */
public class LogFileListener extends TailerListenerAdapter {

	private ZMQ.Context zeroMqContext;
	private ZMQ.Socket socket;

	public LogFileListener(int port) {
		super();
		
		/*
		 * Initialize ZMQ server
		 */
		zeroMqContext = ZMQ.context(1);
        socket = zeroMqContext.socket(ZMQ.PUSH);
 
        String url = "tcp://*:" + port;
        socket.bind(url);

	}
	
	/* (non-Javadoc)
	 * @see org.apache.commons.io.input.TailerListenerAdapter#handle(java.lang.String)
	 */
	public void handle(String line) {
		/*
		 * Push detected file line 
		 */
		socket.send(line.getBytes(), 0);
		
	}
	
 }
