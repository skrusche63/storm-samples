package de.kp.storm.bolt;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/*
 * A sample Bolt for Named Entity Recognition using the GATE
 * text mining system wrapped by a socket server
 */
public class NERBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -8743510615133653434L;
	private int port;
	
	public void execute(Tuple input, BasicOutputCollector collector) {

		/*
		 * acquire text line from previous spout or bolt
		 */
		String line = (String) input.getValueByField("line");

		try {
			
			/*
			 * create a connection to the socker server
			 */			
			InetAddress host = InetAddress.getLocalHost();
			Socket socket = new Socket(host.getHostName(), port);

			/*
			 * send a message
			 */
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			oos.writeObject(line);
			
			/*
			 * receive a message
			 */
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			String message = (String) ois.readObject();

			ois.close();
			oos.close();

			socket.close();
			
			/*
			 * when using the GATE socker server for Named Entity Recognition the
			 * message returned is an XML document of the following structure
			 * 
			 * GATE is configured to find product names with in respective forum posts
			 * 
			 * <?xml version='1.0' encoding='MacRoman'?>
			 *  <GateDocument version="2">
			 *  
			 *    <!-- The document's features-->
			 *  
			 *    <GateDocumentFeatures>
			 *      <Feature>
			 *        <Name className="java.lang.String">gate.SourceURL</Name>
			 *        <Value className="java.lang.String">created from String</Value>
			 *      </Feature>
			 *    </GateDocumentFeatures>
			 *    
			 *    <!-- The document content area with serialized nodes -->
			 *    
			 *    <TextWithNodes>
			 *      <Node id="0"/>...Yamaha TX-350<Node id="13"/> and a Technics SL-P230 discussed on an appropriate forum...
			 *    </TextWithNodes>
			 *    
			 *    <!-- The default annotation set -->
			 *    
			 *    <AnnotationSet>
			 *    
			 *      <!-- annotation for Yamaha TX-350 -->
			 *      
			 *      <Annotation Id="0" Type="Lookup" StartNode="0" EndNode="13">
			 *        <Feature>
			 *          <Name className="java.lang.String">wholeWord</Name>
			 *          <Value className="java.lang.Boolean">true</Value>
			 *        </Feature>
			 *        <Feature>
			 *          <Name className="java.lang.String">majorType</Name>
			 *          <Value className="java.lang.String">product</Value>
			 *        </Feature>
			 *        <Feature>
			 *          <Name className="java.lang.String">atBeginning</Name>
			 *          <Value className="java.lang.Boolean">true</Value>
			 *        </Feature>
			 *        <Feature>
			 *		    <Name className="java.lang.String">firstcharUpper</Name>
			 *		    <Value className="java.lang.Boolean">true</Value>
			 *		  </Feature>
			 *		  <Feature>
			 *		    <Name className="java.lang.String">company</Name>
			 *		    <Value className="java.lang.String">Yamaha</Value>
			 *		  </Feature>
			 *		  <Feature>
			 *		    <Name className="java.lang.String">brand</Name>
			 *		    <Value className="java.lang.String">TX 350</Value>
			 *		  </Feature>
			 *		  <Feature>
			 *		    <Name className="java.lang.String">minorType</Name>
			 *		    <Value className="java.lang.String">hifi</Value>
			 *		  </Feature>
			 *		  <Feature>
			 *		    <Name className="java.lang.String">groupid</Name>
			 *		    <Value className="java.lang.String">1903328</Value>
			 *		  </Feature>
			 *		  <Feature>
			 *		    <Name className="java.lang.String">type</Name>
			 *		    <Value className="java.lang.String">full</Value>
			 *		  </Feature>
			 *		  <Feature>
			 *		    <Name className="java.lang.String">atEnd</Name>
			 *		    <Value className="java.lang.Boolean">true</Value>
			 *		  </Feature>
			 *		  <Feature>
			 *		    <Name className="java.lang.String">firstcharCategory</Name>
			 *		    <Value className="java.lang.Integer">1</Value>
			 *		  </Feature>
			 *      </Annotation>
			 *      
			 *      ...
			 *      
			 *    </AnnotationSet>
			 *  </GateDocument>
			 */
			
			
			/*
			 * do some further named entity processing
			 */
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {       
		
		/*
		 * retrieve port for socket connection
		 */
		port = (Integer)conf.get("port");
		
	}

}
