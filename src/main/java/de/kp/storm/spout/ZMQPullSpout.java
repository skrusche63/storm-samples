package de.kp.storm.spout;

import java.util.Map;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/*
 * Read messages from the ZeroMQ PUSH producer
 */
public class ZMQPullSpout extends BaseRichSpout {

	private static final long serialVersionUID = 6217408770845987029L;

	private SpoutOutputCollector collector;

	private ZMQ.Context zeroMqContext;
	private ZMQ.Socket socket;

	/*
	 * Remarks
	 * 
	 * In zeromq 2.x, the HWM is unlimited by default. If the PUSH generates 
	 * messages faster than the spout can handle, memory will keep increasing.
	 * 
	 * To avoid this problem, we have to set the HWM of the socket created by 
	 * the spout before connecting: 
	 * 
	 * http://lists.zeromq.org/pipermail/zeromq-dev/2011-March/010226.html
	 */
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		
		this.zeroMqContext = ZMQ.context(10);	
		this.socket = this.zeroMqContext.socket(ZMQ.PULL);
		
		/*
		 * The HWM option shall set the high water mark for the specified socket. The high water mark is a hard limit 
		 * on the maximum number of outstanding messages ZMQ shall queue in memory for any single peer that the specified 
		 * socket is communicating with.
		 * 
		 * If this limit has been reached the socket shall enter an exceptional state and depending on the socket type, ZMQ 
		 * shall take appropriate action such as blocking or dropping sent messages.
		 */		
		this.socket.setHWM(20);
		
		String url = (String)conf.get("url");
		this.socket.connect(url);

		this.collector = collector;

	}

	public void nextTuple() {

		byte[] bytes = null;

		if (this.socket != null) {

			try {

				bytes = socket.recv(0);
				String message = new String(bytes);

				collector.emit(new Values(message));

			} catch (ZMQException e) {
				throw new RuntimeException(e.getCause());

			}

		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}

}
