package de.kp.storm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import de.kp.storm.bolt.FileBolt;
import de.kp.storm.spout.FileSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class SimpleFileTopology {

	public static void main(String[] args) {
		
		String fname = args[0];
		
		TopologyBuilder builder = new TopologyBuilder();
		
		/*
		 * The number of parallel tasks is set to '1'
		 */
		builder.setSpout("line-getter", new FileSpout(), 1);
		builder.setBolt("line-setter",  new FileBolt(), 1).shuffleGrouping("line-getter"); 

		LocalCluster cluster = new LocalCluster();
		
		Config conf = new Config();
		conf.put("file", fname);
		
		cluster.submitTopology("file-storm", conf, builder.createTopology());

		/*
		 * Wait for input from command line
		 */
		System.out.println("Press any key to exit");
		try {

			new BufferedReader(new InputStreamReader(System.in)).readLine();

		} catch (IOException e) {
			e.printStackTrace();
		}

		cluster.killTopology("file-storm");
		cluster.shutdown();
		
	}

}
