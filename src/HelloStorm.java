
import java.io.DataInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class HelloStorm {

	public static void main(String[] args) throws Exception{
		Socket listeningSock=null;
		ServerSocket controllerSock = new ServerSocket(0);
		
		controllerSock.getInetAddress();
		Config config = new Config();
		config.put("mode", args[0]);
		config.put("outThreashold", args[1]);
		config.put("epslon", args[2]);
		config.put("outFile", args[3]);
		config.put("ipaddress", InetAddress.getLocalHost().getHostAddress().toString());
		config.put("port", controllerSock.getLocalPort());
		System.out.println(config.get("ipaddress")+"\t"+ config.get("port"));
		config.setDebug(false);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("line-reader-spout", new LineReaderSpout());
		builder.setBolt("logger-bolt", new LoggerBolt()).shuffleGrouping("line-reader-spout");
		if (args[0].toLowerCase().equals("local")) 
		{
			builder.setBolt("word-counter", new WordCounterBolt()).shuffleGrouping("logger-bolt");
			builder.setBolt("Report-generator", new WordSpitterBolt()).shuffleGrouping("word-counter");
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("HelloStorm", config, builder.createTopology());
		}
		else if(args[0].toLowerCase().equals("remote")) 
		{
			builder.setBolt("word-counter", new WordCounterBolt(), 4).fieldsGrouping("logger-bolt", new Fields("hashTag"));
			builder.setBolt("Report-generator", new WordSpitterBolt()).globalGrouping("word-counter");
			config.setNumWorkers(Integer.parseInt(args[4]));
	        StormSubmitter.submitTopology("topo_Name", config, builder.createTopology());            
	        while (true) {

				listeningSock = controllerSock.accept();
				DataInputStream incomingControllerStream;
				
				incomingControllerStream = new DataInputStream(
						listeningSock.getInputStream());
				System.out.println(incomingControllerStream.readUTF());
			}
		}
		
		
		
		
		//cluster.shutdown();
	}
}
