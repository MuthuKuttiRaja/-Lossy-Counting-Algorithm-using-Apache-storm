import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

class ReportsScheduler extends TimerTask
{
	private Map<Integer,String> map;
	private Map<String,FrequencyCount> unsortedMap;
	public void run()
	{
		unsortedMap= new HashMap<String, FrequencyCount>();
		unsortedMap.putAll(WordSpitterBolt.unsortedMap);
		map=new TreeMap<Integer,String>(new Comparator<Integer>() {

			@Override
			public int compare(Integer o1, Integer o2) {
				return o2.compareTo(o1);
			}
		
			
		});
		for(Map.Entry<String, FrequencyCount> entry:unsortedMap.entrySet())
		{
			if(map.containsKey(entry.getValue().getFrequency()))
				map.put(entry.getValue().getFrequency(), map.get(entry.getValue().getFrequency())+", { <HashTag>:"+entry.getKey()+" <DeltaVal>: "+entry.getValue().getBucketId() + " <Freq>: "+entry.getValue().getFrequency()+" }");
			else
				map.put(entry.getValue().getFrequency(), "{ <HashTag>:"+entry.getKey()+" <DeltaVal>: "+entry.getValue().getBucketId() + " <Freq>: "+entry.getValue().getFrequency()+" }");
		}
		try {
			Socket sock=null;
			if(WordSpitterBolt.modeType.toLowerCase().equals("remote"))
				sock= new Socket(WordSpitterBolt.IPAddress,WordSpitterBolt.portNum);
			
			String aggregatedString="Report generated on " +  new Date().toString()+"\n";
			System.out.println("Report generated on " +  new Date().toString());
		
			for(Map.Entry<Integer, String> entry:map.entrySet())
			{
				if(WordSpitterBolt.modeType.toLowerCase().equals("local"))
				 System.out.println(entry.getValue());
				else
				{
					System.out.println(entry.getValue());
					aggregatedString+=entry.getValue()+"\n";
				}
					
			}
			if(WordSpitterBolt.modeType.toLowerCase().equals("remote"))
			{
				DataOutputStream fileMessageOut = new DataOutputStream(
					sock.getOutputStream());
				fileMessageOut.writeUTF(aggregatedString);
				fileMessageOut.flush();
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
@SuppressWarnings("serial")
public class WordSpitterBolt implements IRichBolt{
	private OutputCollector collector;
	private String hashTag;
	private int hashTagFreq;
	private int deltaVal;
	public static String IPAddress;
	public static String modeType;
	public static int portNum;
	public static Map<String,FrequencyCount> unsortedMap= new ConcurrentHashMap<String,FrequencyCount>();
	
	public void executeScheduleTasks(){
		Timer timer = new Timer();
		ReportsScheduler rS = new ReportsScheduler();
		timer.schedule(rS , 2000,10000);
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		this.hashTag="";
		this.hashTagFreq=0;
		this.deltaVal=0;
		IPAddress=stormConf.get("ipaddress").toString();
		portNum=Integer.parseInt(stormConf.get("port").toString());
		modeType=stormConf.get("mode").toString();
		executeScheduleTasks();
		
	}
	@Override
	public void execute(Tuple input) {
		if(modeType.toLowerCase().equals("local"))
			unsortedMap.clear();
		ArrayList<WordCounterBoltOutput> boltInputArrayList=(ArrayList<WordCounterBoltOutput>)input.getValueByField("word");
		for(WordCounterBoltOutput boltInput:boltInputArrayList)
		{
		hashTag=boltInput.getHashTag();
		hashTagFreq=boltInput.getFrequency();
		deltaVal=boltInput.getDeltaVal();
		FrequencyCount fq= new FrequencyCount(hashTagFreq, deltaVal);
		unsortedMap.put(hashTag,fq);
		}
		
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public void cleanup() {
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
