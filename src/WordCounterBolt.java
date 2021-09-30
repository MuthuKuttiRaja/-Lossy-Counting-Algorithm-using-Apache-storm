
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class WordCounterBolt implements IRichBolt{
	Map<String, Integer> counters;
	
	private OutputCollector collector;
	private Map<String,FrequencyCount> map= new ConcurrentHashMap<String,FrequencyCount>();
	private int totalCount=0;
	private int bucketId=1;
	private int windowSize;
	private double epslon;
	private double outThreashold;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.epslon= Double.parseDouble(stormConf.get("epslon").toString());
		this.outThreashold=Double.parseDouble(stormConf.get("outThreashold").toString());
		this.counters = new HashMap<String, Integer>();
		this.collector = collector;
		this.windowSize=(int) (1/epslon);
	}
	@Override
	public void execute(Tuple input) {
		
		FrequencyCount fq;
		SpoutOutputFormat inputSOF = (SpoutOutputFormat)input.getValueByField("out");
		String str=inputSOF.getHashTag();
		Date tweetDate=inputSOF.getHashTagCreatedAt();
		totalCount++;
		if(map.containsKey(str))
    	{
    		fq=map.get(str);
    		fq.setFrequency(fq.getFrequency()+1);
    		fq.setHashTagCreatedAt(tweetDate);
    		map.put(str, fq);
    	}
    	else
    	{
    		fq=new FrequencyCount(1, bucketId-1,new Date());
    		map.put(str, fq);
    	}
		int deltaVal=0;
		int frequency=0;
		Date hashTagCreatedAt= new Date();
		if(totalCount % windowSize==0)
		{
			for(Map.Entry<String, FrequencyCount> entry:map.entrySet())
			{
				fq=entry.getValue();
				frequency=fq.getFrequency();
				deltaVal=fq.getBucketId();
				if((frequency+deltaVal)<=bucketId) 
					map.remove(entry.getKey());
			}
			bucketId++;
			ArrayList<WordCounterBoltOutput> finalOut=new ArrayList<WordCounterBoltOutput>();
			for(Map.Entry<String, FrequencyCount> entry:map.entrySet())
			{
				fq=entry.getValue();
				frequency=fq.getFrequency();
				deltaVal=fq.getBucketId();
				hashTagCreatedAt = fq.getHashTagCreatedAt();
				WordCounterBoltOutput boltOut= new WordCounterBoltOutput(hashTagCreatedAt,entry.getKey(),frequency,deltaVal);
				//System.out.println("out: "+entry.getKey()+"\tFrequency: "+frequency);
				if(frequency>=((outThreashold-epslon)*totalCount))
					finalOut.add(boltOut);
				 
			}
			collector.emit(new Values(finalOut));
			//System.out.println("WordCounter Bolt\tcurrent Bucket id: "  +bucketId+"\tTotal COunt: "+totalCount );
		}
		collector.ack(input);
	}
	@Override
	public void cleanup() {
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}