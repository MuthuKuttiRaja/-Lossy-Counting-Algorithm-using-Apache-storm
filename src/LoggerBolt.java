

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class LoggerBolt implements IRichBolt{
	
	private OutputCollector collector;
	private FileOutputStream fOS;
	
	
	@Override
	public void execute(Tuple input) {
		try
		{
		SpoutOutputFormat inputSOF = (SpoutOutputFormat)input.getValueByField("out");
		ArrayList<String> hashTags = inputSOF.getHashTags();
		SpoutOutputFormat outputSOF = new SpoutOutputFormat();
		String fileContent="";
		DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		fileContent="<"+df.format(inputSOF.getHashTagCreatedAt())+">\t";
		int hashTagCount=0;
		for(String s: hashTags)
		{
			outputSOF=new SpoutOutputFormat();
			outputSOF.setHashTagCreatedAt(inputSOF.getHashTagCreatedAt());
			hashTagCount++;
			fileContent+="<"+s+">\t";
			//System.out.println(s);
			outputSOF.setHashTag(s);
			
			collector.emit(new Values(outputSOF,s));
			collector.ack(input);
		}
		fileContent+="\n";
		if(hashTagCount!=0)
		{
			fOS.write(fileContent.getBytes());
		}
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	
	}
	@Override
	public void cleanup() {
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("out","hashTag"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector=collector;
		try {
			this.fOS= new FileOutputStream(new File(stormConf.get("outFile").toString()),true);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}