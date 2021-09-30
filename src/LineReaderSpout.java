
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;



public class LineReaderSpout implements IRichSpout {
	private SpoutOutputCollector _collector;
	//private FileReader fileReader;
	
	private int totalCount=0;
	private TopologyContext context;
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream _twitterStream;
	String consumerKey;
	String consumerSecret;
	String accessToken;
	String accessTokenSecret;
	String[] keyWords;
	
	public LineReaderSpout(String consumerKey, String consumerSecret,
			String accessToken, String accessTokenSecret, String[] keyWords) {
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
		this.keyWords = keyWords;
	}

	public LineReaderSpout() {
		// TODO Auto-generated constructor stub
	}
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		_collector = collector;

		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
			
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onException(Exception ex) {
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub

			}

		};

		/*TwitterStream twitterStream = new TwitterStreamFactory(
				new ConfigurationBuilder().setJSONStoreEnabled(true).build())
				.getInstance();

		twitterStream.addListener(listener);
		
		twitterStream.setOAuthConsumer("pOjmB0p78k2Pt7VsiXt3tA2ZT",
				"wWqrRtmH3wihXUW7oiQcnertSxdKgyQt0oT5W94jSePtjQj76i");
		
		twitterStream.setOAuthAccessToken(token);
		twitterStream.sample();*/
		AccessToken token= new AccessToken(
				"3442785313-TSr3R5batI6bwZCOPNWRcv4y6qHBnTWzsGO8KME",
				"8EpHkChlsP2wDOfbW2MkhxaDScm7FQwokOtGASKXsAIMS");
		TwitterStreamFactory fact = new TwitterStreamFactory(new ConfigurationBuilder().setJSONStoreEnabled(true).setOAuthConsumerKey("pOjmB0p78k2Pt7VsiXt3tA2ZT").setOAuthConsumerSecret("wWqrRtmH3wihXUW7oiQcnertSxdKgyQt0oT5W94jSePtjQj76i").build());
        _twitterStream = fact.getInstance();
        _twitterStream.addListener(listener);
        _twitterStream.setOAuthAccessToken(token);

        _twitterStream.sample();

	
	}


	@Override
	public void nextTuple() {
		
		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
		} else {
			 SpoutOutputFormat value = new SpoutOutputFormat();
			 value.setHashTagCreatedAt(ret.getCreatedAt());
			 HashtagEntity[] hashTags= ret.getHashtagEntities();
			 ArrayList<String> aL= new ArrayList<String>();
			 for(HashtagEntity individualTag:hashTags)
			 {
				 aL.add("#"+individualTag.getText().toLowerCase());
			 }
			 value.setHashTags(aL);
			 this._collector.emit(new Values(value));
				// System.out.println(value);
			
		}
	}
		
		
		
	/*	if (completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {

			}
		}
		String str;
		try {
			Twitter twiter = new TwitterFactory().getInstance();
			twiter.setOAuthConsumer("pOjmB0p78k2Pt7VsiXt3tA2ZT",
					"wWqrRtmH3wihXUW7oiQcnertSxdKgyQt0oT5W94jSePtjQj76i");
			twiter.setOAuthAccessToken(new AccessToken(
					"3442785313-TSr3R5batI6bwZCOPNWRcv4y6qHBnTWzsGO8KME",
					"8EpHkChlsP2wDOfbW2MkhxaDScm7FQwokOtGASKXsAIMS"));
			try {
				double lat = 13.5000;
				double lon = 80.1600;
				double res = 100;
				String resUnit="mi";
				Query query = new Query().geoCode(new GeoLocation(lat,lon), res,resUnit ); 
				//query.count(100); //You can also set the number of tweets to return per page, up to a max of 100
				//Query query=new Query("jayalalitha");
				query.setCount(100);
				QueryResult result = twiter.search(query);
				User user = twiter.verifyCredentials();
				System.out.println("Showing @" + user.getScreenName()
						+ "'s home timeline.");
				String value="";
				Pattern pattern = Pattern.compile("\\#(.*?)\\ ");
				Matcher matcher;
				
				for (Status b : result.getTweets()) {
					{
						matcher = pattern.matcher(b.getText());
						while(matcher.find()) {
							 value=matcher.group();
							 this.collector.emit(new Values(value), value);
						}
					
					}
					//System.out.println(i++ + "\t" + b.getText());
				}
			} catch (TwitterException e) {
				e.printStackTrace();
			}
			
			
//			while ((str = reader.readLine()) != null) {
//				this.collector.emit(new Values(str), str);
//			}
		} catch (Exception e) {
			throw new RuntimeException("Error reading typle", e);
		} finally {
			completed = true;
		}

	}*/
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("out"));
	}

	@Override
	public void close() {
		//_twitterStream.shutdown();
	}
	public boolean isDistributed() {
		return false;
	}
	@Override
	public void activate() {
	}
	@Override
	public void deactivate() {
	}
	@Override
	public void ack(Object msgId) {
	}
	@Override
	public void fail(Object msgId) {
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

}