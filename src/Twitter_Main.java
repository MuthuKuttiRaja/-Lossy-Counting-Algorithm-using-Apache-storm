import twitter4j.GeoLocation;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.auth.AccessToken;

public class Twitter_Main {
	public static void main(String args[]) {

		Twitter twiter = new TwitterFactory().getInstance();
		twiter.setOAuthConsumer("",
				"");
		twiter.setOAuthAccessToken(new AccessToken(
				"",
				""));
		try {
			double lat = 13.5000;
			double lon = 80.1600;
			double res = 100;
			String resUnit="mi";
			//Query query = new Query("Delhi").geoCode(new GeoLocation(lat,lon), res,resUnit ); 
			//query.count(100); //You can also set the number of tweets to return per page, up to a max of 100
			Query query=new Query("nasdaq");
			query.setCount(100);
			QueryResult result = twiter.search(query);
			User user = twiter.verifyCredentials();
			System.out.println("Showing @" + user.getScreenName()
					+ "'s home timeline.");
			int i = 1;
			for (Status b : result.getTweets()) {
				System.out.println(i++ + "\t" + b.getText());
			}
		} catch (TwitterException e) {
			e.printStackTrace();
		}
	}

}
