package com.fhx.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

import twitter4j.Status;
import twitter4j.StatusAdapter;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.TwitterException;

public final class TwitterStreamingService extends StatusAdapter {
	private Logger log = LogManager.getLogger(TwitterStreamingService.class);
	
	private TwitterStream twitterStream = null;
	private String ConsumerKey;
	private String ConsumerSecret;
	private String AccessToken;
	private String AccessTokenSecret;

	public TwitterStreamingService() {
		if (loadTweeterProperties()) {
			initTweeter();
		}
	}

	private boolean loadTweeterProperties() {
		File file = new File("conf/twitter4j.properties");
		Properties prop = new Properties();
		InputStream is = null;
		try {
			if (file.exists()) {
				is = new FileInputStream(file);
				prop.load(is);

				ConsumerKey = prop.getProperty("oauth.consumerKey");
				ConsumerSecret = prop.getProperty("oauth.consumerSecret");
				AccessToken = prop.getProperty("oauth.accessToken");
				AccessTokenSecret = prop.getProperty("oauth.accessTokenSecret");

				if (ConsumerKey.isEmpty() || ConsumerSecret.isEmpty()
						|| AccessToken.isEmpty() || AccessTokenSecret.isEmpty()) {
					log.warn("OAuth adapter properties cannot be empty. TweeterService is not initalized.");
					return false;
				}
				log.info(String.format(
						"setting tweeter properties: key=%s, token=%s",
						ConsumerKey, AccessToken));

			} else {
				log.error("cannot find twitter4j.properties.  will not send status to tweeter.");
			}

		} catch (IOException ioe) {
			ioe.printStackTrace();
			return false;
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException ignore) {
				}
			}
		}

		return true;
	}

	private void initTweeter() {
		try {
			System.setProperty("twitter4j.oauth.consumerKey", ConsumerKey);
			System.setProperty("twitter4j.oauth.consumerSecret", ConsumerSecret);
			// System.setProperty("twitter4j.oauth.accessToken", AccessToken );
			// System.setProperty("twitter4j.oauth.accessTokenSecret",
			// AccessTokenSecret );

			this.twitterStream = new TwitterStreamFactory().getInstance();
			// this.twitter.setOAuthConsumer(ConsumerKey, ConsumerSecret);

			AccessToken oathAccessToken = new AccessToken(AccessToken, AccessTokenSecret);
			this.twitterStream.setOAuthAccessToken(oathAccessToken);
			
	    	// Mongo db service
			Mongo m = new Mongo();
			DB db = m.getDB("test");
			final DBCollection coll = db.getCollection("retweets");

	        //TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
	        StatusListener listener = new StatusListener() {
	            @Override
	            public void onStatus(Status status) {
	            	
	            	/*status=StatusJSONImpl{
	            		createdAt=Mon Aug 27 22:57:03 EDT 2012, 
	            		id=240281815385010178, 
	            		text='@jennyXwho hey, a laugh is how I get out of bed in the morning, either that or one of my dogs Dutch ovens me the fuckers....but I digress.', 
	            		source='<a href="http://twitter.com/download/iphone" 
	            		rel="nofollow">Twitter for iPhone</a>', 
	            		isTruncated=false, 
	            		inReplyToStatusId=240281092463140864, 
	            		inReplyToUserId=245517480, 
	            		isFavorited=false, 
	            		inReplyToScreenName='jennyXwho', 
	            		geoLocation=null, 
	            		place=null, 
	            		retweetCount=0, 
	            		wasRetweetedByMe=false, 
	            		contributors=null, 
	            		annotations=null, 
	            		retweetedStatus=null, 
	            		userMentionEntities=[UserMentionEntityJSONImpl{start=0, end=10, name='Jenny', screenName='jennyXwho', id=245517480}], 
	            		urlEntities=[], 
	            		hashtagEntities=[], 
		            	user=UserJSONImpl{id=36058674, name='Roy Husson', screenName='Rhusson', location='', description='Fuckmonkey!', 
		            		isContributorsEnabled=false, 
		            		profileImageUrl='http://a0.twimg.com/profile_images/2550715021/image_normal.jpg', 
		            		profileImageUrlHttps='https://si0.twimg.com/profile_images/2550715021/image_normal.jpg', 
		            		url='null', isProtected=false, followersCount=245, status=null, profileBackgroundColor='C0DEED', 
		            		profileTextColor='333333', profileLinkColor='0084B4', profileSidebarFillColor='DDEEF6', 
		            		profileSidebarBorderColor='C0DEED', profileUseBackgroundImage=true, showAllInlineMedia=false, friendsCount=517, 
		            		createdAt=Tue Apr 28 09:04:06 EDT 2009, favouritesCount=856, utcOffset=-18000, 
		            		timeZone='Eastern Time (US & Canada)', 
		            		profileBackgroundImageUrl='http://a0.twimg.com/images/themes/theme1/bg.png', 
		            		profileBackgroundImageUrlHttps='https://si0.twimg.com/images/themes/theme1/bg.png', 
		            		profileBackgroundTiled=false, lang='en', statusesCount=3113, isGeoEnabled=false, isVerified=false, translator=false, listedCount=2, isFollowRequestSent=false
		            		}
		            	}*/	            	
	            	//System.out.println("HEHE: status=" +status + ", source=" + status.getSource() + ", rtcount=" + status.getRetweetCount());
	                System.out.println("#" + status.getUser().getScreenName() + " - " + status.getText() + " @T " + status.getCreatedAt());	                
	                
	                // dispatch to a Queue
	        	    BasicDBObject doc = new BasicDBObject();
	        	    doc.put("user_name",status.getUser().getScreenName());
	        	    doc.put("tweet", status.getText());
	        	    doc.put("tweet_id", status.getId());
	        	    doc.put("date", status.getCreatedAt());
	        	    //doc.put("location", status.getGeoLocation() == null ? "xx" : status.getGeoLocation());
	        	    doc.put("location", status.getPlace() == null ? "xx" : status.getPlace().getName());
	        	    
	        	    coll.insert(doc);
	            }

	            @Override
	            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
	                //System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
	            }

	            @Override
	            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
	                //System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
	            }

	            @Override
	            public void onScrubGeo(long userId, long upToStatusId) {
	                //System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
	            }

//	            @Override
//	            public void onStallWarning(StallWarning warning) {
//	                System.out.println("Got stall warning:" + warning);
//	            }

	            @Override
	            public void onException(Exception ex) {
	                ex.printStackTrace();
	            }
	            
	        };
	        
	        twitterStream.addListener(listener);
	        twitterStream.sample();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public TwitterStream getClient() {
		return this.twitterStream;
	}
    /**
     * Main entry of this application.
     *
     * @param args
     */
    public static void main(String[] args) throws TwitterException {
    	try {

	    	// start streaming service
	    	new TwitterStreamingService();
	    	System.out.println("Service started.");
    	
    	}
    	catch(Exception e) {
    		e.printStackTrace();
    	}
    }
}