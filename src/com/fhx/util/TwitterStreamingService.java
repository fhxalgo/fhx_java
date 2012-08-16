package com.fhx.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import twitter4j.Status;
import twitter4j.StatusAdapter;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
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
			
	        //TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
	        StatusListener listener = new StatusListener() {
	            @Override
	            public void onStatus(Status status) {
	                System.out.println("#" + status.getUser().getScreenName() + " - " + status.getText() + " @T " + status.getCreatedAt());	                
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
    /**
     * Main entry of this application.
     *
     * @param args
     */
    public static void main(String[] args) throws TwitterException {
    	// init anth
    	new TwitterStreamingService();
    	System.out.println("Service started.");
    }
}