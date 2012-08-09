package com.fhx.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import twitter4j.PagableResponseList;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Trends;
import twitter4j.Tweet;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.auth.AccessToken;
import twitter4j.Query;
import twitter4j.ResponseList;
import twitter4j.Trend;

public enum TweeterService {
	INSTANCE;

	private Logger log = LogManager.getLogger(TweeterService.class);

	private static final String COMMAND_SEARCH = "search";
	private static final String advancedName = "Twitter4J API method";

	private Twitter client = null;
	private String ConsumerKey;
	private String ConsumerSecret;
	private String AccessToken;
	private String AccessTokenSecret;

	private TweeterService() {
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

			this.client = new TwitterFactory().getInstance();
			// this.twitter.setOAuthConsumer(ConsumerKey, ConsumerSecret);

			AccessToken oathAccessToken = new AccessToken(AccessToken,
					AccessTokenSecret);

			this.client.setOAuthAccessToken(oathAccessToken);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void sendTweet(String tweet) {
		try {
			if (this.client == null) {
				log.error("tweet service is not intialized, tweet msg ["
						+ tweet + "] is not sent.");
			}

			log.info("tweet-> " + tweet);
			client.updateStatus(tweet.substring(0,
					Math.min(140 - 1, tweet.length())));
		} catch (TwitterException e) {
			e.printStackTrace();
		}
	}

	public void sendQuery(Method commandMethod, String myCommand,
			String myString, Long myLong, Integer myInt, Date myTimestamp) {

		try {
			Object myObject = null;

			// the follow will work for methods that have a single argument
			if ((myString == null) && (myLong == null) && (myInt == null)
					&& (myTimestamp == null)) {
				commandMethod = Twitter.class.getDeclaredMethod(myCommand);
				myObject = commandMethod.invoke(this.client);

			} else if (myString != null) {
				// special handling for commands that are not pure string such
				// as "search"
				if (COMMAND_SEARCH.equals(myCommand)) {
					twitter4j.Query query = new twitter4j.Query();
					query.setQuery(myString);

					if (myTimestamp != null) {
						SimpleDateFormat formatter = new SimpleDateFormat(
								"yyyy-MM-dd");
						String aDate = formatter.format(myTimestamp);
						query.setSince(aDate);
					}
					commandMethod = Twitter.class.getDeclaredMethod(myCommand,
							twitter4j.Query.class);
					myObject = commandMethod.invoke(client, query);
				} else {
					commandMethod = Twitter.class.getDeclaredMethod(myCommand,
							String.class);
					myObject = commandMethod.invoke(client, myString);
				}

			} else if (myLong != null) {
				commandMethod = Twitter.class.getDeclaredMethod(myCommand,
						long.class);
				myObject = commandMethod.invoke(client, myLong.longValue());
			} else if (myInt != null) {
				commandMethod = Twitter.class.getDeclaredMethod(myCommand,
						int.class);
				myObject = commandMethod.invoke(client, myInt.intValue());
			} else if (myTimestamp != null) {
				commandMethod = Twitter.class.getDeclaredMethod(myCommand,
						Date.class);
				myObject = commandMethod.invoke(client, myTimestamp);
			}

			if (myObject == null) {
				// some commands just return void
				return;
			}

			if (myObject instanceof twitter4j.ResponseList) {
				ResponseList<?> listObject = (ResponseList<?>) myObject;
				if (listObject.size() == 0) {
					log.info(String.format(" " + advancedName
							+ " {0} returned an empty list", myCommand));
					return;
				} else {
					log.info("return list of of type: "
							+ listObject.get(0).getClass().getName());
					if (listObject.get(0) instanceof Status) {
						List<Status> twitterStatusList = (List<Status>) listObject;
						List<String> StatusList = new ArrayList<String>();
						try {
							for (int i = 0; i < twitterStatusList.size(); i++) {
								Status st = twitterStatusList.get(i);

								StringBuilder sb = new StringBuilder();
								sb.append(st.getText());
								sb.append(", ");
								sb.append(st.getCreatedAt());
								sb.append(", ");
								sb.append(st.getId());
								sb.append(", ");
								sb.append(st.getUser());
								StatusList.add(sb.toString());
							}
						} catch (Exception e) {
							e.printStackTrace();
							log.error(
									String.format(
											" Exception with Twitter Status list on "
													+ advancedName + ": {0}",
											myCommand), e);
						}
					} else if (listObject.get(0) instanceof twitter4j.User) {
						List<twitter4j.User> twitterUserList = (List<twitter4j.User>) listObject;

						List<String> statusList = new ArrayList<String>();
						try {
							for (int i = 0; i < twitterUserList.size(); i++) {
								User st = twitterUserList.get(i);
								String UserTuple = getUserInfo(st);
								statusList.add(UserTuple);
							}
							log.info(Arrays.toString(statusList.toArray()));

						} catch (Exception e) {
							e.printStackTrace();
							log.error(
									String.format(
											" Exception with Twitter User list on "
													+ advancedName + ": {0}",
											myCommand), e);
						}
					} else if (listObject.get(0) instanceof Long) {
						List<Long> twitterIDList = (List<Long>) listObject;

						List<Long> idList = new ArrayList<Long>();
						try {
							for (int i = 0; i < twitterIDList.size(); i++) {
								Long ID = twitterIDList.get(i);
								idList.add(ID);
							}
							log.info(Arrays.toString(idList.toArray()));

						} catch (Exception e) {
							e.printStackTrace();
							log.error(
									String.format(
											" Exception with Long list on "
													+ advancedName + ": {0}",
											myCommand), e);
						}
					} else {
						log.warn(String.format(" Unknown list type from "
								+ advancedName + ": {0}", myCommand));
					}
				}
			} else if (myObject instanceof twitter4j.Trends /*
															 * listObject.get(0)
															 * instanceof Trend
															 */) {
				Trends myTrends = (Trends) myObject;
				List<String> trendList = extractTrendList(myTrends);

				try {
					log.info(Arrays.toString(trendList.toArray()));
				} catch (Exception e) {
					e.printStackTrace();
					log.warn(
							String.format(" Exception with Long list on "
									+ advancedName + ": {0}", myCommand), e);
				}
			} else if (myObject instanceof List) {
				List<?> twitterList = (List<?>) myObject;
				log.info("list is of type: " + twitterList.getClass().getName());
				if (twitterList.size() == 0) {
					log.info("zero sized list");
				} else {
					log.info("list element is of type: "
							+ twitterList.get(0).getClass().getName());
					if (twitterList.get(0) instanceof twitter4j.Trends) {
						log.debug("list element is of type: Trends");

						for (Object myTrendsObj : twitterList) {
							Trends myTrends = (Trends) myTrendsObj;
							List<String> trendList = extractTrendList(myTrends);
							try {
								log.info(Arrays.toString(trendList.toArray()));
							} catch (Exception e) {
								e.printStackTrace();
								log.error(String.format(
										" Exception with Long list on "
												+ advancedName + ": {0}",
										myCommand), e);
							}
						}
					}
				}
			} else if (myObject instanceof List) {
				List<?> twitterList = (List<?>) myObject;
				log.info("list is of type: " + twitterList.getClass().getName());
				if (twitterList.size() == 0) {
					log.info("zero sized list");
				} else {
					log.info("list element is of type: "
							+ twitterList.get(0).getClass().getName());
					if (twitterList.get(0) instanceof twitter4j.Trends) {
						log.info("list element is of type: Trends");
						for (Object myTrendsObj : twitterList) {
							Trends myTrends = (Trends) myTrendsObj;
							List<String> trendList = extractTrendList(myTrends);
							try {
								log.info(Arrays.toString(trendList.toArray()));
							} catch (Exception e) {
								e.printStackTrace();
								log.error(String.format(
										" Exception with Long list on "
												+ advancedName + ": {0}",
										myCommand), e);
							}
						}
					}
				}
			} else if (myObject instanceof twitter4j.IDs) {
				twitter4j.IDs twitterIDsX = (twitter4j.IDs) myObject;
				long[] twitterIDs = twitterIDsX.getIDs();
				if (twitterIDs.length == 0) {
					log.info("zero sized ID list");
				} else {
					List<Long> longList = new ArrayList<Long>();
					for (long id : twitterIDs) {
						longList.add(new Long(id));
					}

					try {
						log.info(Arrays.toString(longList.toArray()));
					} catch (Exception e) {
						e.printStackTrace();
						log.info(
								String.format(" Exception with Long list on "
										+ advancedName + ": {0}", myCommand), e);
					}
				}
			}
			// TODO: PagableResponseList has not been tested
			else if (myObject instanceof twitter4j.PagableResponseList) {
				PagableResponseList<?> twitterList = (PagableResponseList<?>) myObject;
				log.info("list is of type: " + twitterList.getClass().getName());

				while (twitterList.hasNext()) {
					twitterList.getNextCursor();
					log.info("list element is of type: "
							+ twitterList.get(0).getClass().getName());
					if (twitterList.get(0) instanceof twitter4j.ResponseList) {
						log.info("list element is of type: Trends");
						for (Object myTrendsObj : twitterList) {
							Trends myTrends = (Trends) myTrendsObj;
							List<String> trendList = extractTrendList(myTrends);
							try {
								log.info(Arrays.toString(trendList.toArray()));
							} catch (Exception e) {
								e.printStackTrace();
								log.error(String.format(
										" Exception with Long list on "
												+ advancedName + ": {0}",
										myCommand), e);
							}
						}
					}
				}
			} else if (myObject instanceof twitter4j.Status) {
				Status st = (Status) myObject;
				try {
					List<String> statusList = new ArrayList<String>();
					statusList.add(getStatusInfo(st));
					log.info(Arrays.toString(statusList.toArray()));
				} catch (Exception e) {
					log.error(
							String.format(" Exception with Twitter status on "
									+ advancedName + ": {0}", myCommand), e);
				}
			} else if (myObject instanceof twitter4j.User) {
				User usr = (User) myObject;
				try {
					List<String> userList = new ArrayList<String>();
					userList.add(getUserInfo(usr));
					log.info(Arrays.toString(userList.toArray()));
				} catch (Exception e) {
					log.error(
							String.format(" Exception with Twitter user on "
									+ advancedName + ": {0}", myCommand), e);
				}
			} else if (myObject instanceof Integer) {
				long coerceLong = (Integer) myObject;
				try {
					List<Long> intList = new ArrayList<Long>();
					intList.add(coerceLong);
					log.info(Arrays.toString(intList.toArray()));
				} catch (Exception e) {
					log.error(
							String.format(" Exception with Twitter long on "
									+ advancedName + ": {0}", myCommand), e);
				}
			} else if (myObject instanceof String) {
				String coerceString = (String) myObject;
				try {
					List<String> stringList = new ArrayList<String>();
					stringList.add(coerceString);
					log.info(Arrays.toString(stringList.toArray()));
				} catch (Exception e) {
					log.error(
							String.format(" Exception with Twitter long on "
									+ advancedName + ": {0}", myCommand), e);
				}
			} else if (myObject instanceof QueryResult) {
				QueryResult result = (QueryResult) myObject;
				try {
					java.util.List<Tweet> tweets = result.getTweets();
					List<String> stringList = new ArrayList<String>();
					for (Tweet eachTweet : tweets) {
						stringList.add(eachTweet.getText());
					}

					log.info(Arrays.toString(stringList.toArray()));
					
				} catch (Exception e) {
					log.error(
							String.format(" Exception with Twitter long on "
									+ advancedName + ": {0}", myCommand), e);
				}
			} else {
				log.error(String.format(" Unsupported type {0} returned from "
						+ advancedName + ": {1}",
						myObject.getClass().getName(), myCommand));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private String getUserInfo(User usr) throws Exception {
		StringBuilder sb = new StringBuilder();

		sb.append(usr.getScreenName()).append(", ");
		sb.append(usr.getDescription()).append(", ");
		sb.append(usr.getLocation()).append(", ");

		URL web = usr.getURL();// .getWebsite();
		if (web != null)
			sb.append(usr.getURL().toString());
		sb.append(usr.getId());

		return sb.toString();
	}

	private String getStatusInfo(Status st) throws Exception {
		StringBuilder sb = new StringBuilder();

		sb.append(st.getText());
		sb.append(", ");
		sb.append(st.getCreatedAt());
		sb.append(", ");
		sb.append(st.getId());
		sb.append(", ");
		sb.append(st.getUser());

		return sb.toString();
	}

	private List<String> extractTrendList(Trends myTrends) {
		twitter4j.Trend[] twitterTrendList = myTrends.getTrends();

		List<String> trendList = new ArrayList<String>();

		StringBuilder trendStringify = new StringBuilder();
		for (Trend trend : twitterTrendList) {
			trendStringify.setLength(0);
			trendStringify.append("name=");
			trendStringify.append(trend.getName());
			trendStringify.append(";query=");
			trendStringify.append(trend.getQuery());
			trendStringify.append(";url=");
			trendStringify.append(trend.getUrl());
			trendList.add(trendStringify.toString());
		}
		return trendList;
	}

	public static void main(String[] args) {
		TweeterService.INSTANCE.sendTweet("my testing1@" + new Date());
		TweeterService.INSTANCE.sendTweet("my testing2: haha");
		
		// test commands
		TweeterService.INSTANCE.sendQuery(null, "q=fhxalgo", null, null, null, null);
	}

}
