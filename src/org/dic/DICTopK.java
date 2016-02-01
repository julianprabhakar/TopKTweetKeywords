package org.dic;

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * @author : Julian Prabhakar L
 */
public class DICTopK {
	
	// NOTE: The queue represents a sliding window of certain number of tweets
	private static AbstractQueue<String> queue = null;
	// This is for tracking frequency of incoming hashtags in tweets
	private static Map<String, Integer> enqueueMap = new HashMap<String,Integer>(); 
	// This is for tracking frequency of hashtags of tweets exiting the sliding windown
	private static Map<String, Integer> dequeueMap = new HashMap<String,Integer>();
	// Get valid hashtags
	private static Pattern pattern = Pattern.compile("(?:\\s|\\A)[##]+([A-Za-z0-9-_]+)");
	private static Matcher matcher = null;
	private static List<TopKElement> topKList = null;
	private static TopKComparator topKComparator = new TopKComparator(); 
	private static Integer kVal = null;
	private static Integer slidingWindowSize = null;
	private static Scanner scanner = new Scanner(System.in);


	
	public static void main(String args[])
	{
		String consumerKey, consumerSecret, accessToken,accessSecret,keyWordsStr = "#";
		
		System.out.println("Please enter the Twitter authentication details carefully!");
		System.out.println();
		System.out.print("Please enter the consumer key:");
		consumerKey = scanner.next();
		System.out.println();
		System.out.print("Please enter the consumer secret:");
		consumerSecret = scanner.next();
		System.out.println();
		System.out.print("Please enter the consumer access token:");
		accessToken = scanner.next();
		System.out.println();
		System.out.print("Please enter the consumer access secret:");
		accessSecret = scanner.next();
		System.out.println();
		try{
			System.out.print("Please enter the k value:");
			kVal = Integer.parseInt(scanner.next());
			System.out.println();
			topKList = new ArrayList<TopKElement>(kVal);
		}
		catch(Exception e)
		{
			System.out.println("Invalid k value, taking default value of 20");
			System.out.println();
			kVal = 20;
			topKList = new ArrayList<TopKElement>(kVal);
		}
		try{
			System.out.print("Please enter the number of tweets making up the sliding window size:");
			slidingWindowSize = Integer.parseInt(scanner.next());
			System.out.println();
			queue = new ArrayBlockingQueue<String>(slidingWindowSize);			
		}
		catch(Exception e)
		{
			System.out.println("Invalid sliding window size, taking default value of 2000");
			System.out.println();
			slidingWindowSize = 2000;
			queue = new ArrayBlockingQueue<String>(slidingWindowSize);			
		}
		System.out.println("Please enter the tweet filter keywords separated by a single space");
		System.out.println("For example:movies technology");
		keyWordsStr = keyWordsStr+" " + (scanner.next());
			
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey(consumerKey)
		  .setOAuthConsumerSecret(consumerSecret)
		  .setOAuthAccessToken(accessToken)
		  .setOAuthAccessTokenSecret(accessSecret);
		
		FilterQuery tweetFilterQuery = new FilterQuery();
		tweetFilterQuery.track(keyWordsStr.split(" "));
		tweetFilterQuery.language(new String[]{"en"});  
		
		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
			StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				String currentTweet = status.getText(); 
				String removedText = null;
				// If the queue is full, then we need to remove 1 element from it
				// before we add the incoming tweet
				if(queue.size()==slidingWindowSize)
				{
					// Remove one tweet from the queue
					// Analyze it and check if there are hashtags
					// If yes, then add the count to the dequeue map
					removedText = queue.remove();
					processHashtags(removedText, dequeueMap);
					
					// Add the current incoming tweet to the queue
					// And process the add the hashtag count to the enqueue map
					queue.add(currentTweet);
					processHashtagsAndUpdateTopK(currentTweet);
				}
				else
				{
					// Add the current incoming tweet to the queue
					// And process the add the hashtag count to the enqueue map
					queue.add(currentTweet);
					processHashtagsAndUpdateTopK(currentTweet);
				}
			}
			 @Override
			 public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) { }
			 @Override
			 public void onTrackLimitationNotice(int numberOfLimitedStatuses) {	}
			 @Override
			 public void onScrubGeo(long userId, long upToStatusId) { }
			 @Override
			 public void onStallWarning(StallWarning warning) {
				 System.out.println("Obtained a stall warning:" + warning);
			 }
			 @Override
			 public void onException(Exception ex) {
				 ex.printStackTrace();
			 }
		 };
		 twitterStream.addListener(listener);
		 twitterStream.filter(tweetFilterQuery);
		 //twitterStream.sample();
	}
	
	// This function will extract hashtags  from the tweet, and will increment it's count
	// It will set the count as 1, if the hashtag has not been encountered before, else increment the previous count
	public static void processHashtags(String tweetText,Map<String,Integer> map) {
		Integer count = 0;
		String hashtag;
		matcher = pattern.matcher(tweetText);
		while (matcher.find()) {
			hashtag = matcher.group().trim();	
			count = map.get(hashtag);
			if(count==null)
				count = 1;
			else
				count++;
			map.put(hashtag, count);
		}
	}
	
	// LOGIC:: There is a queue which represents a sliding window.
	// Each tweet is queued into it, and a hashmap of the frequencies is maintained in enqueueMap
	// Once the queue is full, the first tweet is removed and the count of frequencies of hashtags in it is 
	// updated to dequeueMap. The effective count for the hashtag is now, enqueueMap - dequeueMap for that hash tag
	// Then, the new tweet is added tot he queue
	//****  NOTE:: Printing is done when the top-k is updated ****//
	public static void processHashtagsAndUpdateTopK(String tweetText) {
		Integer count = 0;
		Integer dequeueCount = 0;
		String hashtag;
		// Match the hashtags from the tweets
		matcher = pattern.matcher(tweetText);
		while (matcher.find()) {
			hashtag = matcher.group().trim();
			// If the tweet was not encountered, set to 1 else increment 
			count = enqueueMap.get(hashtag);
			if(count==null)
				count = 1;
			else
				count++;
			enqueueMap.put(hashtag, count);
			// Obtain the dequeue count
			dequeueCount= dequeueMap.get(hashtag);
			if(dequeueCount ==null)
				dequeueCount =0;
			// Sort the list in descending order before processing
			Collections.sort(topKList, topKComparator);
			if(topKList.size()  ==0)
			{
				// If the topK is empty, just add the element with it's count
				topKList.add(new TopKElement(hashtag, (long)(count -dequeueCount) ));
				printTopK();
			}	
			else if(topKList.size() <kVal)
			{
				// If the topK is not empty and sliding window is not full yet,
				// check if the hashtag is present in the top-k list, if yes, update the count
				// If no, then add it to the top-k list
				// In this case, dequeueCount will be 0
				boolean added = false;
				for(int i=0;i<topKList.size();i++ )
				{
					if(topKList.get(i).getElement().equals(hashtag))
					{
						topKList.get(i).setCount((long) (long)(count -dequeueCount));
						printTopK();
						added = true;
					}
				}
				if(!added)
				{
					topKList.add(new TopKElement(hashtag, (long)(count -dequeueCount) ));
					printTopK();
				}
			}
			else if(topKList.size()==kVal)
			{
				// If the topK is not empty and sliding window is  full yet
				// Update the count if the hashtag is already in the top-k
				// Replace the item it's effective count is greater than the least count of a top-k item
				for(int i=0;i<topKList.size();i++ )
				{
					if(topKList.get(i).getElement().equals(hashtag))
					{
						topKList.get(i).setCount((long) (count-dequeueCount));
						printTopK();
						break;
					}
					else if((i==topKList.size()-1) && topKList.get(i).getCount().intValue()< (count-dequeueCount) )
					{
						// Replace with last item here,( since it is sorted)
						topKList.set(i,new TopKElement(hashtag, (long) (count-dequeueCount) ));
						printTopK();
					}
				}					
			}
		}
	}
	private static void printTopK() {
		Collections.sort(topKList, topKComparator);
		System.out.println(topKList.toString());
	}
}
