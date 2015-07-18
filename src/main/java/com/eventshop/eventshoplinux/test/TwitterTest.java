package com.eventshop.eventshoplinux.test;

import static com.eventshop.eventshoplinux.constant.Constant.DB_URL;
import static com.eventshop.eventshoplinux.constant.Constant.DRIVER_NAME;
import static com.eventshop.eventshoplinux.constant.Constant.PASSWORD;
import static com.eventshop.eventshoplinux.constant.Constant.USR_NAME;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Arrays;

import twitter4j.GeoLocation;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import com.eventshop.eventshoplinux.constant.Constant;
import com.eventshop.eventshoplinux.domain.common.FrameParameters;
import com.eventshop.eventshoplinux.domain.datasource.emage.ResolutionMapper.SpatialMapper;
import com.eventshop.eventshoplinux.domain.datasource.emage.ResolutionMapper.TemporalMapper;
import com.eventshop.eventshoplinux.domain.datasource.emage.STMerger;
import com.eventshop.eventshoplinux.domain.datasource.emage.STTPoint;
import com.eventshop.eventshoplinux.domain.datasource.emageiterator.STTEmageIterator;
import com.eventshop.eventshoplinux.util.commonUtil.Config;
import com.eventshop.eventshoplinux.util.datasourceUtil.DataProcess;

public class TwitterTest {
	
	public static void main(String[] args)
	{
		ConfigurationBuilder cb = new ConfigurationBuilder();
        /*cb.setDebugEnabled(true)
              .setOAuthConsumerKey("jYjBN7eXBhU0EurloMYGFQ")
              .setOAuthConsumerSecret("lBekOCCDULTkr7JBLPBlvAV9MV4wVkYoKYcEoBimaWY")
              .setOAuthAccessToken("159216395-cJVwHRC2nQdikscMo1Kg6ag1t0vcpmAT3epNZqrB")
              .setOAuthAccessTokenSecret("baXaT7OlavzZwkG6U3WFLMB2OUnUnWq9xRcxXYg1vs");
              */
        cb.setDebugEnabled(true)
        .setOAuthConsumerKey(Config.getProperty("twtConsumerKey"))
        .setOAuthConsumerSecret(Config.getProperty("twtConsumerSecret"))
        .setOAuthAccessToken(Config.getProperty("twtAccessToken"))
        .setOAuthAccessTokenSecret(Config.getProperty("twtAccessTokenSecret"));
        cb.setUseSSL(true); 
        Twitter twitter = new TwitterFactory(cb.build()).getInstance();
        for(int i=0;i<args.length/2;i++)
        {
	        int lat=Integer.valueOf(args[i]);
	        int lng=Integer.valueOf(args[i+1]);
	        
        	Query query = new Query("allergy OR pollen");
	        query.setCount(100);
	        query.setSinceId(0);
	        query.setGeoCode(new GeoLocation(lat,lng), 60.0*2, Query.KILOMETERS);
	        try
	        {
	        	QueryResult result = twitter.search(query);
	        	System.out.println(result.getTweets());
	        
	        for (Status status : result.getTweets()) {
	        	System.out.println("Sanjana.........................lat:"+lat+" lng:"+lng );
	            System.out.println("@" + status.getUser().getScreenName() + ":" + status.getText());
	            
	            Date d1=status.getCreatedAt();
	           
	          
	        }
	        }
	        
	        catch(TwitterException t)
	        {
	        	System.out.println(t.getMessage());
	        }
        }
        
        Date cur=new Date();
        SimpleDateFormat formatterDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        //SimpleDateFormat formatterTime = new SimpleDateFormat("HH:mm:ss.SSS'Z'");
        String tweetDate=formatterDate.format(cur);
       // String tweetTime=formatterTime.format(cur);
        String astDate="dateTime(\""+tweetDate+"\")";
        String a=formatDateAsterix(cur);
        System.out.println(a);
	}
	
	public static String formatDateAsterix(Date date)
	{
		SimpleDateFormat dateFormatter = new SimpleDateFormat("'dateTime('\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"')'");
		String formattedDate=dateFormatter.format(date);
		return formattedDate;
	}

}
