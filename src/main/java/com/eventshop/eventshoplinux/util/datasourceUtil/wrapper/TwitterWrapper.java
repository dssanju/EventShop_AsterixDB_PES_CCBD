package com.eventshop.eventshoplinux.util.datasourceUtil.wrapper;
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
import com.google.gson.JsonObject;
import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import org.json.JSONArray;
import org.json.JSONObject;

public class TwitterWrapper extends Wrapper 
{
	
	protected static Log log=LogFactory.getLog(TwitterWrapper.class);        
	private String[] bagOfWords;

	private long[][] sinceID;
	private Twitter twitter;

	private LinkedBlockingQueue<STTPoint> ls;

	private boolean isRunning = false;

	boolean [][] isPopulated;// = new boolean [13][30];
	//int [][] numTweetsLoc = new int[13][30];
	
	// For storing tweets into MySQL
	private boolean saveTweets;
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private Connection conn;
	private String tableName;
	private String theme;
	
	int i1=0;
	int i2=-1;
	public TwitterWrapper(String url, String theme, FrameParameters params, boolean saveTweets)
	{
		super(url, theme, params);
		this.theme=theme;
	//	/*@dssanju*/System.out.println("theme:"+theme);
		if (params.numOfRows == 0  || params.numOfColumns == 0) {
			params.calcRowsColumns();
			log.info("calculated params, rows:"+params.numOfRows+" ,cols:"+params.numOfColumns);
		}
		
		isPopulated = new boolean[params.numOfRows][params.numOfColumns];
		
		// Twitter API v1.1 requires that the request must be authenticated
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
        
        TwitterFactory tf = new TwitterFactory(cb.build());
        twitter = tf.getInstance();
        
		ls = new LinkedBlockingQueue<STTPoint>();
		
		this.saveTweets = saveTweets;		
		if(this.saveTweets)
		{			
			this.tableName = "tbl_" + theme + "_tweet";
			connection();
			createTweetTable(theme);
		}
		
	}

	public void setBagOfWords(String[] words)
	{
		bagOfWords = words;
	}
	
/*@dssanju send an HTTP request to AsterixDB*/

	private String reqAsterix(String url) throws Exception {
		 
		//String url = "http://localhost:19002/update?statements=use%20dataverse%20eventshop;insert%20into%20dataset%20STTStream({%20%22id%22:3,%20%22stt-point%22:{%20%22theme%22:%22%22,%22start%22:%222015-03-31%2020:25:00%22,%22end%22:%222015-03-31%2020:30:00%22,%22latitude%22:30.0,%22longitude%22:-93.0,%22latUnit%22:2.0,%22longUnit%22:2.0,%22value%22:21.0}})";
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
 
		// optional default is GET
		con.setRequestMethod("GET");
 
		//add request header
		con.setRequestProperty("User-Agent", "Mozilla/5.0");
		con.setRequestProperty("Accept","application/json");
		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + responseCode);
 
		BufferedReader in = new BufferedReader(
		        new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
 
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
 
		//print result
		//System.out.println(response.toString());
		return response.toString();
 
	}

	public static String formatDateAsterix(Date date)
	{
		SimpleDateFormat dateFormatter = new SimpleDateFormat("'datetime('\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"')'");
		String formattedDate=dateFormatter.format(date);
		return formattedDate;
	}
	
	
	/*@dssanju format URL according to Asterix REST API*/
	
	public static String formatURLAsterix(String qstr)
	{
		/*url=url.replaceAll(" ","%20");
		url=url.replaceAll("\n","%20");
		url=url.replaceAll("\"","%22");
		url=url.replaceAll("#","%23");
		
		return url;*/
		
		try{
			
			qstr=URLEncoder.encode(qstr,"UTF-8");
			}
			
			catch(Exception e)
			{
				e.printStackTrace();
			}
			
			qstr=qstr.replaceAll("\\+","%20");
			
			System.out.println(qstr);
			return qstr;

	}
	
	public void run() 
	{
		isRunning = true;
		
		try {
			getPopulation();
		} catch (IOException e1) {
			log.error(e1.getMessage());
		}
		
		int numofRows=params.getNumOfRows();
		int numOfColumns=params.getNumOfColumns();
		// Initialize the SinceID matrix
		sinceID = new long[numofRows][numOfColumns];
		for(int i = 0; i < numofRows; ++i)
			for(int j = 0; j < numOfColumns; ++j)
				sinceID[i][j] = 0;

		// Setting the bagOfWords for search
		String queryStr = bagOfWords[0];
		for (int i = 1; i < bagOfWords.length; i++)
			queryStr += (" OR " + bagOfWords[i]);

		// Set end time
		long endTime = (long)Math.ceil(System.currentTimeMillis() / params.timeWindow) * params.timeWindow + params.syncAtMilSec;

		while(isRunning)
		{
			STTPoint point;

			Date start = new Date(endTime - params.timeWindow);
			
			int numQueries=0;
			// Loop for Latitude Longitude blocks 
			for(double i = params.swLat; i < params.neLat; i = i+params.latUnit)
			{
				for(double j = params.swLong; j < params.neLong; j = j+params.longUnit)
				{
					if(!isRunning) break;

					int y = (int)Math.ceil(Math.abs((BigDecimal.valueOf(j)).subtract(BigDecimal.valueOf(params.swLong)).divide(BigDecimal.valueOf(params.longUnit)).doubleValue()));
					int x = (int)Math.ceil(Math.abs((BigDecimal.valueOf(i)).subtract(BigDecimal.valueOf(params.swLat)).divide(BigDecimal.valueOf(params.latUnit)).doubleValue()));

					int ret = 0;
					if (isPopulated[12-x][y])
					{
						ret = doCollection(i+0.5*params.latUnit, j+0.5*params.longUnit, x, y, params.latUnit, start, queryStr);
						numQueries++;
					/*@dssanju*/if(ret>0)System.out.print((12-x)+" "+y+" ");
					}			
					//System.out.println("adding...."+ret+" "+theme);
					point = new STTPoint(ret, start, new Date(endTime), params.latUnit, params.longUnit, i, j, theme);
					
				//	ls.add(point);
				//	System.out.println("added");
					//adding to asterixdb
					//System.out.println("asterix");
					//if(ret!=0)
					//{	
						//System.out.println("asterix");
						String sttp=point.toJSON().toString();
						//System.out.println("json: "+sttp);
						sttp=sttp.replaceAll(" ","%20");
						sttp=sttp.replaceAll("\n","%20");
						sttp=sttp.replaceAll("\"","%22");
						//String url_ins="http://localhost:19002/update?statements=use%20dataverse%20eventshop;insert%20into%20dataset%20STTStream({%20%22id%22:"+i1 +",%20%22stt-point%22:+" +sttp +",%22isProc%22:false"+ "})";
						String url_ins="http://localhost:19002/update?statements=use%20dataverse%20eventshop;insert%20into%20dataset%20STTStream_"+theme+"({%20%22id%22:"+i1 +",%20%22stt-point%22:+" +sttp +",%22isProc%22:false"+ "})";
						try
						{
							reqAsterix(url_ins); //@dssanju insert STTPoint into AsterixDB
							i1++;
						}
						catch(Exception e)
						{
							e.printStackTrace();
						}
					//}
						
						System.out.println("adding...."+ret+" "+theme);
						ls.add(point);
						System.out.println("added");
				
				}
			}
			log.info("TwitterWrapper NumQueries made:"+numQueries);

			// Sleeping when window is not up yet
			endTime += params.timeWindow;
			while(System.currentTimeMillis() < endTime) 
			{
				try {
					Thread.sleep(endTime - System.currentTimeMillis());
				} catch (InterruptedException e) {
					log.error(e.getMessage());
				}
			}
		}
	}

	public int doCollection(double lat, double lng, int x, int y, double latUnit, Date date, String queryStr)
	{		
		// Create the query and initialize the parameters
		//System.out.println("Sanjana.....lat:"+lat+" lng:"+lng+" latUnit:"+latUnit+" date:"+date.toString());
	//	System.out.print((int)lat+" "+(int)lng+" ");
		Query query = new Query();
		query.setCount(100);
		query.setGeoCode(new GeoLocation(lat,lng), 60.0*latUnit, Query.KILOMETERS);
		query.setQuery(queryStr);
		query.setSinceId(sinceID[x][y]);
		//System.out.println(new SimpleDateFormat("yyyy-MM-dd").format(date));
		query.setSince(new SimpleDateFormat("yyyy-MM-dd").format(date));
		
		boolean firstOne = true;
		int count = 0;
		QueryResult result;
		try{
	       do {
	            result = twitter.search(query);
	            List<Status> tweets = result.getTweets();
	            
	            for (Status tweet : tweets) {
	            	//log.info("@" + tweet.getUser().getScreenName() + " - " + tweet.getText());
	                
	            	// Update sinceID of this block
					if(firstOne){
						sinceID[x][y] = tweet.getId();
						firstOne = false;
					}
	             //@dssanju commented
					  if(this.saveTweets){
						GeoLocation tw_loc = tweet.getGeoLocation();
						double p_lat = 0;
						double p_lon = 0;
						if (tw_loc != null){
							p_lat = tw_loc.getLatitude();
							p_lon = tw_loc.getLongitude();				
						}
						insertTweet(lat, lng, x, y, tweet, p_lat, p_lon);
					}
	                count++;
	            }
	        } while ((query = result.nextQuery()) != null && count < 100);
	       //log.info("#Tweets @ (lat,long) " + count + " (" + lat + ", " + lng + ")");
			
		} catch (TwitterException e) {
			log.error("theme:" + theme + ", error: " + e.getMessage());
			try {
				if(e.getMessage().contains("500"))
					return 0;
				if(e.getMessage().contains("420") || e.getMessage().contains("429") || e.getMessage().contains("limit")) {
					Thread.sleep(1000*60*15);
					log.info("thread sleep for 15 minutes");
				}
			} catch (InterruptedException e2) {
				System.out.println("why error here?"+e2);
				log.error(e2.getMessage());
			}
			return -1;
		}
        return count;
    }
	
	/*
	 * This method works with twitter4j version 2.0 which supports twitter API 1.0. 
	 * This API 1.0 is not longer supported. 
	 * By: Siripen, 10/02/13
	 */

	/*
	public int doCollection(double lat, double lng, int x, int y, double latUnit, Date date, String queryStr)
	{		
		// Create the query and initialize the parameters
		Query query = new Query();

		query.setGeoCode(new GeoLocation(lat,lng), 60.0*latUnit, Query.KILOMETERS);
		query.setRpp(100);
		query.setQuery(queryStr);
		query.setSinceId(sinceID[x][y]);
		query.setSince(new SimpleDateFormat("yyyy-MM-dd").format(date));

		int count = 0;
		int pageID = 1;
		boolean more = true;
		boolean firstOne = true;
		while(more)
		{
			int cnt = 0;
			query.setPage(pageID);
			pageID++;

			try {
				QueryResult result = twitter.search(query);
				Iterator<Tweet> iterator = result.getTweets().iterator();
				while(iterator.hasNext())
				{
					Tweet tweet = iterator.next();
					// Update sinceID of this block
					if(firstOne)
					{
						sinceID[x][y] = tweet.getId();
						firstOne = false;
					}
					
					if(this.saveTweets)
					{
						GeoLocation tw_loc = tweet.getGeoLocation();
						double p_lat = 0;
						double p_lon = 0;
						if (tw_loc != null)
						{
							p_lat = tw_loc.getLatitude();
							p_lon = tw_loc.getLongitude();				
						}
						insertTweet(lat, lng, x, y, tweet, p_lat, p_lon);
					}
					cnt++;
					count++;
				}

				// No more tweets in the search result
				if (queryStr.indexOf("Cain")>-1 || queryStr.indexOf("Romney")>-1 || queryStr.indexOf("Perry")>-1 )//political tweets
				{
					if(cnt < 100 ) //this page contains less than 100 ...i.e. incomplete. hence no point asking for next page 
					more = false;	
				}
				else
				{ more=false;}
				
				
			} catch (TwitterException e) {
				try {
					if(e.getMessage().contains("500"))
						return 0;
					if(e.getMessage().contains("420")) {
						Thread.sleep(1000*60*20);
					}
				} catch (InterruptedException e2) {
					log.error(e2.getMessage());
				}
				return -1;
			}
		}
		return count;
	}

	*/

	public boolean stop()
	{
		isRunning = false;
		Thread.currentThread().interrupt();
		return true;
	}

	/*@dssanju
	get next STTPoint from AsterixDB instead of linked blocking queue ls*/

	public STTPoint next()
	{
		try {
			//System.out.println("comes here while taking next point");
			//String url_take="use dataverse eventshop;for $i in dataset STTStream_allergy_san where $i.isProc=false order by $i.id limit 1 return $i.stt-point;";
			//String url_take="use dataverse eventshop;for $i in dataset STTStream_allergy_san where $i.isProc=false order by $i.id limit 1 return $i";
			String url_take="use dataverse eventshop;for $i in dataset STTStream_allergy_san where $i.id>"+ i2 +"order by $i.id limit 1 return $i";
			url_take="http://localhost:19002/query?query="+formatURLAsterix(url_take);
			String response=reqAsterix(url_take);
			i2++;
			System.out.println("From Asterix: "+response);
			JSONObject j=new JSONObject(response);
			JSONArray ja=j.getJSONArray("results");
			String stt=ja.getString(0);
			JSONObject jo=new JSONObject(stt);
			jo=jo.getJSONObject("stt-point");
			System.out.println("STT from Asterix:"+stt);
			
			double value; Date start; Date end; double latUnit;
			double longUnit; double latitude; double longitude; String theme;
			
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			value=jo.getDouble("value");
			start=dateFormat.parse(jo.getString("start"));
			end=dateFormat.parse(jo.getString("end"));
			latUnit=jo.getDouble("latUnit");
			longUnit=jo.getDouble("longUnit");
			latitude=jo.getDouble("latitude");
			longitude=jo.getDouble("longitude");
			theme=jo.getString("theme");
			
			STTPoint p=new STTPoint(value,start,end,latUnit,longUnit,latitude,longitude,theme);
			
		//	String url_update="use dataverse eventshop;";
			 
			/*Gson gson = new Gson();
			STTPoint pt= gson.fromJson(response, STTPoint.class);
			//JsonObject jo=new JsonObject(response);
			System.out.println("From Asterix: "+response);
			System.out.println("sttpoint from asterix:"+pt.toJSON().toString());*/
			System.out.println("next() asterix: "+p.toJSON().toString());
			//System.out.println("next() ls: "+ls.take().toJSON().toString());
			
			ls.take();
			return p;
			//return ls.take();
			
			
			
		} catch (InterruptedException e) {
			log.error(e.getMessage());
		}
		catch(Exception e)
		{
			
			e.printStackTrace();
		}
		return null;
	}

/*@dssanju
	check if next point is available in  AsterixDB*/
	
	public boolean hasNext()
	{
		
		//System.out.println("hasNext() asterix: "+ (i2<i1));
		//System.out.println("hasNext() ls: "+ls.peek()!=null);
		return (i2<i1-1);
		//return (ls.peek() != null);
	}


	public void remove()
	{ 	
		ls.remove();
	}


	public void getPopulation() throws IOException
	{	
		//BufferedReader br1= new BufferedReader (new FileReader(Config.getProperty("tempDir") + "/visual/newPopulation.txt"));	
		BufferedReader br1= new BufferedReader (new FileReader("C:\\eventshoptemp" + "\\visual\\newPopulation.txt"));
		String myline = "";
		//double startLat = 24.5;

		int numPop = 0;
		System.out.println("paramssw=="+params.swLat+"params.neLat "+params.neLat+" now params.latUnit "+params.latUnit);
		System.out.println("J LOOP -===params.swLong=="+params.swLong+"params.neLong "+params.neLong+" now params.longUnit "+params.longUnit);
		for(double i = params.swLat; i < params.neLat; i+=params.latUnit)
		{		
			myline=br1.readLine();
			StringTokenizer vals = new StringTokenizer(myline, " ,"); 
			log.info("size of vals:"+vals.countTokens());

			for(double j = params.swLong; j <params.neLong ;j+=params.longUnit)
			{				
				String val= vals.nextToken();
				if (val.compareTo("1")==0)
				{	
					isPopulated[(int) ((i-params.swLat)/params.latUnit)][(int) ((j-params.swLong)/params.longUnit)] = true;
					numPop++;
				}
				else
					isPopulated[(int) ((i-params.swLat)/params.latUnit)][(int) ((j-params.swLong)/params.longUnit)] = false;
			}
		}
		
		log.info("numPop:"+numPop);
		
		//need to change this code and remove hard coded values
//		for(int i = 0; i < 13; i++)
//		{
//			for(int j = 0; j < 30; j++)
//			{
//				numTweetsLoc[i][j] =0;
//			}
//		}
	}

	/*
	public void setPopulation() throws IOException
	{

		int []vals = new int[13*30];

		for(int i = 0; i < 13; i++)
		{
			for(int j = 0; j < 30; j++)
			{
				vals[i*j] = numTweetsLoc[i][j];
			}
		}
		Arrays.sort(vals);

		int cutOffVal = vals[vals.length-40];
		cutOffVal = 5;
		log.info("cutoff is :"+ cutOffVal);
		FileWriter f0 = new FileWriter("newPopulation.txt");

		for(int i = 0; i < 13; i++)
		{
			for(int j = 0; j < 30; j++)
			{
				if (j<29)
				{
					if(numTweetsLoc[i][j]>=cutOffVal)
					{ 
						f0.write(" 1 ,");
					}
					else
					{ 
						f0.write(" 0 ,");
					}
				}
				else
				{
					if(numTweetsLoc[i][j]>=cutOffVal)
					{ 
						f0.write(" 1 ");
					}
					else
					{ f0.write(" 0 ");
					}

				}

			}
			f0.write("\n");
		}
		f0.close();
	}

	 */
	 
	 /*@dssanju create tweet table in AsterixDB, for raw data*/
	 
	void createTweetTable(String theme)
	{
	/*	String query1="CREATE TABLE IF NOT EXISTS "+tableName+" ( " +
				"id INT AUTO_INCREMENT," +
				"tweetid BIGINT NOT NULL," +
				"latitude DOUBLE NOT NULL,"+
				"longitude DOUBLE NOT NULL,"+
				"date DATETIME NOT NULL,"+
				"text TEXT NOT NULL,"+
			    "p_latitude DOUBLE NOT NULL,"+
			    "p_longitude DOUBLE NOT NULL,"+
			    "userName TEXT NOT NULL,"+
			    "PRIMARY KEY(id));\n\n" ;
			String query2 = "CREATE INDEX idx_"+theme+"_Latitude ON "+ tableName+"(latitude);\n";
			String query3 = "CREATE INDEX idx_"+theme+"_Longitude ON "+ tableName+"(longitude);\n" ;
			String query4 = "CREATE INDEX idx_"+theme+"_Date ON "+tableName+"(date);";


		try {
			log.info(query1);
			Statement statement = conn.createStatement();
			statement.execute(query1);
			statement.execute(query2);
			statement.execute(query3);
			statement.execute(query4);
		} catch (SQLException e) {			
			connection();
		}	*/
		
		String qstr="use dataverse eventshop;create dataset "+ tableName+"(TweetMessageType) if not exists primary key tweet-id;create index idx_theme_latitude if not exists on "+tableName+"(latitude);create index idx_theme_longitude if not exists on "+tableName+"(longitude);create index idx_theme_date if not exists on "+tableName+"(date);";
		qstr=formatURLAsterix(qstr);
		qstr="http://localhost:19002/ddl?ddl="+qstr;
		
		String qstr1="use dataverse eventshop;create dataset "+ "STTStream_"+theme+"(STTStream) if not exists primary key id;";
		qstr1=formatURLAsterix(qstr1);
		qstr1="http://localhost:19002/ddl?ddl="+qstr1;
		
		try
		{
			reqAsterix(qstr);
			reqAsterix(qstr1);
		}
		
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	/*@dssanju insert raw tweets into AsterixDB*/
	
	public int insertTweet(double lat, double lng, int x, int y, Status tweet, double p_lat, double p_lon)
	{
		String text = textFilter(tweet.getText());
		if(text == null) return -1;
		
		String date=formatDateAsterix(tweet.getCreatedAt());
		Long id=tweet.getId();
		String user=textFilter(tweet.getUser().getName());
		
		/*		String date = formatter.format(tweet.getCreatedAt());
		
				String sql = "INSERT INTO " + tableName + " VALUES (NULL, " +
			tweet.getId() + ", " + lat + ", " + lng + ", '" + 
			date + "', '" + text + "',"+p_lat+","+p_lon+", '"+textFilter(tweet.getUser().getName())+"')";
		try {
			Statement statement = conn.createStatement();
			statement.execute(sql);
		} catch (SQLException e) {
			log.error(sql);
			log.error(e.getMessage());
			connection();
		}
	*/
		String qstr="use dataverse eventshop;insert into dataset "+ tableName+"({\"tweet-id\":"+id+",\"latitude\":"+lat+",\"longitude\":"+lng+",\"date\":"+date+",\"text\":\""+text+"\",\"p_latitude\":"+p_lat+",\"p_longitude\":"+p_lon+ ",\"username\":\"" +user+"\"})";
		qstr=formatURLAsterix(qstr);
		qstr="http://localhost:19002/update?statements="+qstr;
		try
		{
			reqAsterix(qstr);
		}
		
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return 0;
	}
	
	
	/*
	 * This method works with twitter4j version 2.0 which supports twitter API 1.0. 
	 * This API 1.0 is not longer supported. 
	 * By: Siripen, 10/02/13
	 */
	/*
	public int insertTweet(double lat, double lng, int x, int y, Tweet tweet, double p_lat, double p_lon)
	{
		String text = textFilter(tweet.getText());
		if(text == null) return -1;

		String date = formatter.format(tweet.getCreatedAt());
		String sql = "INSERT INTO " + tableName + " VALUES (NULL, " +
			tweet.getId() + ", " + lat + ", " + lng + ", '" + 
			date + "', '" + text + "',"+p_lat+","+p_lon+", '"+textFilter(tweet.getFromUser())+"')";
		try {
			Statement statement = conn.createStatement();
			statement.execute(sql);
		} catch (SQLException e) {
			log.error(sql);
			log.error(e.getMessage());
			connection();
		}
		return 0;
	}
	*/
	
	public String textFilter(String text)
	{
		if(text == null || text.length() == 0) return null;

		for(int i = 0; i < text.length(); i++)
			if(Character.UnicodeBlock.of(text.charAt(i)) != Character.UnicodeBlock.BASIC_LATIN)
			{
				return null;
			}

		text = text.replaceAll("\t|\r|\n|\r\n|\f", " ");
		text = text.replaceAll("'", "");
		text = text.replace('\\', ' ');
		return text;
	}
	
	public Connection connection()
	{
	  try {
		  Class.forName(DRIVER_NAME);
		    String url = Config.getProperty(DB_URL);
		    String userName = Config.getProperty(USR_NAME);
		    String pwd = Config.getProperty(PASSWORD);
		    conn = DriverManager.getConnection(url,userName,pwd);
	    // log.info("Connected to Drishti!!");
	    
	  } catch(Exception e) {
	   // log.error(e.getMessage());
	   // log.error("COULD NOT Connect to Drishti!!");
	  }
	      return conn;
	}
	
	
	public static void main(String[] args)
	{
		String imgBasePath = Config.getProperty("context") + Constant.RESULT_DS;
		String tempDir = Config.getProperty("tempDir");
		String url = Config.getProperty("twtrURL");
		FrameParameters fp = new FrameParameters(1*60*60*1000, 0, 2, 2, 24, -125, 50, -66);//CHANGED TO 6 MINUTES for testing...change back !!!!*****
		FrameParameters fpFinal= new FrameParameters(5*60*1000, 0, 0.1, 0.1, 24, -125, 50, -66);
	
		// 1. Twitter-Obama
		TwitterWrapper wrapper = new TwitterWrapper(url, "test", fp, true);
		wrapper.setBagOfWords(new String[]{"test","test2","test3"});
		STTEmageIterator EIterObama = new STTEmageIterator();
		EIterObama.setSTTPointIterator(wrapper);
		
		STMerger mergerObama = new STMerger(fpFinal);
		SpatialMapper sp = SpatialMapper.repeat;
		TemporalMapper tp = TemporalMapper.repeat;		
		mergerObama.addIterator(EIterObama, sp, tp);
		mergerObama.setMergingExpression("mulED(R0,1)");
		DataProcess process = new DataProcess(mergerObama, EIterObama, wrapper, tempDir + "0_Twitter-test", imgBasePath + "0", "0");
		new Thread(process).start();
		/*
		try {

			long timeWindow = 1000*60*60; //*60*24*2;//the last 2 days
			long syncAtMilliSec = 1000;

			double latUnit = 2;
			double longUnit = 2;
			double swLat = 24;
			double swLong = -125;
			double neLat = 50;
			double neLong = -66;

			FrameParameters params = new FrameParameters(timeWindow, syncAtMilliSec, latUnit,longUnit, swLat,swLong , neLat, neLong);
			String url = Config.getProperty("twtrURL");    

			//ds1
			TwitterWrapper wrapper = new TwitterWrapper(url, "Flu", params, true);       	
			//To get the population
			wrapper.getPopulation();

			wrapper.setBagOfWords(new String[]{"obama","president","barack"});
			wrapper.run();

			while (wrapper.hasNext())
			{
				STTPoint a = wrapper.next();
				wrapper.log.info(a.latitude + " " + a.longitude + " " + a.value + " , ");
			}

		} catch(Exception e) {
			log.error(e.getMessage());
		}
		*/
		System.out.println("done");
		System.exit(0);
	}
}
