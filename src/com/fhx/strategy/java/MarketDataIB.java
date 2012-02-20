package com.fhx.strategy.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.marketcetera.event.AskEvent;
import org.marketcetera.event.BidEvent;
import org.marketcetera.event.TradeEvent;
import org.marketcetera.marketdata.MarketDataRequest;
import org.marketcetera.marketdata.MarketDataRequest.Content;
import org.marketcetera.marketdata.interactivebrokers.LatestMarketData;
import org.marketcetera.strategy.java.Strategy;

/**
 * Strategy that receives IB market data
 *
 * @author 
 * @version $Id$
 * @since $Release$
 */
public class MarketDataIB extends Strategy {
    //private static String SYMBOLS = "DIA,SPY,QQQ,IWM,MMM,AA,AXP,T,BAC,BA,CAT,CVX,CSCO,KO,DD,XOM,GE,HPQ,HD,INTC,IBM,JNJ,JPM,KFT,MCD,MRK,MSFT,PFE,PG,TRV,UTX,VZ,WMT,DIS,GS,C,XLK";
	//private static String SYMBOLS = "SPY,IBM,MSFT"; // test symbols
	private static String SYMBOLS = "EUR,GBP,JPY";  // use FX to get real-time tick events for testing
    private static final String MARKET_DATA_PROVIDER = "interactivebrokers"; 
	private static SimpleDateFormat marketTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");	
    
	private static Logger log = Logger.getLogger(MarketDataIB.class);
	
    private List<String> symbolList = new ArrayList<String>();

	// contains the latest market data for subscribed symbols
	private Hashtable<String, LatestMarketData> latestDataCache=new Hashtable<String, LatestMarketData>();
	private BlockingQueue<Hashtable<String, LatestMarketData>> mdQueue;
	private MarketDataHandler mdHandle;
	private int tickFrequency = Integer.parseInt(System.getProperty("tickFrequency", "1")); // in seconds
	
	private AtomicLong tickCount = new AtomicLong(0);
	
    /**
     * Executed when the strategy is started.
     * Use this method to set up data flows
     * and other initialization tasks.
     */
    @Override
    public void onStart() {

    	//loadSymbolsFromFile();
    	
    	for (String symbol : SYMBOLS.split(",")) {
    		if (!symbolList.contains(symbol)) {
    			symbolList.add(symbol);	
    		}
		}

    	Collections.sort(symbolList);  // sort it for fast binary search of file handle index

		// create market data object for each symbol
		StringBuilder sb = new StringBuilder();
		for (String symbol : symbolList) {
			latestDataCache.put(symbol, new LatestMarketData(symbol));
			
			sb.append(symbol);
			sb.append(",");
		}
		SYMBOLS = sb.replace(sb.lastIndexOf(","), sb.length(), "").toString();
		log.info("XXXX Subscribing to market data for symbols: " + SYMBOLS);
		// this goes to metc logs
		warn("XXXX Subscribed symbols: ");
		warn(Arrays.toString(symbolList.toArray()));
		
		// send update to work thread every 5 seconds
		ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(5);
		
		// start the market data processing thread
		this.mdQueue = new LinkedBlockingQueue<Hashtable<String, LatestMarketData>>();
		mdHandle = new MarketDataHandler(symbolList, mdQueue);
		stpe.execute(mdHandle);

		TickDataContainer.INSTANCE.init();
		
		// start the market data update thread
		stpe.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					mdQueue.put(latestDataCache);
					
					log.info("ZZZZZ: Add ticks to TickDataContainer\n");
					/*
					 * Collect tick data in the TickDataContainer
					 */
					TickDataContainer.INSTANCE.addATick(latestDataCache);
					
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}		
		}, 0, tickFrequency, TimeUnit.SECONDS);
		
		log.info("XXXX calling requestMarketData(): ");
        requestMarketData(MarketDataRequest.newRequest().
                withSymbols(SYMBOLS).
                fromProvider(MARKET_DATA_PROVIDER).
                withContent(Content.LATEST_TICK, Content.TOP_OF_BOOK));
    }

    /**
     * Executed when the strategy receives an ask event.
     *
     * @param inAsk the ask event.
     */
    @Override
    public void onAsk(AskEvent inAsk) {
        warn("Ask " + inAsk);
        
    	tickCount.getAndIncrement();
        LatestMarketData data = latestDataCache.get(inAsk.getSymbolAsString());
        data.setOfferPrice(inAsk.getPrice());
        data.setTime(inAsk.getTimestampAsDate());
    }

    /**
     * Executed when the strategy receives a bid event.
     *
     * @param inBid the bid event.
     */
    @Override
    public void onBid(BidEvent inBid) {
    	tickCount.getAndIncrement();
    	
        warn("Bid " + inBid);        
        LatestMarketData data = latestDataCache.get(inBid.getSymbolAsString());
        data.setBidPrice(inBid.getPrice());
        data.setTime(inBid.getTimestampAsDate());
    }

    /**
     * Executed when the strategy receives a trade event.
     *
     * @param inTrade the ask event.
     */
    @Override
    public void onTrade(TradeEvent inTrade) {
        warn("Trade " + inTrade);
        
        //System.out.println("xxxx Trade 1 " + inTrade);
        LatestMarketData data = latestDataCache.get(inTrade.getSymbolAsString());
        data.setTradePrice(inTrade.getPrice());
        data.setTradeSize(data.getLastestTrade().getSize().add(inTrade.getSize()));
        data.setTime(inTrade.getTimestampAsDate());
        //latestData.put(inTrade.getSymbolAsString(), data);
        
    	tickCount.getAndIncrement();    	
    	if (tickCount.get() % 100000 ==0) {
    		String symbol= inTrade.getSymbolAsString();
    		
    		StringBuilder sb = new StringBuilder();
    		sb.append("XXXX onTrade: ");
    		sb.append(symbol);
    		sb.append(",");
    		sb.append(inTrade.getMessageId());
    		sb.append(",");
    		sb.append(inTrade.getPrice().setScale(4));
    		sb.append(",");
    		sb.append(inTrade.getSize());
    		sb.append(",");
    		sb.append(marketTimeFormat.format(inTrade.getTimestampAsDate()));
    		sb.append("\n");
    		
            log.info("XXXX onTrade= " + tickCount.get() + " | "+ sb.toString());            
    	}
        
    }

    @Override
    public void onOther(Object inEvent)
    {
    	warn("onOther" +inEvent);
    	log.info("XXXX: onOther="+ inEvent);
    }

    // utils: other search method?, faster from a HashMap
    public int getSymbolIndex(String symbol) {
    	return Collections.binarySearch(symbolList, symbol);    	
    }
    
    public void loadSymbolsFromFile() {

    	BufferedReader bufReader =null;  

    	try { 
        	String symbolFile = System.getProperty("input.symbols");
        	File inputFile = new File(symbolFile);
        	
        	// make input file configurable
        	if (!inputFile.exists()) {
        		return ;
        	}
        	bufReader = new BufferedReader(new FileReader(inputFile));

    		log.info("Start loading symbols for marketdata subscription from file: " + inputFile.getName());

    		String line = bufReader.readLine(); // skip the first line (header)
    		while ((line =bufReader.readLine()) !=null) {
    			symbolList.add(line);
    		}
    		log.info("Done: "+inputFile+" EOF reached!!!");

    		bufReader.close();

    	} catch (FileNotFoundException fnfe) {
    		log.info("The file was not found: " + fnfe.getMessage());
    	} catch (IOException ioe) {
    		log.info("An IOException occurred: " + ioe.getMessage());
    	} finally {
    		if (bufReader != null) {
    			try {
    				bufReader.close();
    			} catch (IOException ioe) {
    				ioe.printStackTrace();
    			}
    		}
    	}
    }
}
