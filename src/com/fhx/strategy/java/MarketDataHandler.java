package com.fhx.strategy.java;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.marketcetera.marketdata.interactivebrokers.LatestMarketData;

public class MarketDataHandler implements Runnable {
	private static Logger log = LogManager.getLogger(MarketDataHandler.class);

	private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
	private static SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");	
    
    private List<String> symbolList;
	private RandomAccessFile[] symbolDataFileHandles;

	private BlockingQueue<Hashtable<String, LatestMarketData>> mdQueue;
	private int sleepInterval = 100; // default to 100 milliseconds
	
	public MarketDataHandler(List<String> symbolList, BlockingQueue<Hashtable<String, LatestMarketData>> mdQueue) {
		this.symbolList = symbolList;
		this.mdQueue = mdQueue;
		
		// create file handle for each symbol
		// we store data each symbol file
		String fileDate = DATE_FORMAT.format(new Date());
		
		try {
    		File dataDir = new File("/export/data/"+fileDate);
    		if (!dataDir.exists()) {
    			dataDir.mkdirs();
    		}
    		
    		System.out.println("$$$ handling market data for " + symbolList.size() + " symbols.");
    		
    		symbolDataFileHandles = new RandomAccessFile[symbolList.size()];
    		String dataDirPath = dataDir.getAbsolutePath();
    		
			for (int i=0; i < symbolList.size(); i++) {
	    		symbolDataFileHandles[i] = new RandomAccessFile(dataDirPath+"/"+symbolList.get(i)+"_"+fileDate+"_tick.csv","rw");
	    		
	    		// write header 
	    		symbolDataFileHandles[i].writeBytes("Symbol,Bid,Ask,TradePrice,TradeSize,SourceTime,CreateTime\n");
			}
			
			// start tick data processing  
			TickDataContainer.INSTANCE.init();
			 
		}
		catch(FileNotFoundException e){
			e.printStackTrace();
		}
		catch(IOException e){
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void run() {
		// create basic window ticks 
		while(true) {
			try {
				Hashtable<String, LatestMarketData> ticks = this.mdQueue.take();

				// only log between 9:25 and 16:05
			    Calendar cal = Calendar.getInstance();
			    
			    int hh = cal.get(Calendar.HOUR_OF_DAY);
			    int mm = cal.get(Calendar.MINUTE);
			    
			    if (hh < 9 || (hh <9 && mm <25))
			    	continue;			    	
			    else if (hh > 16 || (hh>16 && mm > 5))
			    	continue; 
			    else {
			    	
			    	for (Map.Entry<String, LatestMarketData> tick : ticks.entrySet()) {
			    		String symbol = tick.getKey();
			    		LatestMarketData data = tick.getValue();

			    		// write to file
			    		int fileHandleIdx = getSymbolIndex(symbol);

			    		if (fileHandleIdx >=0 && fileHandleIdx <= symbolDataFileHandles.length) {
			    			symbolDataFileHandles[fileHandleIdx].writeBytes(data.toString());
			    		}
			    		else {
			    			System.err.println("ERROR: no file handle is available for symbol: " + symbol);
			    		}
			    	}
			    }
				
				//Thread.sleep(sleepInterval);
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
    public int getSymbolIndex(String symbol) {
    	return Collections.binarySearch(symbolList, symbol);    	
    }

//    public void initTickDataContainer() {
//		log.info("Starting tick data container collection thread...");
//		sNotifierPool.submit(new Runnable() {
//            public void run() {
//         	   try {
//         		  TickDataContainer.INSTANCE.init();
//         	   } catch (Exception e) {
//         		   // TODO Auto-generated catch block
//         		   e.printStackTrace();
//         	   }
//            }
//		});
//    }
}
