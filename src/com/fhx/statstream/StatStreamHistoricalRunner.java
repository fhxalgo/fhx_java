package com.fhx.statstream;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.marketcetera.marketdata.interactivebrokers.LatestMarketData;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REXPString;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.RList;
import org.rosuda.REngine.Rserve.RserveException;

/*
 * Singleton class that hold one sliding window worth of data across all symbols
 * When the container is full, the data is served to the StatStreamProto
 */
public class StatStreamHistoricalRunner extends StatStreamServiceBase {

	private static Logger log = Logger.getLogger(StatStreamHistoricalRunner.class);

	private int basicWindowCnt = 1;
	private int basicWindowSize = -1;
	private int bwNum = 0;
	private final Properties config = new Properties();
	private static StatStreamHistoricalService ssService = new StatStreamHistoricalService();

	private Hashtable<String, List<LatestMarketData>> tickDataCache = new Hashtable<String, List<LatestMarketData>>();
	private Set<String> symbols = new TreeSet<String>();
	
	/*
	 * Use TreeMap to guarantee ordering Example: IBM -> [LatestMarketData1,
	 * LatestMarketData2, LatestMarketData3, ... ]
	 */
	private Map<String, List<LatestMarketData>> basicWindowTicks = new TreeMap<String, List<LatestMarketData>>();

	public StatStreamHistoricalRunner() {
		try {
			ssService.init();

			PropertyConfigurator.configure("conf/log4j.properties");

			config.load(new FileInputStream("conf/statstream.properties"));

			String basicWin = config.getProperty("BASIC_WINDOW_SIZE");
			if (basicWin == null) {
				log.error("Must define basic window size");
			}
			basicWindowSize = Integer.parseInt(basicWin);
			
		} catch (Exception e1) {
			System.out.format("ERROR HERE\n");
			log.error("Error loading config file\n");
			e1.printStackTrace();
			System.exit(1);
		}
	}

	private void getAllSymbols() {
		String fileName = config.getProperty("SYMBOL_FILE");
		if (fileName == null) {
			fileName = "~/dev/FHX/workspace_java/FHX_sa/conf/xlk.us.csv";
		}

		FileReader fileReader;
		try {
			fileReader = new FileReader(fileName);

			BufferedReader bufferedReader = new BufferedReader(fileReader);

			// skip header
			bufferedReader.readLine();

			String line = null;
			while ((line = bufferedReader.readLine()) != null) {
				symbols.add(line.trim());
			}

			log.info("Gathered " + symbols.size() + " symbols");
			bufferedReader.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/*
	 * Replay all the ticks from the tick data files
	 */
	public void gatherAllHistTicks() {
		String dataDir = config.getProperty("TICKDATA_DIR");
		if (dataDir == null) {
			dataDir = "/export/data/statstream/";
		}

		getAllSymbols();

		Iterator<String> iter = symbols.iterator();
		while (iter.hasNext()) {
			String symbol = (String) iter.next();

			if (tickDataCache.contains(symbol)) {
				log.error("Should not happen, found duplicate symbol " + symbol
						+ " in tick data directory");
			} else {
				List<LatestMarketData> tickStream = readTicksFromFile(dataDir,
						symbol);
				tickDataCache.put(symbol, tickStream);
			}
		}
	}

	private List<LatestMarketData> readTicksFromFile(String dataDir,
			String symbol) {
		List<LatestMarketData> tickStream = new ArrayList<LatestMarketData>();
		StringTokenizer st;
		BigDecimal bid, ask;
		Date latestTime;

		String fileName = dataDir + "_Tick_5Min.csv";

		FileReader fileReader;
		try {
			fileReader = new FileReader(fileName);

			BufferedReader bufferedReader = new BufferedReader(fileReader);

			// skip header
			bufferedReader.readLine();

			String line = null;
			while ((line = bufferedReader.readLine()) != null) {
				st = new StringTokenizer(line, ",");

				// TODO: string tokenize

				LatestMarketData lmd = new LatestMarketData(symbol);
				lmd.setBidPrice(new BigDecimal(0.0));
				lmd.setOfferPrice(new BigDecimal(0.0));
				lmd.setTime(new Date());
				tickStream.add(lmd);
			}

			bufferedReader.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		log.info("Read " + tickStream.size() + " ticks for " + fileName);
		return tickStream;
	}

	public void flushBasicWindow() {
		log.info("Serving basic window # " + basicWindowCnt	+ " to the StatStream model");

		/*
		 * format basic window data and serve it to StatStream model
		 */
		ssService.tick(basicWindowTicks, basicWindowCnt++);

		log.info("Flushing basic window " + basicWindowCnt
				+ ", invoking StatStreamService");
		log.info("Size of basic window = "
				+ basicWindowTicks.values().iterator().next().size());
		basicWindowTicks.clear();
	}

	public void addATick(Map<String, LatestMarketData> aTick) {
		String symbol = "";
		LatestMarketData data;

		for (Map.Entry<String, LatestMarketData> tick : aTick.entrySet()) {
			symbol = tick.getKey();
			data = tick.getValue();

			List<LatestMarketData> ticksPerSymbol = basicWindowTicks.get(symbol);
			if (ticksPerSymbol == null) {
				log.info("initializing arraylist for symbol " + symbol);
				ticksPerSymbol = new ArrayList<LatestMarketData>();
				ticksPerSymbol.add(data);
				basicWindowTicks.put(symbol, ticksPerSymbol);
			} else {
				ticksPerSymbol.add(data);
			}
		}

		if (basicWindowTicks.get(symbol).size() >= basicWindowSize)
			flushBasicWindow();
	}

	protected RList getBasicWindowRList(int bwNum) {
		String symbol;
		List<LatestMarketData> tickStream;
		List<String> timeStamp = new ArrayList<String>();
		List<Integer> winNum = new ArrayList<Integer>();
		LatestMarketData md;
		List<Double> val;
		Map<String, List<Double>> midPx = new HashMap<String, List<Double>>();
		boolean addOnce = false;
		
		final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");	
		RList bwList = new RList();
		
		try {	
			log.info("Creating basic window and pass it to R...");
			
			for(Map.Entry<String, List<LatestMarketData>> entry : tickDataCache.entrySet()) {
				symbol = entry.getKey();
				tickStream = entry.getValue();
				
				List<Double> value = midPx.get(symbol);
				if(value==null) {
					value = new ArrayList<Double>();
					midPx.put(symbol, value);
				}
				
				for(int i=basicWindowSize*bwNum; i<basicWindowSize*(bwNum+1); i++) {
					md =tickStream.get(i);
					value.add(md.getLatestBid().getPrice().add(md.getLatestOffer().getPrice()).divide(new BigDecimal(2)).doubleValue());
					
					if(!addOnce) {
						timeStamp.add(SDF.format(md.getTime()));
						winNum.add(bwNum);
					}
				}
				addOnce = true;
			}
			
			bwList.put("timestamp", new REXPString(timeStamp.toArray(new String[timeStamp.size()])));
			bwList.put("winNum", new REXPInteger(ArrayUtils.toPrimitive(winNum.toArray(new Integer[winNum.size()]))));
			
			for(Map.Entry<String, List<Double>> entry : midPx.entrySet()) {
				symbol = entry.getKey();
				val = entry.getValue();
				
				bwList.put(symbol, new REXPDouble(ArrayUtils.toPrimitive(val.toArray(new Double[val.size()]))));
			}			
			
			for(int i=0; i<timeStamp.size(); i++) {
				StringBuffer sb = new StringBuffer();			
				Iterator<Map.Entry<String, List<Double>>> iter = midPx.entrySet().iterator();
			
				sb.append("["+i+"] "+timeStamp.get(i)+"|"+winNum.get(i)+"|");
				while(iter.hasNext()) {
					val = (List<Double>) iter.next().getValue();
					sb.append(val.get(i)+"|");
				}
				log.info(sb.toString());
			}	
		
		} catch (Exception e) {
	            System.err.println(e);
	            System.exit(1);
	    }
		
		return bwList;
	}
	
	public void tick() {
		log.info("Processing basic window " + bwNum++);
		
		RList bwList = getBasicWindowRList(bwNum);
		log.info("RList getBasicWindowRList: " + bwList);

		try {
			conn.assign("streamData", REXP.createDataFrame(bwList));

			// make sure all global variables that func needs exist
			REXP cmd_ls_vars, streamDataTest, chopChunk, bwDat;

			cmd_ls_vars = conn.parseAndEval("ls()");
			streamDataTest = conn.parseAndEval("streamData");
			chopChunk = conn.parseAndEval("chopChunk");

			log.info("cmd_ls_vars(debug): " + cmd_ls_vars.toDebugString());
			log.info("streamData(debug): " + streamDataTest.toDebugString());
			log.info("chopChunk(debug): " + chopChunk.toDebugString());
			
			// String corrFunc = "corr_report <- process_sliding_window2(1)";
			String corrFunc = "corr_report <- process_basic_window3(streamData)";
			
			int m = conn.parseAndEval(corrFunc).asInteger();

			log.info("return from process_basic_window call = " + m);	
	
			chopChunk = conn.parseAndEval("chopChunk");
			log.info("chopChunk(debug): " + chopChunk.toDebugString());
			
			bwDat = conn.parseAndEval("bwdat");
			log.info("bwDat(debug): " + bwDat.toDebugString());
			
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (REngineException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public boolean tick(Map<String, List<LatestMarketData>> aTick, int bwNum) {
		return true;
	}

	public static void main(String... aArgs) {
		final StatStreamHistoricalRunner runner = new StatStreamHistoricalRunner();
		runner.init();
		runner.gatherAllHistTicks();

		// send update to work thread every 5 seconds
		ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(5);

		// start the market data update thread
		stpe.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				log.info("Running historical tick simulation\n");
					
				runner.tick();
			}
		}, 0, 5, TimeUnit.SECONDS);
	}

}
