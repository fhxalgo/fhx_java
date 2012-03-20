package com.fhx.statstream;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.marketcetera.marketdata.interactivebrokers.LatestMarketData;
import org.marketcetera.trade.Factory;
import org.marketcetera.trade.MSymbol;
import org.marketcetera.trade.OrderID;
import org.marketcetera.trade.OrderSingle;
import org.marketcetera.trade.OrderType;
import org.marketcetera.trade.Side;
import org.marketcetera.trade.TimeInForce;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REXPString;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.RList;
import org.rosuda.REngine.Rserve.RserveException;

import com.fhx.service.ib.marketdata.RequestIDGenerator;
import com.fhx.service.ib.order.IBOrderSender;

/*
 * Singleton class that hold one sliding window worth of data across all symbols
 * When the container is full, the data is served to the StatStreamProto
 */
public class StatStreamHistoricalRunner extends StatStreamServiceBase {

	private static Logger log = Logger.getLogger(StatStreamHistoricalRunner.class);

	private int basicWindowCnt = 1;
	private int basicWindowSize = -1;
	private int bwNum = -1;
	private int m_tickStreamSize = 0;
	private int mktOpenHr,mktOpenMin,mktOpenSec,mktClsHr,mktClsMin,mktClsSec;
	private Date mktOpenTime, mktCloseTime;
	private final Properties config = new Properties();
	private static StatStreamHistoricalService ssService = new StatStreamHistoricalService();
	private static StatStreamHistoricalRunner runner = new StatStreamHistoricalRunner();
	
	private Hashtable<String, List<LatestMarketData>> tickDataCache = new Hashtable<String, List<LatestMarketData>>();
	private List<String> symbols = new ArrayList<String>();
	
	private static BlockingQueue<OrderSingle> orderQ = new ArrayBlockingQueue<OrderSingle>(1024);
	
	/*
	 * Use TreeMap to guarantee ordering Example: IBM -> [LatestMarketData1,
	 * LatestMarketData2, LatestMarketData3, ... ]
	 */
	private Map<String, List<LatestMarketData>> basicWindowTicks = new TreeMap<String, List<LatestMarketData>>();

	@SuppressWarnings("deprecation")
	private StatStreamHistoricalRunner() {	
		try {
			ssService.init();

			PropertyConfigurator.configure("conf/log4j.properties");

			config.load(new FileInputStream("conf/statstream.properties"));

			basicWindowSize = Integer.parseInt(config.getProperty("BASIC_WINDOW_SIZE","24"));
			
			String runDateStr = config.getProperty("RUN_DATE","20120217");
			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
			Date runDate = (Date)formatter.parse(runDateStr);  
			
			mktOpenHr = Integer.parseInt(config.getProperty("MKT_OPEN_HR","9"));
			mktOpenMin = Integer.parseInt(config.getProperty("MKT_OPEN_MIN","30"));
			mktOpenSec = Integer.parseInt(config.getProperty("MKT_OPEN_SEC","0"));
			mktClsHr = Integer.parseInt(config.getProperty("MKT_CLOSE_HR","16"));
			mktClsMin = Integer.parseInt(config.getProperty("MKT_CLOSE_MIN","0"));
			mktClsSec = Integer.parseInt(config.getProperty("MKT_CLOSE_SEC","0"));
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(runDate);
			cal.set(Calendar.HOUR_OF_DAY, mktOpenHr);
			cal.set(Calendar.MINUTE, mktOpenMin);
			cal.set(Calendar.SECOND, mktOpenSec);
			mktOpenTime = cal.getTime();
			
			cal.set(Calendar.HOUR_OF_DAY, mktClsHr);
			cal.set(Calendar.MINUTE, mktClsMin);
			cal.set(Calendar.SECOND, mktClsSec);
			mktCloseTime = cal.getTime();
			
			log.info("Setting market period between " + mktOpenTime.toString() + " and " + mktCloseTime.toString());
			
		} catch (Exception e1) {
			System.out.format("ERROR HERE\n");
			log.error("Error loading config file\n");
			e1.printStackTrace();
			System.exit(1);
		}
	}
	
	public static StatStreamHistoricalRunner getInstance() {
		if(runner != null) 
			return runner;
		else 
			return new StatStreamHistoricalRunner();
	}

	private void getAllSymbols() {
		String fileName = config.getProperty("SYMBOL_FILE");
		if (fileName == null) {
			fileName = "~/dev/FHX/fhx_java/conf/dia.us.csv";
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
			dataDir = "/export/data/";
		}

		String index = config.getProperty("BENCHMARK_INDEX");
		if (index == null) {
			log.error("No benchmark index defined for the run");
			System.exit(1);
		}
		
		symbols.add(index);
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

	private List<LatestMarketData> readTicksFromFile(String dataDir, String symbol) {
		List<LatestMarketData> tickStream = new ArrayList<LatestMarketData>();
		StringTokenizer st;

		final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String dateStr = config.getProperty("RUN_DATE","20120217");
		String fileName = dataDir + symbol + "_"+dateStr+"_tick.csv";
		
		log.info("Loading tick data file " + fileName);

		FileReader fileReader;
		try {
			fileReader = new FileReader(fileName);

			BufferedReader bufferedReader = new BufferedReader(fileReader);

			// skip header
			bufferedReader.readLine();

			String line = null;
			while ((line = bufferedReader.readLine()) != null) {
				st = new StringTokenizer(line, ",");

				LatestMarketData lmd = new LatestMarketData(symbol);

				st.nextToken(); //symbol
				lmd.setBidPrice(new BigDecimal(Double.parseDouble(st.nextToken())));  //bid
				lmd.setOfferPrice(new BigDecimal(Double.parseDouble(st.nextToken())));//ask
				st.nextToken(); //trade price
				st.nextToken(); //trade size
				
				Date time = SDF.parse(st.nextToken());
				if(time.before(mktOpenTime) || time.after(mktCloseTime))
					continue;
				
				lmd.setTime(time);   //source time

				tickStream.add(lmd);
			}

			bufferedReader.close();
			fileReader.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		log.info("Read " + tickStream.size() + " ticks for " + fileName);
		m_tickStreamSize = tickStream.size();
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
		List<List<Double>> midPxNew = new ArrayList<List<Double>>(symbols.size());
		boolean addOnce = false;
		
		final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");	
		RList bwList = new RList();
		
		try {	
			log.info("Creating basic window and pass it to R...");
			
			for(int j=0; j<symbols.size(); j++) {
				symbol = symbols.get(j);
				tickStream = tickDataCache.get(symbol);
				
				List<Double> value = null;
				if(midPxNew.size() <= j) {
					value = new ArrayList<Double>();
					midPxNew.add(j, value);
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
			
			for(int i=0; i<midPxNew.size(); i++) {
				symbol = symbols.get(i);
				val = midPxNew.get(i);
		
				bwList.put(symbol, new REXPDouble(ArrayUtils.toPrimitive(val.toArray(new Double[val.size()]))));
			}			
			
			for(int i=0; i<timeStamp.size(); i++) {
				StringBuffer sb = new StringBuffer();			
				
				sb.append("["+i+"] "+timeStamp.get(i)+"|"+winNum.get(i)+"|");
				for(int j=0; j<midPxNew.size(); j++) {
					val = midPxNew.get(j);
					sb.append(val.get(i)+"|");
				}
				log.info(sb.toString());
			}	
		
		} catch (Exception e) {
			log.error("Whoops error creating data for basic window");
			e.printStackTrace();
	        System.exit(1);
	    }
		
		return bwList;
	}
	
	private synchronized void addOrder(String symbol, String type, int qty, double price) {
		int orderNumber = RequestIDGenerator.singleton().getNextOrderId();
		//order.setNumber(orderNumber);
		OrderSingle order = Factory.getInstance().createOrderSingle();
		order.setOrderID(new OrderID(orderNumber+""));
		
		order.setOrderType(OrderType.Limit);
		order.setQuantity(new BigDecimal(qty));	
		
		double pxDbl = Double.parseDouble(new DecimalFormat("#.##").format(price));
		order.setPrice(new BigDecimal(pxDbl));
		
		if (type.equalsIgnoreCase("buy"))
			order.setSide(Side.Buy);
		else if (type.equalsIgnoreCase("sell"))
			order.setSide(Side.Sell);
		else if (type.equalsIgnoreCase("sellshort"))
			order.setSide(Side.SellShort); 
			
		order.setSymbol(new MSymbol(symbol));
		order.setTimeInForce(TimeInForce.Day);
		
		//Sending order to IB
		log.info("Sending order to IB - "+order.getSide()+" "+order.getQuantity()+" "+order.getSymbol()+" @ "+order.getPrice());
		orderQ.add(order);
	}
	
	public void tick() {
		log.info("Processing basic window " + bwNum);
		if(basicWindowSize*bwNum >= m_tickStreamSize ) {
			log.info("Reached eod of the data stream, simulation done...");
			System.exit(0);
		}
		
		bwNum++;
		
		RList bwList = getBasicWindowRList(bwNum);

		try {
			conn.assign("streamData", REXP.createDataFrame(bwList));

			String corrFunc = "corr_report <- process_basic_window3(streamData)";
			
			log.info("calling process_basic_window");	
			
			REXP retVal = conn.parseAndEval(corrFunc);
			conn.assign("prev_value_list", retVal);
		
			//log.info(conn.eval("paste(capture.output(print(order_list)),collapse='\\n')").asString());
			
			/*
			 * parsing the order list
			 * retVal.asList()[0] is the order list of R data frame
			 * 		"Symbol",	"OrderType",	"Quantity",	"Price",	"BasicWinNum", "Time", "PnL"
			 * 1	ABC			Buy				100			10			1			12:00:00	-
			 * 1	CBA			Sell			100			10			1			12:00:00	-  
			 */			
			RList orderList = retVal.asList().at(0).asList();
			if(orderList != null && orderList.size() > 0 ) {
				int numRows = orderList.at(0).asStrings().length;
				
				String[] symbolColVal = orderList.at(0).asStrings();
				String[] sideColVal = orderList.at(1).asStrings();
				double[] qtyColVal = orderList.at(2).asDoubles();	//qty will be rounded into a round lot
				double[] priceColVal = orderList.at(3).asDoubles(); //TODO: use mid-px at this point to place order

				for(int i = 0; i < numRows; i++) {
					addOrder(symbolColVal[i], 
							 sideColVal[i], 
							 (int)qtyColVal[i],
							 priceColVal[i]);
				}
			}	
			
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
		final StatStreamHistoricalRunner runner = StatStreamHistoricalRunner.getInstance();
		runner.gatherAllHistTicks();

		new Thread(new IBOrderSender(orderQ)).start();
		
		// send update to work thread every 5 seconds
		ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(5);

		// start the market data update thread
		stpe.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				log.info("Running historical tick simulation");
					
				runner.tick();
			}
		}, 0, 5, TimeUnit.SECONDS);
	}

}
