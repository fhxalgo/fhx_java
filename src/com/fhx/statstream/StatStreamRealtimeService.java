package com.fhx.statstream;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.marketcetera.marketdata.interactivebrokers.LatestMarketData;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REXPString;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.RList;
import org.rosuda.REngine.Rserve.RserveException;

public class StatStreamRealtimeService extends StatStreamServiceBase {

	private static Logger log = Logger
			.getLogger(StatStreamRealtimeService.class);
	
	List<String> m_timeStamp = new ArrayList<String>();
	List<Integer> m_winNum = new ArrayList<Integer>();
	Map<String, List<Double>> m_midPx = new HashMap<String, List<Double>>();

	public StatStreamRealtimeService() {
	}

	@Override
	public boolean tick(Map<String, List<LatestMarketData>> aTick, int bwNum) {
		RList bwList = getBasicWindowRList(aTick, bwNum);
		log.info("RList getBasicWindowRList: " + bwList);

		try {
			conn.assign("streamData", REXP.createDataFrame(bwList));

			// this is nice!!!
			// String nas =
			// conn.eval("paste(capture.output(print(curBW)),collapse='\\n')").asString();
			// System.out.println(nas);
			
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
		return true;
	}
	
	protected RList getBasicWindowRList(Map<String, List<LatestMarketData>> aTick, int bwNum) {
		BigDecimal bid, ask, mid;
		String symbol;
		List<LatestMarketData> basicWindowData;
		List<Double> val;
		
		boolean addOnce = false;
		final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");	
		RList bwList = new RList();
		
		try {	
			log.info("Creating basic window and pass it to R...");
			
			for(Map.Entry<String, List<LatestMarketData>> entry : aTick.entrySet()) {
				symbol = entry.getKey();
				basicWindowData = entry.getValue();
				
				List<Double> value = m_midPx.get(symbol);
				if(value==null) {
					value = new ArrayList<Double>();
					m_midPx.put(symbol, value);
				}
				
				for(int timePt = 0; timePt < basicWindowData.size(); timePt++) {	
					bid = basicWindowData.get(timePt).getLatestBid().getPrice();
					ask = basicWindowData.get(timePt).getLatestOffer().getPrice();
					mid = bid.add(ask).divide(new BigDecimal(2));
					mid.setScale(4);   // two decimal points
					value.add(mid.doubleValue());	
					
					if(!addOnce) {
						if(basicWindowData.get(timePt).getTime() == null) {
							m_timeStamp.add(SDF.format(new Date()));
						}
						else {
							m_timeStamp.add(SDF.format(basicWindowData.get(timePt).getTime()));
						}
						m_winNum.add(bwNum);
					}
				}
				addOnce = true;
			}		
			
			assert(m_timeStamp.size()==m_winNum.size());	
			bwList.put("timestamp", new REXPString(m_timeStamp.toArray(new String[m_timeStamp.size()])));
			bwList.put("winNum", new REXPInteger(ArrayUtils.toPrimitive(m_winNum.toArray(new Integer[m_winNum.size()]))));
			
			for(Map.Entry<String, List<Double>> entry : m_midPx.entrySet()) {
				symbol = entry.getKey();
				val = entry.getValue();
				
				assert(m_timeStamp.size()==val.size());
				
				bwList.put(symbol, new REXPDouble(ArrayUtils.toPrimitive(val.toArray(new Double[val.size()]))));
			}
			
			for(int i=0; i<m_timeStamp.size(); i++) {
				StringBuffer sb = new StringBuffer();			
				Iterator<Map.Entry<String, List<Double>>> iter = m_midPx.entrySet().iterator();
			
				sb.append("["+i+"] "+m_timeStamp.get(i)+"|"+m_winNum.get(i)+"|");
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
}
