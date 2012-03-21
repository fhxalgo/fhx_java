package com.fhx.statstream;

import java.util.List;
import java.util.Map;

import org.marketcetera.marketdata.interactivebrokers.LatestMarketData;

public class StatStreamHistoricalService extends StatStreamServiceBase {
	
	public StatStreamHistoricalService() {
		//super.init();
	}

	@Override
	public boolean tick(Map<String, List<LatestMarketData>> aTick, int bwNum) {
		
		
		return true;
	}
}
