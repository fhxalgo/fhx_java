package com.fhx.service.ib.order;

import java.math.BigDecimal;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.marketcetera.trade.Factory;
import org.marketcetera.trade.MSymbol;
import org.marketcetera.trade.OrderID;
import org.marketcetera.trade.OrderSingle;
import org.marketcetera.trade.OrderType;
import org.marketcetera.trade.Side;
import org.marketcetera.trade.TimeInForce;

import com.fhx.service.ib.marketdata.IBEventServiceImpl;
import com.fhx.service.ib.marketdata.IBOrderService;
import com.fhx.service.ib.marketdata.RequestIDGenerator;

public enum IBOrderSender implements Runnable {
	INSTANCE;
	
	private static Logger log = Logger.getLogger(IBOrderService.class);
	
	private IBOrderService ors = IBOrderService.getInstance();
	
	private BlockingQueue<OrderSingle> orderQ = new ArrayBlockingQueue<OrderSingle>(1024);
	
	public void run() {
		IBEventServiceImpl.getEventService().addOrderEventListener(ors);
		ors.handleInit();			
		ors.handleInitWatchlist();
		
		log.info("Starting IBOrderSender thread");
		
		while (true)
			try {
				OrderSingle order = orderQ.take();
				log.info("took next order off q: " + order.getSide()+" "+order.getSymbol()+" "+order.getQuantity()+"@"+order.getPrice());
				
				ors.sendOrModifyOrder(order);
				log.info("sent order to IB");
				
			} catch (InterruptedException e) {
				log.error("thread interrupted error " + e.getMessage() );
				System.exit(1);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
	public void addOrder(String symbol, String type, int size, double price) {
		int orderNumber = RequestIDGenerator.singleton().getNextOrderId();
		//order.setNumber(orderNumber);
		OrderSingle order = Factory.getInstance().createOrderSingle();
		order.setOrderID(new OrderID(orderNumber+""));
		
		//order.setOrderType(OrderType.Market);
		order.setOrderType(OrderType.Limit);
		order.setQuantity(new BigDecimal(size));
		order.setPrice(new BigDecimal(price));
		
		if (type.equalsIgnoreCase("buy"))
			order.setSide(Side.Buy);
		else if (type.equalsIgnoreCase("sell"))
			order.setSide(Side.Sell);
		else if (type.equalsIgnoreCase("sellshort"))
			order.setSide(Side.SellShort); 
			
		order.setSymbol(new MSymbol(symbol));
		order.setTimeInForce(TimeInForce.Day);
		
		orderQ.add(order);
	}

}
