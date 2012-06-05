package com.fhx.service.ib.marketdata;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.ib.client.Contract;

import org.marketcetera.trade.*;;

//public class IBOrderService implements IBService {
public class IBOrderService extends IBOrderEventListener {

	private static final long serialVersionUID = -7426452967133280762L;
	private static Logger log = Logger.getLogger(IBOrderService.class);
	final private static String[] SYMBOLS = {"DIA","SPY","QQQ","IWM","MMM","AA","AXP","T","BAC","BA",
		"CAT","CVX","CSCO","KO","DD","XOM","GE","HPQ","HD","INTC",
		"IBM","JNJ","JPM","KFT","MCD","MRK","MSFT","PFE","PG","TRV",
		"UTX","VZ","WMT","DIS","GS","C"};
	
	private static boolean faEnabled = Boolean.parseBoolean(System.getProperty("ib.faEnabled"));
	private static String faAccount = System.getProperty("ib.faAccount");
	private static String group = System.getProperty("ib.group");
	private static String openMethod = System.getProperty("ib.openMethod");
	private static String closeMethod = System.getProperty("ib.closeMethod");

	private static IBClient client;

	private static boolean simulation = Boolean.parseBoolean(System.getProperty("simulation"));

	static IBOrderService INSTANCE = new IBOrderService();
	
	private IBOrderService() {
		PropertyConfigurator.configure("conf/log4j.properties");
	}
	
	public static IBOrderService getInstance() {
		if(INSTANCE != null)
			return INSTANCE;
		else
			return new IBOrderService();
	}
	
	public void handleInit() {

		if (!simulation) {
			client = IBClient.getDefaultInstance();
		}
	}
	
	public void handleInitWatchlist() {

		if ((client.getIbAdapter().getState().equals(ConnectionState.READY) || client.getIbAdapter().getState().equals(ConnectionState.SUBSCRIBED))
				//&& !client.getIbAdapter().isRequested() && !simulation) {
				&& !client.getIbAdapter().isRequested() ) {

			client.getIbAdapter().setRequested(true);
			client.getIbAdapter().setState(ConnectionState.SUBSCRIBED);

		}
	}

	public void sendNewOrder(String symbol, String type, int size, double price) throws Exception {

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
		
		sendOrModifyOrder(order);
	}

	protected void modifyOrder(OrderSingle order) throws Exception {

		sendOrModifyOrder(order);
	}

	protected void cancelOrder(OrderSingle order) throws Exception {
	
		if (!(client.getIbAdapter().getState().equals(ConnectionState.READY) || client.getIbAdapter().getState().equals(ConnectionState.SUBSCRIBED))) {
			log.error("transaction cannot be executed, because IB is not connected");
			return;
		}
	
		client.cancelOrder(Integer.parseInt(order.getOrderID().getValue()));

		log.info("requested order cancallation for order: " + order);
	}

	protected void cancelOrderById(int orderId) throws Exception {
		
		if (!(client.getIbAdapter().getState().equals(ConnectionState.READY) || client.getIbAdapter().getState().equals(ConnectionState.SUBSCRIBED))) {
			log.error("transaction cannot be executed, because IB is not connected");
			return;
		}
	
		client.cancelOrder(orderId);

		log.info("requested order cancallation for order: " + orderId);
	}
	
	protected void reqOpenOrders() throws Exception {
		
		if (!(client.getIbAdapter().getState().equals(ConnectionState.READY) || client.getIbAdapter().getState().equals(ConnectionState.SUBSCRIBED))) {
			log.error("transaction cannot be executed, because IB is not connected");
			return;
		}
	
		//client.reqOpenOrders();
		//log.info("reqOpenOrders: ");
		
		client.reqAllOpenOrders();
		log.info("reqAllOpenOrders: ");
	}
	/**
	 * helper method to be used in both sendorder and modifyorder.
	 * @throws Exception
	 */
	public void sendOrModifyOrder(OrderSingle order) throws Exception {

		if (!(client.getIbAdapter().getState().equals(ConnectionState.READY) || client.getIbAdapter().getState().equals(ConnectionState.SUBSCRIBED))) {
			log.error("transaction cannot be executed, because IB is not connected");
			return;
		}
		
		Contract contract = IBUtil.getContract(order.getSymbol().getFullSymbol());

		com.ib.client.Order ibOrder = new com.ib.client.Order();
		ibOrder.m_action = order.getSide().name();
		ibOrder.m_orderType = IBUtil.getIBOrderType(order);
		ibOrder.m_transmit = true;
		ibOrder.m_orderType = OrderType.Market.toString();

		// handling for financial advisor accounts
		if (faEnabled) {
			// doesn't apply 
		} else {
			ibOrder.m_totalQuantity = (int) order.getQuantity().intValue();

			// if fa is disabled, it is still possible to work with an IB FA setup if a single client account is specified
			if (faAccount != null) {
				ibOrder.m_account = faAccount;
			}
		}

		//set the limit price if order is a limit order or stop limit order
		//if (order.getOrderType().equals(OrderType.Limit)) {
		//	ibOrder.m_lmtPrice = order.getPrice().doubleValue();
		//}

		//set the stop price if order is a stop order or stop limit order
		//if (order instanceof StopOrderInterface) {
		//	ibOrder.m_auxPrice = ((StopOrderInterface) order).getStop().doubleValue();
		//}

		// progapate the order to all corresponding esper engines
		//propagateOrder(order);

		// place the order through IBClient
		client.placeOrder(Integer.parseInt(order.getOrderID().getValue()), contract, ibOrder);
		submittedOrders.put(order.getOrderID().getValue(), ibOrder);

		log.info("placed or modified order: " + order);
	}
	
	public static void main(String[] args) {
		//sendNewOrder(String symbol, String type, int size, double price)
		
		String reqStr = System.getProperty("reqInterval", "5");
		int reqInterval = Integer.parseInt(reqStr);
		
		final IBOrderService ors = IBOrderService.getInstance();
		IBEventServiceImpl.getEventService().addOrderEventListener(ors);
		
		try {
			ors.handleInit();
			Thread.sleep(10*1000);
			ors.handleInitWatchlist();

			// get all open orders if any
			
			ors.sendNewOrder("IBM", "buy", 100, 0.99);
			//ors.cancelOrderById(9);

			
			while(true) {
				// make sure the thread doesn't end
				printOrders(ors.submittedOrders, "submitted ");
				Thread.sleep(1*1000);
				printOrders(ors.openOrders, "open ");
				Thread.sleep(1*1000);
				printOrders(ors.execOrders, "executed ");
				Thread.sleep(1*1000);
				printOrders(ors.cancelledOrders, "cancelled ");

				log.info("main thread--sleeping...");
				Thread.sleep(10*1000);
				
				// send test orders
				int idx = (new Random()).nextInt(SYMBOLS.length);
				ors.sendNewOrder(SYMBOLS[idx], "buy", 100, 0.99);
				ors.reqOpenOrders();
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		finally {
			// make sure order failure is handled here
		}
		
	}
	
	public static void printOrders(Map<String, com.ib.client.Order> orders, String type) {
		StringBuffer sb = new StringBuffer();
		
		sb.append(type + " orders: \n");
		
		for(Map.Entry<String, com.ib.client.Order> ord : orders.entrySet())
		{
			com.ib.client.Order o = ord.getValue();
			sb.append("orderId="+ord.getKey());
			sb.append(",size="+o.m_action);
			sb.append(",totalQty="+o.m_totalQuantity);
			sb.append(",price="+o.m_lmtPrice);
			sb.append("\n");
		}
		log.info(sb.toString());
	}
	
	
	// order management
	private Map<String, com.ib.client.Order> submittedOrders = new HashMap<String, com.ib.client.Order>();
	private Map<String, com.ib.client.Order> openOrders = new HashMap<String, com.ib.client.Order>();
	private Map<String, com.ib.client.Order> execOrders = new HashMap<String, com.ib.client.Order>();
	private Map<String, com.ib.client.Order> cancelledOrders = new HashMap<String, com.ib.client.Order>();

//	//@Override
//	public void ibDataReceived(int reqId, IBEvent event) {
//		// process specific IB callback events
//		// Type: OpenOrder, OrderStatus, Error
//		
//	}
	
	@Override
	public void onIBEvent(IBEventData event) {
		// TODO Auto-generated method stub
		
		if (event.getEventType()==IBEventType.NextValidId) {
			RequestIDGenerator.singleton().initializeOrderId(event.getNextOrderId());
		}
		
		log.info("onIBEvent callback: event type " + event.getEventType());
		
		if(submittedOrders.size()>0)
			printOrders(submittedOrders, "submitted ");
		if(openOrders.size()>0)
			printOrders(openOrders, "open ");
		if(execOrders.size()>0)
			printOrders(execOrders, "executed ");
		if(cancelledOrders.size()>0)
			printOrders(cancelledOrders, "cancelled ");
	}
	
}
