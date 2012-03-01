package com.fhx.service.ib.marketdata;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;

import org.apache.log4j.Logger;

import com.ib.client.EClientSocket;

public final class IBClient extends EClientSocket {

	private static Logger logger = Logger.getLogger(IBClient.class.getName());

//	private static int defaultClientId = ConfigurationUtil.getBaseConfig().getInt("ib.defaultClientId"); //0
//	private static int port = ConfigurationUtil.getBaseConfig().getInt("ib.port"); //7496;//
//	private static String host = ConfigurationUtil.getBaseConfig().getString("ib.host"); // "127.0.0.1";
//	private static long connectionTimeout = ConfigurationUtil.getBaseConfig().getInt("ib.connectionTimeout"); //10000;//

	private static int clientId=0;
    private static int port=7496;  // connect to TWS webclient
    //private static int port=4001;  // connect to IB gateway
	private static String host = "127.0.0.1";
	private static long connectionTimeout = 10000;

	private static int defaultClientId = 7;  // for historical request to keep IBGW alive
	private static IBClient instance;

	public IBClient(int clientId, IBDefaultAdapter wrapper) {

		super(wrapper);
		IBClient.clientId = clientId;
	}

	public static IBClient getDefaultInstance() {

		if (instance == null) {
			
			String ibPort = System.getProperty("port");
			String ibClientId = System.getProperty("clientId");
			
			if ( ibPort !=null ) {
				port = Integer.parseInt(ibPort);
			}
			if (ibClientId !=null) {
				clientId = Integer.parseInt(ibClientId);
				defaultClientId = Integer.parseInt(ibClientId);	
			}

			instance = new IBClient(defaultClientId, new IBDefaultAdapter(defaultClientId));

			// connect in a separate thread, because it might take a while
			(new Thread() {
				@Override
				public void run() {
					System.out.println("xxxx IBClient...connecting");
					instance.connect();
				}
			}).start();
		}
		return instance;
	}

	public IBDefaultAdapter getIbAdapter() {
		return (IBDefaultAdapter) super.wrapper();
	}

	public void connect() {
		
		System.out.println("xxxx IBClient...connect(1)");
		if (isConnected()) {
			eDisconnect();

			sleep();
		}
		System.out.println("xxxx IBClient...connect(2)");
		this.getIbAdapter().setRequested(false);
		System.out.println("xxxx IBClient...connect(3)");
		while (!connectionAvailable()) {
			sleep();
		}
		
		System.out.format("xxxx IBClient: %d...connect(host=%s, port=%d) \n", clientId, host, port);
		eConnect(host, port, clientId);

		if (isConnected()) {
			this.getIbAdapter().setState(ConnectionState.READY);

			System.out.println("xxxx IBClient...connected");
			// in case there is no 2104 message from the IB Gateway (Market data farm connection is OK)
			// manually invoke initWatchlist after some time
			sleep();
			//ServiceLocator.commonInstance().getMarketDataService().initWatchlist();
		}
		
		System.out.println("xxxx IBClient...haha");
	}

	private void sleep() {

		try {
			Thread.sleep(connectionTimeout);
		} catch (InterruptedException e1) {
			try {
				// during eDisconnect this thread get's interrupted so sleep again
				Thread.sleep(connectionTimeout);
			} catch (InterruptedException e2) {
				logger.error("problem sleeping", e2);
			}
		}
	}

	public void disconnect() {

		if (isConnected()) {
			eDisconnect();
		}
	}

	private static synchronized boolean connectionAvailable() {
		try {
System.out.format("xxxx host: %s, port: %d \n", host, port);			
			Socket socket = new Socket(host, port);
			socket.close();
			return true;
		} catch (ConnectException e) {
			// do nothing, gateway is down
			return false;
		} catch (IOException e) {
			logger.error("connection error", e);
			return false;
		}
	}
}