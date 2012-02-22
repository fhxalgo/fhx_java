package com.fhx.util;

import java.io.File;

import org.apache.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;

public class StatStreamUtil {
	
	private static Logger log = Logger.getLogger(StatStreamUtil.class);
	
	public static boolean launchRserve(String cmd) { 
		return launchRserve(cmd, "--no-save --slave","--no-save --slave",false); 
	}
	
	/* checks whether Rserve is running and if that's not the case it attempts to start it using the defaults for the platform where it is run on. 
	 * This method is meant to be set-and-forget and cover most default setups. 
	 * For special setups you may get more control over R with <<code>launchRserve</code> instead. 
	 */
	public static boolean checkLocalRserve() {
		if (isRserveRunning()) return true;
		String osname = System.getProperty("os.name");
		if (osname != null && osname.length() >= 7 && osname.substring(0,7).equals("Windows")) {
			log.info("Windows: query registry to find where R is installed ...");
			String installPath = null;
			try {
				Process rp = Runtime.getRuntime().exec("reg query HKLM\\Software\\R-core\\R");
				StreamHog regHog = new StreamHog(rp.getInputStream(), true);
				rp.waitFor();
				regHog.join();
				installPath = regHog.getInstallPath();
			} catch (Exception rge) {
				log.info("ERROR: unable to run REG to find the location of R: "+rge);
				return false;
			}
			if (installPath == null) {
				log.info("ERROR: canot find path to R. Make sure reg is available and R was installed with registry settings.");
				return false;
			}
			return launchRserve(installPath+"\\bin\\R.exe");
		}
		return (launchRserve("R") || /* try some common unix locations of R */
			((new File("/Library/Frameworks/R.framework/Resources/bin/R")).exists() && launchRserve("/Library/Frameworks/R.framework/Resources/bin/R")) ||
			((new File("/usr/local/lib/R/bin/R")).exists() && launchRserve("/usr/local/lib/R/bin/R")) ||
			((new File("/usr/lib/R/bin/R")).exists() && launchRserve("/usr/lib/R/bin/R")) ||
			((new File("/usr/local/bin/R")).exists() && launchRserve("/usr/local/bin/R")) ||
			((new File("/sw/bin/R")).exists() && launchRserve("/sw/bin/R")) ||
			((new File("/usr/common/bin/R")).exists() && launchRserve("/usr/common/bin/R")) ||
			((new File("/opt/bin/R")).exists() && launchRserve("/opt/bin/R"))
			);
	}
	
	/** attempt to start Rserve. Note: parameters are <b>not</b> quoted, so avoid using any quotes in arguments
	 @param cmd command necessary to start R
	 @param rargs arguments are are to be passed to R
	 @param rsrvargs arguments to be passed to Rserve
	 @return <code>true</code> if Rserve is running or was successfully started, <code>false</code> otherwise.
	 */
	public static boolean launchRserve(String cmd, String rargs, String rsrvargs, boolean debug) {
		try {
			debug = true;
			
			Process p;
			boolean isWindows = false;
			String osname = System.getProperty("os.name");
			if (osname != null && osname.length() >= 7 && osname.substring(0,7).equals("Windows")) {
				isWindows = true; /* Windows startup */
				p = Runtime.getRuntime().exec("\""+cmd+"\" -e \"library(Rserve);Rserve("+(debug?"TRUE":"FALSE")+",args='"+rsrvargs+"')\" "+rargs);
			} else { /* unix startup */
				p = Runtime.getRuntime().exec(new String[] {
							      "/bin/sh", "-c",
							      "echo 'library(Rserve);Rserve("+(debug?"TRUE":"FALSE")+",args=\""+rsrvargs+"\")'|"+cmd+" "+rargs
							      });
			}
			log.info("waiting for Rserve to start ... ("+p+")");
			// we need to fetch the output - some platforms will die if you don't ...
			StreamHog errorHog = new StreamHog(p.getErrorStream(), false);
			StreamHog outputHog = new StreamHog(p.getInputStream(), false);
			if (!isWindows) /* on Windows the process will never return, so we cannot wait */
			//	p.waitFor();
			log.info("call terminated, let us try to connect ...");
		} catch (Exception x) {
			log.info("failed to start Rserve process with "+x.getMessage());
			return false;
		}
		int attempts = 5; /* try up to 5 times before giving up. We can be conservative here, because at this point the process execution itself was successful and the start up is usually asynchronous */
		while (attempts > 0) {
			try {
				RConnection c = new RConnection();
				log.info("Rserve is running.");
				c.close();
				return true;
			} catch (Exception e2) {
				log.info("Try failed with: "+e2.getMessage());
			}
			/* a safety sleep just in case the start up is delayed or asynchronous */
			try { Thread.sleep(500); } catch (InterruptedException ix) { };
			attempts--;
		}
		return false;
	}
	
	/** check whether Rserve is currently running (on local machine and default port).
	 @return <code>true</code> if local Rserve instance is running, <code>false</code> otherwise
	 */
	public static boolean isRserveRunning() {
		try {
			RConnection c = new RConnection();
			log.info("Rserve is running.");
			c.close();
			return true;
		} catch (Exception e) {
			log.info("First connect try failed with: "+e.getMessage());
		}
		return false;
	}
}
