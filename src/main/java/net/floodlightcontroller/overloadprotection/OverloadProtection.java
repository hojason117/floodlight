package net.floodlightcontroller.overloadprotection;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.Random;

import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.types.DatapathId;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.threadpool.IThreadPoolService;
import net.floodlightcontroller.core.IFloodlightProviderService;

import net.floodlightcontroller.util.LoadMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverloadProtection implements IOFMessageListener, IFloodlightModule {
	protected IFloodlightProviderService floodlightProvider;
	protected static Logger logger;
	protected static CpuLoadMonitor cpuloadmonitor = null;
	protected static MemoryLoadMonitor memloadmonitor = null;
	protected static DropRateMonitor dropRateMonitor = null;
	protected static boolean protection = false;
	protected static long ingressPktCount = 0;
	protected static long totalDropCount = 0;
	private static HashMap<DatapathId, Long> switchPktCounter = null;
	private static Random rand;
	
	@Override
	public String getName() {
		return OverloadProtection.class.getSimpleName();
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		return false;
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		return true;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		l.add(IThreadPoolService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context) throws FloodlightModuleException {
		floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
		logger = LoggerFactory.getLogger(OverloadProtection.class);
		cpuloadmonitor = new CpuLoadMonitor();
		cpuloadmonitor.startMonitoring(context.getServiceImpl(IThreadPoolService.class).getScheduledExecutor());
		memloadmonitor = new MemoryLoadMonitor();
		memloadmonitor.startMonitoring(context.getServiceImpl(IThreadPoolService.class).getScheduledExecutor());
		dropRateMonitor = new DropRateMonitor(logger);
		dropRateMonitor.startMonitoring(context.getServiceImpl(IThreadPoolService.class).getScheduledExecutor());
		switchPktCounter = new HashMap<DatapathId, Long>();
		rand = new Random();
	}

	@Override
	public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
		floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
	}

	@Override
	public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		ingressPktCount++;
		
		if(protection && msg.getType() == OFType.PACKET_IN) {
			totalDropCount++;
			
			long count = 1;
			if(switchPktCounter.get(sw.getId()) == null)
				switchPktCounter.put(sw.getId(), (long)1);
			else {
				count = switchPktCounter.get(sw.getId()) + 1;
				switchPktCounter.put(sw.getId(), count);
			}
			
			if(rand.nextFloat() < (float)count / (float)ingressPktCount)
				return Command.STOP;
			else
				return Command.CONTINUE;
		}
		
		return Command.STOP;
	}
	
	private static class DropRateMonitor implements Runnable {
		protected Logger log;
		private static final int SAMPLING_INTERVAL = 200; // mili-sec
		private static long lastPktCount;
		private static long lastDropCount;
		
		public DropRateMonitor(Logger log) {
			this.log = log;
		}
		
		@Override
		public void run() {
			long pktCount = ingressPktCount - lastPktCount;
			long dropCount = totalDropCount - lastDropCount;
			
			double dropRate = (double)dropCount / (double)pktCount;
			
			//System.out.format("total: %d   dropped: %d\n", ingressPktCount, totalDropCount);
			//System.out.format("Drop rate:   %3d%%\n", (int)(dropRate * 100));
			
			if(dropRate > 0.01) {
				String msg = String.format("System under heavy load, dropping %3d%% packet-ins.", (int)(dropRate * 100));
				
				if (log != null) {
	                log.error(msg);
				}
	            else {
	                System.out.println(msg);
	            }
			}
			
			lastPktCount = ingressPktCount;
			lastDropCount = totalDropCount;
		}
		
		public ScheduledFuture<?> startMonitoring(ScheduledExecutorService ses) {
	        ScheduledFuture<?> monitorTask =	 ses.scheduleAtFixedRate(this, 0, SAMPLING_INTERVAL, TimeUnit.MILLISECONDS);
	        return monitorTask;
	    }
	}
	
	private class CpuLoadMonitor extends LoadMonitor {
		public static final int SAMPLING_INTERVAL = 200; // mili-sec
		protected volatile double cpuLoad;
		
		public CpuLoadMonitor() {
			super(logger);
		}
		
		@Override
	    public void run() {
			if (isLinux) {
		        long currNanos = System.nanoTime();
		        long currIdle = this.readIdle();
		        for (int i=0 ; i < (MAX_LOAD_HISTORY - 1) ; i++) {
		            lastNanos[i] = lastNanos[i+1];
		            lastIdle[i] = lastIdle[i+1];
		        }
		        lastNanos[MAX_LOAD_HISTORY - 1] = currNanos;
		        lastIdle[MAX_LOAD_HISTORY - 1] = currIdle;
	
		        if (itersLoaded >= MAX_LOADED_ITERATIONS) {
		            loadlevel = LoadLevel.OK;
		            itersLoaded = 0;
		            return;
		        }
	
		        long nanos = lastNanos[MAX_LOAD_HISTORY - 1] - lastNanos[0];
		        long idle = lastIdle[MAX_LOAD_HISTORY - 1] - lastIdle[0];
		        
		        cpuLoad = 1.0 - ((double)(idle * jiffyNanos) / (double)(nanos * numcores));
		        
		        protection = (cpuLoad > 0.7) ? true : false;
		        //System.out.format("CPU    load: %3d%%\n", (int)(cpuLoad * 100));
	
		        if (cpuLoad > THRESHOLD_VERYHIGH) {
		            loadlevel = LoadLevel.VERYHIGH;
		            itersLoaded += 1;
		            String msg = "System under very heavy load, dropping packet-ins.";
	
		            if (log != null) {
		                log.error(msg);
		            }
		            else {
		                System.out.println(msg);
		            }
		            return;
		        }
	
		        if (cpuLoad > THRESHOLD_HIGH) {
		            loadlevel = LoadLevel.HIGH;
		            itersLoaded += 1;
		            String msg = "System under heavy load, dropping new flows.";
	
		            if (log != null) {
		                log.error(msg);
		            }
		            else {
		                System.out.println(msg);
		            }
		            return;
		        }
		        
		        loadlevel = LoadLevel.OK;
		        itersLoaded = 0;
	        }
			
	        return;
	    }
		
		@Override
		public ScheduledFuture<?> startMonitoring(ScheduledExecutorService ses) {
	        ScheduledFuture<?> monitorTask = ses.scheduleAtFixedRate(this, 0, SAMPLING_INTERVAL, TimeUnit.MILLISECONDS);
	        return monitorTask;
	    }
	}
	
	private class MemoryLoadMonitor extends LoadMonitor {
		public static final int SAMPLING_INTERVAL = 200; // mili-sec
		protected volatile double memLoad;
		
		public MemoryLoadMonitor() {
			super(logger);
		}
		
		@Override
	    public void run() {
			memLoad = (double)(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (double)Runtime.getRuntime().maxMemory();
			
			protection = (memLoad > 0.6) ? true : false;
	        //System.out.format("Memory load: %3d%%\n", (int)(memLoad * 100));
	        
			if (memLoad > THRESHOLD_VERYHIGH) {
	            loadlevel = LoadLevel.VERYHIGH;
	            String msg = "System under very heavy load, dropping packet-ins.";

	            if (log != null) {
	                log.error(msg);
	            }
	            else {
	                System.out.println(msg);
	            }
	            return;
	        }

	        if (memLoad > THRESHOLD_HIGH) {
	            loadlevel = LoadLevel.HIGH;
	            String msg = "System under heavy load, dropping new flows.";

	            if (log != null) {
	                log.error(msg);
	            }
	            else {
	                System.out.println(msg);
	            }
	            return;
	        }
	        
	        loadlevel = LoadLevel.OK;
	        
	        return;
	    }
		
		@Override
		public ScheduledFuture<?> startMonitoring(ScheduledExecutorService ses) {
	        ScheduledFuture<?> monitorTask = ses.scheduleAtFixedRate(this, 0, SAMPLING_INTERVAL, TimeUnit.MILLISECONDS);
	        return monitorTask;
	    }
	}
}
