package net.floodlightcontroller.custommodule;

import java.util.Collection;
import java.util.Map;
import java.util.ArrayList;

import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFlowAdd;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.action.OFActions;
import org.projectfloodlight.openflow.protocol.action.OFActionOutput;
import org.projectfloodlight.openflow.protocol.instruction.OFInstruction;
import org.projectfloodlight.openflow.protocol.instruction.OFInstructions;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.protocol.instruction.OFInstructionWriteActions;
import org.projectfloodlight.openflow.protocol.OFPacketInReason;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.TableId;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.IFloodlightProviderService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomModule implements IOFMessageListener, IFloodlightModule {
	protected IFloodlightProviderService floodlightProvider;
	protected static Logger logger;
	
	@Override
	public String getName() {
		return CustomModule.class.getSimpleName();
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context) throws FloodlightModuleException {
		floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
		logger = LoggerFactory.getLogger(CustomModule.class);
	}

	@Override
	public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
		floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
	}

	@Override
	public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		OFFactory my13Factory = OFFactories.getFactory(OFVersion.OF_13);
		OFInstructions instructions = my13Factory.instructions();
		OFActions actions = my13Factory.actions();
		
		switch(msg.getType()) {
		case PACKET_IN:
			ArrayList<OFInstruction> instructionList = new ArrayList<OFInstruction>();
			ArrayList<OFAction> actionList = new ArrayList<OFAction>();
			
			OFActionOutput output = actions.buildOutput().setPort(OFPort.of(2)).build();
			actionList.add(output);
			OFInstructionWriteActions instruct = instructions.buildWriteActions().setActions(actionList).build();
			instructionList.add(instruct);
			
			Match myMatch = my13Factory.buildMatch()
					//.setExact(MatchField.IN_PORT, OFPort.of(1))
					.setExact(MatchField.ETH_TYPE, EthType.IPv4)
					//.setExact(MatchField.IN_PHY_PORT,)
					.build();
			
			OFPacketIn packetIn = (OFPacketIn)msg;
			if(packetIn.getReason() == OFPacketInReason.NO_MATCH) {
				OFFlowAdd flowAdd = my13Factory.buildFlowAdd()
						.setBufferId(OFBufferId.NO_BUFFER)
						.setHardTimeout(3600)
						.setIdleTimeout(10)
						.setPriority(32768)
						.setMatch(myMatch)
						.setInstructions(instructionList)
						.setTableId(TableId.of(1))
						.build();
				sw.write(flowAdd);
			}
			break;
			
		default:
			break;	
		}
		
		return Command.CONTINUE;
		//return Command.STOP;
	}
}
