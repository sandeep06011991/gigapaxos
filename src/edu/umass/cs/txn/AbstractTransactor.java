package edu.umass.cs.txn;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParserBytes;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.PaxosReplicaCoordinator;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReconfiguratorCallback;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.txn.interfaces.TXInterface;
import edu.umass.cs.txn.txpackets.LockRequest;
import edu.umass.cs.txn.txpackets.TXInitRequest;
import edu.umass.cs.txn.txpackets.TXPacket;
import edu.umass.cs.txn.txpackets.TxOpRequest;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.GCConcurrentHashMap;

/**
 * @author arun
 * Deleted:
 * A lot of code was doing the same as is done in super class
 * this.coordinator was doing what this.app does in parent class
 * Not required as app=coordinator
 * @param <NodeIDType>
 */


public  abstract class AbstractTransactor<NodeIDType> extends
		AbstractReplicaCoordinator<NodeIDType> implements ReconfiguratorCallback {

	private final AbstractReplicaCoordinator<NodeIDType> coordinator;

	protected AbstractTransactor(
			AbstractReplicaCoordinator<NodeIDType> coordinator) {
		super(coordinator);
		System.out.println("Class of Coordinator"+coordinator.getClass());
		assert (coordinator instanceof PaxosReplicaCoordinator);
		this.coordinator = coordinator;
		this.coordinator.setCallback(this);
		AppRequestParser appRequestParser=new AppRequestParser() {
			@Override
			public Request getRequest(String stringified) throws RequestParseException {
				return AbstractTransactor.this.getRequestNew(stringified);
			}
			@Override
			public Request getRequest(byte[] message, NIOHeader header) throws RequestParseException {
				return AbstractTransactor.this.getRequestNew(message,header);

			}
			@Override
			public Set<IntegerPacketType> getRequestTypes() {
				return AbstractTransactor.this.getAppRequestTypes();
			}
		};

		this.coordinator.setGetRequestImpl(appRequestParser);
		this.coordinator.setGetRequestImpl((AppRequestParserBytes) appRequestParser);
		this.setGetRequestImpl(appRequestParser);

	}


	private static final boolean ENABLE_TRANSACTIONS = Config
			.getGlobalBoolean(RC.ENABLE_TRANSACTIONS);

	/* FIXME: how is a transaction's timeout decided? Any limit imposes a limit
	 * on the type of operations that can be done within a transaction.
	 * Databases don't impose such a timeout as they allow transactional
	 * operations to take arbitrarily long. So should we. */
	private static final long DEFAULT_TX_TIMEOUT = Long.MAX_VALUE;
	private static final long MAX_QUEUED_REQUESTS = 8000;

//	/* FIXME: Why would we need call backs ?? */
	/* Isnt rigging the  pre-executed function not enough */
	private GCConcurrentHashMap<Request, ExecutedCallback> callbacks = new GCConcurrentHashMap<Request, ExecutedCallback>(
			DEFAULT_TX_TIMEOUT);
	/* ********* Start of coordinator-related methods *************** */
	private Set<IntegerPacketType> cachedRequestTypes = null;

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		if (cachedRequestTypes != null)
			return cachedRequestTypes;
		Set<IntegerPacketType> types = new HashSet<IntegerPacketType>();
		// underlying coordinator request (includes app types)
		types.addAll(this.coordinator.getRequestTypes());
		// tx types
		if (ENABLE_TRANSACTIONS)
			types.addAll(TXPacket.PacketType.intToType.values());
//			types.addAll(new HashSet<IntegerPacketType>(Arrays.asList(txTypes)));
		return cachedRequestTypes = types;
	}

	// simply enqueue if room and call parent
	@Override
	public boolean coordinateRequest(Request request, ExecutedCallback callback)
			throws IOException, RequestParseException {
		if (!ENABLE_TRANSACTIONS || this.callbacks.size() < MAX_QUEUED_REQUESTS
				&& this.callbacks.putIfAbsent(request, callback) == null)
			return this.coordinator.coordinateRequest(request, callback);
		// else
		ReconfigurationConfig.getLogger().log(Level.WARNING,
				"{0} dropping request {1} because queue size limit reached",
				new Object[] { this, request.getSummary() });
		return false;
	}


//	Rigging methods to underlying coordinator
//	AbstractReplicaCoordinator takes in coordinator but stores the app
//	directly calling the parent class for the methods below imply
//	that the paxos cordinator is by passed
	@Override
	/*Intercept execute requests here*/

	public boolean execute(Request request, boolean noReplyToClient){
		return this.coordinator.execute(request,noReplyToClient);
	}


	public AbstractReplicaCoordinator<NodeIDType> getCoordinator() {
		return coordinator;
	}

	public  boolean createReplicaGroup(String serviceName, int epoch,
											   String state, Set<NodeIDType> nodes){
		return this.coordinator.createReplicaGroup(serviceName,epoch,state,nodes);
	}


	@Override
	public boolean deleteReplicaGroup(String serviceName, int epoch) {
		return this.coordinator.deleteReplicaGroup(serviceName,epoch);
	}

	@Override
	public boolean execute(Request request) {
		return this.coordinator.execute(request);
	}

	public ReconfigurableRequest getStopRequest(String name, int epoch){
		return  this.coordinator.getStopRequest(name,epoch);
	}

	public String getFinalState(String name, int epoch){
		return this.coordinator.getFinalState(name,epoch);
	}

	public void putInitialState(String name, int epoch, String state){
		this.coordinator.putInitialState(name,epoch,state);
	}


	@Override
	public Integer getEpoch(String name) {
		return this.coordinator.getEpoch(name);
	}


	@Override
	public Set<NodeIDType> getReplicaGroup(String serviceName) {
		return this.coordinator.getReplicaGroup(serviceName);
	}

	public abstract Request getRequestNew(String str) throws RequestParseException;

	public abstract Request getRequestNew(byte[] bytes, NIOHeader header) throws RequestParseException;

	@Override
	public void executed(Request request, boolean handled) {
//		Uselessly call
		System.out.println("Nothing much done");
	}
}
