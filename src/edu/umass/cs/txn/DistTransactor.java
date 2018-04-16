package edu.umass.cs.txn;

import java.io.IOException;
import java.net.InetSocketAddress;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.gigapaxos.interfaces.*;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.PaxosReplicaCoordinator;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import edu.umass.cs.txn.exceptions.TXException;
import edu.umass.cs.txn.exceptions.TxnState;
import edu.umass.cs.txn.interfaces.TXLocker;
import edu.umass.cs.txn.protocol.*;
import edu.umass.cs.txn.txpackets.*;
import edu.umass.cs.utils.Config;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author arun
 * 
 *         This class is used at a node pushing forward the transactions steps,
 *         which is normally the primary designate in the transaction group. If
 *         the primary crashes, a secondary might use this class for the same
 *         purpose.
 * @param <NodeIDType>
 */
public class DistTransactor<NodeIDType> extends AbstractTransactor<NodeIDType>
		implements Replicable {

	/**
	 * A distributed transaction processor needs a client to submit transaction
	 * operations as well as to acquire and release locks.
	 */
	final public  ReconfigurableAppClientAsync<Request> gpClient;
	
	final ProtocolExecutor<NodeIDType,TXPacket.PacketType,String> protocolExecutor;

	private final TXLocker txLocker;

	private TxMessenger txMessenger;


	HashMap<String,LeaderState> leaderStateHashMap = new HashMap<>();
	/**
	 * @param coordinator
	 * @throws IOException
	 */
	public DistTransactor(AbstractReplicaCoordinator<NodeIDType> coordinator)
			throws IOException {
		super(coordinator);
		this.gpClient = TXUtils.getGPClient(this);
		this.txLocker = new TXLockerMap(coordinator);
		txMessenger=new TxMessenger(this.gpClient,this);
		protocolExecutor=new ProtocolExecutor<>(txMessenger);
		txMessenger.setProtocolExecutor(protocolExecutor);

	}




	/**
	 * A blocking call that returns upon successfully locking {@code lockID} or
	 * throws a {@link TXException}. Locking a group involves synchronously 
	 * checkpointing its state and maintaining in memory its locked status.
	 * 
	 * @param lockID
	 * @throws TXException
	 */
//	public void lock(String lockID) throws TXException {
//		this.txLocker.lock(lockID);
//	}

	/**
	 * Acquires the locks in the order specified by {@code lockIDs}.
	 * 
	 * @param lockIDs
	 * @throws TXException
	 */
	public void lock(String[] lockIDs) throws TXException {
		throw new RuntimeException("Unimplemented");
	}

	/**
	 * A blocking call that returns upon successfully release {@code lockID} or
	 * throws a {@link TXException} .
	 * 
	 * @param lockID
	 * @throws TXException
	 */
	public void unlock(String lockID) throws TXException {

		throw new RuntimeException("Unimplemented");
	}

	/**
	 * Releases the locks in the order specified by {@code lockIDs}.
	 * 
	 * @param lockIDs
	 * @throws TXException
	 */
	public void unlock(String[] lockIDs) throws TXException {
		throw new RuntimeException("Unimplemented");
	}

	/**
	 * This method is the top-level method initiating a transaction and consists
	 * of the following sequence of steps: (1) create transaction group; (2)
	 * acquire participant group locks; (3) execute transaction operations in
	 * order; (4) issue commit to transaction group; (5) release participant
	 * group locks; (6) delete transaction group. Each step in this sequence is
	 * blocking and all steps must succeed for this method to complete
	 * successfully, otherwise it will throw a {@link TXException}. The
	 * transaction group acts as the "monitor" group that makes it easy to
	 * reason about the safety property that all participant groups agree on
	 * whether a transaction is committed or aborted and that either decision is
	 * final.
	 * 
	 * Message complexity: Below, P1 refers to the message complexity of a paxos
	 * operation in the transaction group, P2 to that of a paxos operation in
	 * the largest participant group, and P3 to that in a reconfigurator group;
	 * N1 is the number of transaction steps involving participant groups, N2 is
	 * the number of name create operations, and N3 the number of name delete
	 * operations; M is the distinct number of participant groups (or names)
	 * involved;
	 * 
	 * (1) 2*P3 + 2/3*P1
	 * 
	 * (2) N1*P2
	 * 
	 * (3) (N1+N3)*P2 + 2*(N2+N3)*P3 + 2/3*N2*P2
	 * 
	 * (4) P1
	 * 
	 * (5) (N1 + N2 + N3)*P2
	 * 
	 * (6) P1 + 2*P3
	 * 
	 * In comparison, simply executing the transaction's steps sequentially
	 * without ensuring any transactional semantics has a message complexity of
	 * (N1+N3)*P2 + 2*(N2+N3)*P3 + 2/3*N2*P2, i.e., just step 3 above. Thus,
	 * transactions roughly increase the message complexity by a factor of 3
	 * (for (lock, execute, unlock) compared to just execute for each operation)
	 * plus a fixed number, a total of ~7, of additional paxos operations across
	 * the transaction or various reconfigurator groups involved.
	 * 
	 * Optimizations: The overhead of the transaction group can be reduced to
	 * just a single paxos operation with a corresponding liveness cost if we
	 * reuse transaction groups across different transactions. (1) Choosing the
	 * transaction group as a fixed set of active replicas makes transactional
	 * liveness limited by majority availability in that set; (2) Choosing the
	 * transaction group as the set of all active replicas can incur a
	 * prohibitively high overhead for even a single paxos operation as the
	 * total number of active replicas may be much higher than the size of
	 * typical participant groups.
	 * 
	 * 
	 * @param tx
	 * @throws TXException
	 */




	private boolean fixedTXGroupCreated = true;
	private static final boolean FIXED_TX_GROUP = true;

	/**
	 * This is the first step in a transaction. There is no point proceeding
	 * with a transaction if the transaction group does not exist. A transaction
	 * can be requested by an end-client but has to be initiated by an active
	 * replica, i.e., this createTxGroup has to be made from an active replica.
	 * There are two reasons for this: (1) there is a safety issue with allowing
	 * end-clients to lock consensus groups without any checks; (2) an active
	 * replica can be part of the transaction group saving the need to issue and
	 * maintain triggers in order to respond back to the end-client.
	 * 
	 * @return True if the group got created or if the group already exists;
	 *         false otherwise.
	 * @throws IOException
	 */
	private boolean createTxGroup(Transaction tx) throws IOException,ReconfigurableAppClientAsync.ReconfigurationException {
		if (FIXED_TX_GROUP && this.fixedTXGroupCreated)
			return true;
		throw new RuntimeException("Unimplemented");
	}

	private static final int MAX_TX_GROUP_SIZE = 11;

	/* The default policy is to use a deterministic set of active replicas for
	 * each transaction of size MAX_TX_GROUP_SIZE of the total number of active
	 * replica, whichever is lower. */
	protected Set<InetSocketAddress> getTxGroup(String txid) throws IOException,ReconfigurableAppClientAsync.ReconfigurationException {
		InetSocketAddress[] addresses = this.getAllActiveReplicas().toArray(
				new InetSocketAddress[0]);
		Set<InetSocketAddress> group = new HashSet<InetSocketAddress>();
		/* Picking start index randomly introduces some load balancing in the
		 * transaction group when the total number of active replicas is much
		 * higher than MAX_TX_GROUP_SIZE */
		int startIndex = txid.hashCode() % addresses.length;
		for (int i = startIndex; group.size() <   MAX_TX_GROUP_SIZE; i = (i + 1)
				% addresses.length)
			group.add(addresses[i]);
		return group;
	}

	/**
	 * There isn't an easy way to get the correct list of all active replicas at
	 * an active replica without consulting reconfigurators. Reading it from the
	 * config file will in generaublic class DistTransactor<NodeIDType> extends AbstractTransactor<NodeIDType> implements TXLocker {

	/**l be incorrect if active replicas are added or
	 * deleted over time.
	 * 
	 * @return The set of active replica socket addresses.
	 * @throws IOException
	 */
	private Set<InetSocketAddress> getAllActiveReplicas() throws IOException,ReconfigurableAppClientAsync.ReconfigurationException {
		return ((RequestActiveReplicas) this.gpClient
				.sendRequest(new RequestActiveReplicas(Config
						.getGlobalString(RC.BROADCAST_NAME)))).getActives();
	}

	public Request getRequestNew(String str) throws RequestParseException{
		try{
			JSONObject jsonObject=new  JSONObject(str);
			TXPacket.PacketType packetId=TXPacket.PacketType.intToType.get(jsonObject.getInt("type"));
			if(packetId !=null){
			switch(packetId) {
				case TX_INIT:
					return new TXInitRequest(jsonObject,this.getCoordinator());
				case LOCK_REQUEST:
					return new LockRequest(jsonObject);
				case TX_OP_REQUEST:
					return new TxOpRequest(jsonObject,this.getCoordinator());
				case UNLOCK_REQUEST:
					return new UnlockRequest(jsonObject);
				case RESULT:
					return new TXResult(jsonObject);
				case TX_TAKEOVER:
					return new TXTakeover(jsonObject);
				case TX_STATE_REQUEST:
					return new TxStateRequest(jsonObject);
				case TX_CLIENT:
					return new TxClientRequest(jsonObject,this.getCoordinator());
				case TX_CLIENT_RESPONSE:
					return new TxClientResult(jsonObject);

				default:
						throw new RuntimeException("Forgot handling some TX packet");
				}
			}
		}catch(JSONException e){
			throw new RequestParseException(e);
		}
//		FixMe: this must be handled outside, dont need to pass it iin
		return this.app.getRequest(str);
	}

	public  Request getRequestNew(byte[] bytes, NIOHeader header)
			throws RequestParseException{
//		FIXME: These methods are highly specific
		try{
			String str=new String(bytes, NIOHeader.CHARSET);
			Request request=getRequestNew(str);
			if(request instanceof TxClientRequest){
				TxClientRequest t=(TxClientRequest) request;
				t.clientAddr=header.sndr;
				t.recvrAddr= header.rcvr;
			}
			return request;
		}catch(Exception e){
			e.printStackTrace();
		}
		return this.app.getRequest(bytes,header);
	}

	public Set<IntegerPacketType> getAppRequestTypes(){
		return this.getRequestTypes();
	}




	@Override
	public boolean preExecuted(Request request) {
		if(request==null){return false;}

		if(request instanceof TxClientResult) {
			TxClientResult txClientResult = (TxClientResult) request;
			try {
				System.out.println("Sending to client from entry server"+txClientResult.toString());
				((JSONMessenger)this.getMessenger()).sendClient(txClientResult.getClientAddr(),txClientResult,txClientResult.getServerAddr());
			} catch (JSONException|IOException e) {
				System.out.println("Unable to send to client");
			}
			return true;
		}

		if(request instanceof TxClientRequest) {
			String leader = "Service_name_txn";
			TxClientRequest txClientRequest=(TxClientRequest)request;
			Transaction transaction = new Transaction(txClientRequest.recvrAddr,
					((TxClientRequest) request).getRequests(), (String) getMyID(),
					txClientRequest.clientAddr,txClientRequest.getRequestID(),leader);
			try {
					this.gpClient.sendRequest(new TXInitRequest(transaction));

			}catch (IOException ex){
					throw new RuntimeException("Unable to send Transaction to Fixed Groups");
				}
			return true;
		}

//		FIXME: Minor Redundancy why is nodeID being stored both at the transaction
//		FIXME: and inside the secondary transaction protocol
		if(request instanceof TXInitRequest){
			TXInitRequest trx=(TXInitRequest)request;
				if(trx.transaction.nodeId.equals(this.getMyID())){
					System.out.println("Initiating Primary Transaction,leader:"+getMyID());
					this.protocolExecutor.spawnIfNotRunning(new TxLockProtocolTask<NodeIDType>(trx.transaction,protocolExecutor));
				}else{
					this.protocolExecutor.spawnIfNotRunning(
							new TxSecondaryProtocolTask<>
									(trx.transaction,TxState.INIT,protocolExecutor));
				}
			String leader_name="Service_name_txn";
			LeaderState leaderState;
			if((leaderState = leaderStateHashMap.get(leader_name))==null){
				leaderState = new LeaderState(leader_name);
			}
			leaderState.insertNewTransaction(new OngoingTxn(trx.transaction,TxState.INIT));
			leaderStateHashMap.put(leader_name,leaderState);
			return true;
		}
		if(request instanceof LockRequest){
			LockRequest lockRequest=(LockRequest)request;
			boolean success=txLocker.lock(lockRequest.getServiceName(),lockRequest.txid);
			TXResult result=
					new TXResult(lockRequest.txid,lockRequest.getTXPacketType(),
							success,(String) lockRequest.getKey(),lockRequest.getServiceName(),lockRequest.getLeader());
			result.setRequestId(lockRequest.getRequestID());
			lockRequest.response=result;
			return true;
		}

		if(request instanceof UnlockRequest){
			UnlockRequest unlockRequest=(UnlockRequest)request ;
			boolean success=false;
			if(txLocker.isLockedByTxn(unlockRequest.getServiceName(),unlockRequest.getLockID())){
				if(!unlockRequest.isCommited()){
					restore(unlockRequest.getServiceName(),txLocker.getStateMap(unlockRequest.getServiceName()).state);
				}
				success=txLocker.unlock(unlockRequest.getServiceName(),unlockRequest.txid);
			}
			TXResult txResult= new TXResult(unlockRequest.txid,unlockRequest.getTXPacketType(),
					success,(String) unlockRequest.getKey(),unlockRequest.getServiceName(),unlockRequest.getLeader());;
			txResult.setRequestId(unlockRequest.getRequestID());
			unlockRequest.response=txResult;
/*If 2 unlock requests where sent by the same co-ordinator and response is reordered
* it would wait for one of the 2 responses */
			return true;
		}

		if(request instanceof TxOpRequest){
//			FIXME: Op ID on TXOP
			TxOpRequest txOpRequest=(TxOpRequest) request;
			boolean success=txLocker.isLocked(txOpRequest.getServiceName());
			if(success){
				boolean handled=txLocker.allowRequest(txOpRequest.request.getRequestID(),txOpRequest.txid,txOpRequest.getServiceName());
				if(!handled) this.execute(txOpRequest.request,true,null);
			}
			TXResult result=new TXResult(txOpRequest.txid,txOpRequest.getTXPacketType(),success
							,(String) txOpRequest.getKey(),Long.toString(txOpRequest.opId),txOpRequest.getLeader());
			result.setRequestId(txOpRequest.getRequestID());
			txOpRequest.response=result;
			return true;
			}

		if(request instanceof TxStateRequest){
			TransactionProtocolTask protocolTask=(TransactionProtocolTask) protocolExecutor.getTask(((TxStateRequest) request).getTXID());
			if(protocolTask==null){
//				This transaction must have already been completed by a previous one
				return true;
			}
			ProtocolTask newProtocolTask=protocolTask.onStateChange((TxStateRequest) request);
			protocolExecutor.remove((String)protocolTask.getKey());
			if(newProtocolTask!=null)protocolExecutor.spawn(newProtocolTask);
			leaderStateHashMap.get("Service_name_txn").
					updateTransaction(((TxStateRequest) request).getTXID(),((TxStateRequest) request).getState());
			return true;
		}

		if(request instanceof TXTakeover){
			TXTakeover txRequest=(TXTakeover)request;
			TransactionProtocolTask protocolTask=(TransactionProtocolTask) protocolExecutor.getTask(txRequest.txid);
			boolean isPrimary=txRequest.getNewLeader().equals((String)getMyID());
			assert protocolTask != null;
			ProtocolTask newProtocolTask = protocolTask.onTakeOver(txRequest,isPrimary);
			if(isPrimary){System.out.println("My takeover successfull I,"+getMyID()+ " am leader ");}
			if(newProtocolTask!=null){
				protocolTask.cancel();
				protocolExecutor.spawn(newProtocolTask);
				}
			return true;
		}
		if((request instanceof ClientRequest)){
			if(txLocker.isAllowedRequest((ClientRequest) request)){
				return false;
				}
//			FixMe: Can do some Exception handling here
			System.out.println("DROPPING REQUEST. SYSTEM BUSY");
			return true;
		}

		return false;
	}

	@Override
	public void initRecovery() {
		this.getCoordinator().initRecovery();
		this.txMessenger.recoveringComplete();
	}


	public String preCheckpoint(String name) {
		if(!txLocker.isLocked(name)&&!leaderStateHashMap.containsKey(name)){return null;}
		JSONObject jsonObject= new JSONObject();
		try {
			if (txLocker.isLocked(name)) {
				jsonObject.put("txLocker", txLocker.getStateMap(name).toJSONObject());
			}
			if(leaderStateHashMap.containsKey(name)){
				jsonObject.put("leader",leaderStateHashMap.get(name).toJSONObject(name));
			}

		return jsonObject.toString();
		}catch(JSONException ex){
			throw new RuntimeException("Conversion to JSON is flawed");

		}
	}

	public boolean preRestore(String name, String state) {
		try {
			JSONObject jsonObject = new JSONObject(state);
		}catch (JSONException ex){
			return false;
		}

		try {
//			System.out.println("Attempting to restore"+name+"	: "+state);
			JSONObject jsonObject = new JSONObject(state);
			if(jsonObject.has("txLocker")){
				TxnState txnState=new TxnState(jsonObject.getJSONObject("txLocker"));
				this.getCoordinator().restore(name,txnState.state);
				txLocker.updateStateMap(name,txnState);
				for(String req_string:txnState.requests){
					Request request=this.getCoordinator().getRequest(req_string);
					this.getCoordinator().execute(request,true);
				}
			}
			if(jsonObject.has("leader")){
//				FixMe: Repeat some code for a quick fix
				LeaderState leaderState = new LeaderState(jsonObject.getJSONObject("leader"),this.getCoordinator());
				for(OngoingTxn ongoingTxn:leaderState.ongoingTxnHashMap.values()){
					switch(ongoingTxn.txState){
						case INIT:	Transaction transaction = ongoingTxn.transaction;
									if(transaction.nodeId.equals(getMyID())){
									System.out.println("Initiating Primary Transaction	"+getMyID());
										this.protocolExecutor.spawnIfNotRunning(new TxLockProtocolTask<NodeIDType>(transaction,protocolExecutor));
									}else{
										System.out.println("Initiating Secondary Transaction");
										this.protocolExecutor.spawnIfNotRunning(
												new TxSecondaryProtocolTask<>
														(transaction,TxState.INIT,protocolExecutor));
									}
									break;
						case COMMITTED:	protocolExecutor.spawn(new TxCommitProtocolTask<>(ongoingTxn.transaction,protocolExecutor));
										break;
						case ABORTED:	protocolExecutor.spawn(new TxAbortProtocolTask<>(ongoingTxn.transaction,protocolExecutor));
										break;
						case COMPLETE: throw new RuntimeException("If it was complete why would it be recorded");
					}
				}
				leaderStateHashMap.put(name,leaderState);

			}
			return true;
		}catch(JSONException j){
			j.printStackTrace();
			System.out.println("not a jsonObject" +state);
		}catch(RequestParseException rpe){
			System.out.println("not a request");
		}
//		System.out.println("Flowing into the system "+name+":"+state);
		return false;
	}


}