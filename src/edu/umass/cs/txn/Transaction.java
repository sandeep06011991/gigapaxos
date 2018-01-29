package edu.umass.cs.txn;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.txn.interfaces.TXInterface;
import edu.umass.cs.txn.interfaces.TxOp;
import edu.umass.cs.txn.txpackets.TxOpRequest;
import edu.umass.cs.utils.GCConcurrentHashMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author arun
 *
 * 
 *         A transaction is an indivisible sequence of operations that satisfy
 *         ACID (atomicity, consistency, isolation, and durability) properties.
 * 
 *         <p>
 * 
 *         The list of individual {@link TxOp} operations must either be a
 *         {@link ClientRequest} or {@link ClientReconfigurationPacket}. If any
 *         of the TxOp operations is a {@link DeleteServiceName} request, the
 *         deletion(s) need to be implemented as an in-memory pre-delete
 *         operation at both reconfigurators and active replicas so that they
 *         can be reverted if necessary.
 *
 */
public class Transaction implements TXInterface {

	protected static enum Keys {
		TXOPS, TXID, DELETES,
	}

	// a transaction number chosen to be unique at each client
	private final long txn;

	// the server issuing the transaction
	public InetSocketAddress entryServer;

	String nodeId;

	ArrayList<TxOp> txOps;
	/**
	 * @param entryServer
	 */
	public Transaction(InetSocketAddress entryServer ,ArrayList<TxOp> txOps) {
		this.txn = getNewTxid(entryServer);
		this.entryServer = entryServer;
		this.txOps=txOps;
	}

	/**
	 * @return The set of lock identifiers needed for this transaction in
	 *         lexicographic order. The lock identifiers are in general a subset
	 *         of participant groups in the transaction as name creation and
	 *         deletion requests are excluded from this list.
	 */
	public TreeSet<String> getLockList() {
		TreeSet<String> set=new TreeSet<>();
		for(TxOp txOp:txOps){
			set.add(txOp.getServiceName());
		}
		return set;
	}

	/**
	 * @return The sequence of operations constituting this transaction.
	 *         Modifying this sequence will violate safety.
	 */
	public ArrayList<TxOp> getTxOps() {
		return this.txOps;
	}

	/**
	 * @return An ID for this transaction created by concatenating the issuer's
	 *         address and the long transaction number; for safety, this ID must
	 *         be unique across all transactions in the system, so the same
	 *         issuer must not issue different transactions with the same
	 *         transaction number.
	 */
	public String getTXID() {
		return Long.toString(txn);
//		return this.entryServer.getAddress().getHostAddress() + ":"
//				+ this.entryServer.getPort() + ":" + this.txn;
	}

	/**
	 * The transaction group name must be globally unique, otherwise the
	 * transaction will fail in the very first tx_group creation step.
	 * 
	 * @return The name of this transaction, which also acts as the name of the
	 *         replica group conducting the transaction. The name is composed of
	 *         the sender ID and a long transaction number that is chosen
	 *         uniquely at each sender.
	 */
	public String getTxGroupName() {
		return this.getTXID();
	}

	protected String getTxInitState() {
		throw new RuntimeException("Unimplemented");
	}

	protected InetSocketAddress getEntryServer() {
		return this.entryServer;
	}

	private static final long DEFAULT_TIMEOUT = 3600 * 1000;
	private static GCConcurrentHashMap<Long, String> txids = new GCConcurrentHashMap<Long, String>(
			DEFAULT_TIMEOUT);

	private synchronized static long getNewTxid(InetSocketAddress initiator) {
		return getNewTxid(initiator.getAddress().getHostAddress() + ":"
				+ initiator.getPort());
	}

	private synchronized static long getNewTxid(String initiator) {
		long txid = (long) (Math.random() * Long.MAX_VALUE);
		while (txids.contains(txid))
			txid = (long) (Math.random() * Long.MAX_VALUE);
		txids.put(txid, initiator);
		return txid;
	}

	protected synchronized static String releaseTxid(long txid) {
		return txids.remove(txid);
	}


	@Override
	public Iterator<TxOp> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	public JSONObject toJSONObject() throws JSONException{
		JSONObject jsonObject=new JSONObject();
		jsonObject.put("txId",txn);
		ArrayList<JSONObject> j=new ArrayList<>();
		for(TxOp txOp:txOps){
			j.add(txOp.toJSONObject());
		}
		jsonObject.put("txOps",j);
		if(nodeId!=null) {
			jsonObject.put("nodeId", nodeId);
		}
		return jsonObject;
	}

	public Transaction(JSONObject jsonObject) throws JSONException{
		txn=jsonObject.getInt("txId");
		JSONArray j_txops=jsonObject.getJSONArray("txOps");
		txOps=new ArrayList<>();
		for(int i=0;i<j_txops.length();i++){
			txOps.add(new TxOpRequest((JSONObject) j_txops.get(i)));
		}
		if(jsonObject.has("nodeId")){
			nodeId = jsonObject.getString("nodeId");
		}

	}

	public void setEntryServer(InetSocketAddress inetSocketAddress){
		this.entryServer=inetSocketAddress;
	}

}
