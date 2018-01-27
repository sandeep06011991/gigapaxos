package edu.umass.cs.txn.txpackets;

import edu.umass.cs.protocoltask.ProtocolEvent;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.txn.exceptions.ResponseCode;
import edu.umass.cs.utils.IntegerPacketTypeMap;

/**
 * @author arun
 *
 *         All transaction processing related packets inherit from this class.
 */
public abstract class TXPacket extends JSONPacket implements ReplicableRequest,
		ClientRequest,ProtocolEvent {

	/**
	 * Transaction packet types.
	 */
	public enum PacketType implements IntegerPacketType {

		/**
		 * 
		 */
		LOCK_REQUEST(251),

		/**
		 * 
		 */
		UNLOCK_REQUEST(252),

		/**
		 * 
		 */
		ABORT_REQUEST(253),

		/**
		 * 
		 */
		COMMIT_REQUEST(254),

		/**
		 * 
		 */
		TX_STATE_REQUEST(255), 
		
		/**
		 * 
		 */
		TX_OP_REQUEST (256),


		TX_INIT(257),

		LOCK_OK(258)
		;

		private final int number;

		PacketType(int t) {
			this.number = t;
		}

		public int getInt() {
			return number;
		}

		/**
		 * 
		 */
		public static final IntegerPacketTypeMap<PacketType> intToType = new IntegerPacketTypeMap<PacketType>(
				PacketType.values());
	}
	public TXPacket response;
	/* The tuple <txid, initiator> is used to detect conflicting txids chosen by
	 * different initiating nodes. */
	protected final String txid;
	public boolean failed=false;
	private ResponseCode code = null;
	private IntegerPacketType packetType;
	private static enum Keys {
		TXID, LOCKID, INITIATOR
	}

	/**
	 * @param t
	 * @param txid 
	 */
	public TXPacket(IntegerPacketType t, String txid) {
		super(t);
		this.txid = txid;
	}


	/**
	 * @param json
	 * @throws JSONException
	 */
	public TXPacket(JSONObject json) throws JSONException {
		super(json);
		this.txid = json.getString(Keys.TXID.toString());
		this.failed=json.getBoolean("failed");

	}

	@Override
	public IntegerPacketType getRequestType() {
		return PacketType.intToType.get(type);
	}


	@Override
	public String getServiceName() {
		// TODO Auto-generated method stub
		return "Service_name_txn";
	}

	@Override
	public long getRequestID() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ClientRequest getResponse() {
		// TODO Auto-generated method stub
		return response;
	}

	@Override
	public boolean needsCoordination() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected JSONObject toJSONObjectImpl() throws JSONException {
		// TODO Auto-generated method stub
		return new JSONObject();
	}

	/**
	 * @return True if this request failed; false otherwise.
	 */
	public boolean isFailed() {
		return this.failed;
	}
	//Why is this private
	public TXPacket setFailed() {
		this.failed = true;
		return this;
	}

	/**
	 * @return Response code.
	 */
	public ResponseCode getResponseCode() {
		return this.code;
	}

	/**
	 * @param code
	 */
	public void setResponseCode(ResponseCode code) {
		this.code = code;
	}

	/**
	 * @return The ID of the transaction to which this packet corresponds. The
	 *         initiator is set only after a transaction request has been
	 *         received by the entry server, so until then, the ID only has the
	 *         transaction number.
	 */
	public String getTXID() {
		return this.txid;
	}

	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject jsonObject=super.toJSONObject();
		jsonObject.put(Keys.TXID.toString(),txid);
		jsonObject.put("failed",this.failed);
		return jsonObject;
	}

	public PacketType getTXPacketType(){
		return (PacketType)getRequestType();
	}

	@Override
	public Object getType() {
		return getTXPacketType();
	}

	@Override
	public Object getMessage() {
		return TXPacket.this;
	}

	@Override
	public void setKey(Object key) {
		throw new RuntimeException("set Key event should never be called");
	}

	@Override
	public Object getKey() {
		return txid;
	}

}
