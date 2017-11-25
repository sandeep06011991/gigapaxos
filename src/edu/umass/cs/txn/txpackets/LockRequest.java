package edu.umass.cs.txn.txpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.txn.Transaction;

/**
 * @author arun
 *
 */
public class LockRequest extends TXPacket {

	private static enum Keys {
		LOCKID, TXID
	};

	private final String lockID;

	private boolean coordination=true;

	/**
	 * @param lockID
	 * @param tx
	 */
	public LockRequest(String lockID, Transaction tx) {
		super(TXPacket.PacketType.LOCK_REQUEST, tx.getTXID());
		this.lockID = lockID;
	}

	public LockRequest(String lockId,String txId){
		super(TXPacket.PacketType.LOCK_OK,txId);
		this.lockID=lockId;
		this.coordination=false;

	}
	/**
	 * @param json
	 * @throws JSONException
	 */
	public LockRequest(JSONObject json) throws JSONException {
		super(json);
		this.lockID = json.getString(Keys.LOCKID.toString());
		if(json.has("response")){
			this.response=new LockRequest(json.getJSONObject("response"));
		}
	}
	public JSONObject toJSONObject() throws JSONException{
		JSONObject jsonObject=super.toJSONObject();
		jsonObject.put(Keys.LOCKID.toString(),lockID);
		if(response!=null){
			jsonObject.put("response",response.toJSONObject());
		}
		return jsonObject;
	}
	/**
	 * @return Service name that also acts as a lock ID.
	 */
	public String getLockID() {
		return this.lockID;
	}

	public String getServiceName() {
		return this.getLockID();
	}

	@Override
	public boolean needsCoordination() {
		return coordination;
	}
}
