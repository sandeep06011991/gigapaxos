package edu.umass.cs.txn.txpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.txn.Transaction;

import java.util.Random;

/**
 * @author arun
 *
 */
public class LockRequest extends TXPacket {

	private static enum Keys {
		LOCKID, TXID
	};

	private final String lockID;


	public LockRequest(String lockId,String txId){
		super(PacketType.LOCK_REQUEST,txId);
		this.lockID=lockId;
	}
	/**
	 * @param json
	 * @throws JSONException
	 */
	public LockRequest(JSONObject json) throws JSONException {
		super(json);
		this.lockID = json.getString(Keys.LOCKID.toString());
	}

	public JSONObject toJSONObject() throws JSONException{
		JSONObject jsonObject=super.toJSONObject();
		jsonObject.put(Keys.LOCKID.toString(),lockID);
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
	public Object getKey() {
		return txid;
	}


}
