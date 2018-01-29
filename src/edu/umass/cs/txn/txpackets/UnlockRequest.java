package edu.umass.cs.txn.txpackets;

import edu.umass.cs.txn.protocol.TxMessenger;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author arun
 *
 */
public class UnlockRequest extends TXPacket {

	private static enum Keys {
		UNLOCKID, TXID
	};

	private final String lockID;
	private final String txID;

	public UnlockRequest(String lockID, String txID) {
		super(TXPacket.PacketType.UNLOCK_REQUEST, txID);
		this.lockID = lockID;
		this.txID = txID;
	}

	public UnlockRequest(JSONObject json) throws JSONException {
		super(json);
		this.lockID = json.getString(Keys.UNLOCKID.toString());
		this.txID = json.getString(Keys.TXID.toString());
	}

	public String getTXID() {
		return this.txID;
	}

	public JSONObject toJSONObject() throws JSONException{
		JSONObject jsonObject=super.toJSONObject();
		jsonObject.put(Keys.UNLOCKID.toString(),this.lockID);
		return jsonObject;

	}


	public String getLockID() {
		return this.lockID;
	}

	public String getServiceName() {
		return this.getLockID();
	}

	@Override
	public boolean needsCoordination() {
		return true;
	}

	@Override
	public Object getKey() {
		return txid+"Unlock";
	}

}
