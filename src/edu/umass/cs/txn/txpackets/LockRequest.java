package edu.umass.cs.txn.txpackets;

import org.json.JSONException;
import org.json.JSONObject;


/**
 * @author arun
 *
 */
public class LockRequest extends TXPacket {

	private static enum Keys {
		SERVICENAME, TXID
	};

	private final String serviceName;


	public LockRequest(String serviceName ,String txId,String leader){
		super(PacketType.LOCK_REQUEST,txId,leader);
		this.serviceName = serviceName;
	}
	/**
	 * @param json
	 * @throws JSONException
	 */
	public LockRequest(JSONObject json) throws JSONException {
		super(json);
		this.serviceName = json.getString(Keys.SERVICENAME.toString());
	}

	public JSONObject toJSONObject() throws JSONException{
		JSONObject jsonObject=super.toJSONObject();
		jsonObject.put(Keys.SERVICENAME.toString(),serviceName);
		return jsonObject;
	}
	/**
	 * @return Transaction id is the identification of
	 *
	 */
	public String getLockID() {
		return this.txid;
	}

	public String getServiceName() {
		return this.serviceName;
	}

	@Override
	public Object getKey() {
		return txid;
	}

}
