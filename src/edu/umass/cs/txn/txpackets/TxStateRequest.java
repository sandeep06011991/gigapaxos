package edu.umass.cs.txn.txpackets;

import edu.umass.cs.txn.Transaction;
import org.json.JSONException;
import org.json.JSONObject;

public class TxStateRequest extends TXPacket {

	public static enum State {
		COMMITTED, 
		
		ABORTED, 
		
		EXECUTING,
	}

//	private State state = State.EXECUTING;
	String state;

	public TxStateRequest(String txId,String state) {
		super(TXPacket.PacketType.TX_STATE_REQUEST, txId);
		this.state=state;
	}

	public boolean needsCoordination(){return true;}

	public TxStateRequest(JSONObject json) throws JSONException {
		super(json);
		state=json.getString("state");		// TODO Auto-generated constructor stub
	}

	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject jsonObject = super.toJSONObject();
		jsonObject.put("state",state);
		return jsonObject;
	}

	public String getState() {
		return this.state;
	}

	@Override
	public Object getKey() {
		return this.txid;
	}
}
