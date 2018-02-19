package edu.umass.cs.txn.txpackets;

import org.json.JSONException;
import org.json.JSONObject;


public class TxStateRequest extends TXPacket {

	enum KEYS{
		STATE
	};

//	FIXME: Should this be public
	TxState state;

	public TxStateRequest(String txId,TxState state) {
		super(TXPacket.PacketType.TX_STATE_REQUEST, txId);
		this.state=state;
	}

	public boolean needsCoordination(){return true;}

	public TxStateRequest(JSONObject json) throws JSONException {
		super(json);
		state=TxState.valueOf(json.getString(KEYS.STATE.toString()));
	}

	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject jsonObject = super.toJSONObject();
		jsonObject.put(KEYS.STATE.toString(),state);
		return jsonObject;
	}

	public TxState getState() {
		return this.state;
	}

	@Override
	public Object getKey() {
		return this.txid;
	}
}
