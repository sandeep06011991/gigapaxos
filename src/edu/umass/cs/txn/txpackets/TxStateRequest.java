package edu.umass.cs.txn.txpackets;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;


public class TxStateRequest extends TXPacket {

	enum KEYS{
		STATE
	};
	TxState state;

	Set<String> quorum;


	public TxStateRequest(String txId,TxState state,String leader) {
		super(TXPacket.PacketType.TX_STATE_REQUEST, txId,leader);
		this.state=state;
	}

	public boolean needsCoordination(){return true;}

	public TxStateRequest(JSONObject json) throws JSONException {
		super(json);
		state=TxState.valueOf(json.getString(KEYS.STATE.toString()));
		if(json.has("quorum")){
			JSONArray jsonArray = json.getJSONArray("quorum");
			quorum = new HashSet<>();
			for (int i = 0;i<jsonArray.length();i++){
				quorum.add(jsonArray.getString(i));
			}
		}
	}

	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject jsonObject = super.toJSONObject();
		jsonObject.put(KEYS.STATE.toString(),state);
		if(quorum!=null){
			jsonObject.put("quorum",quorum);
		}
		return jsonObject;
	}

	public void setFailedActives(Set<String> quorum){
		this.quorum = quorum;
	}

	public Set<String> getQuorum(){return quorum;}

	public TxState getState() {
		return this.state;
	}

	@Override
	public Object getKey() {
		return this.txid;
	}
}
