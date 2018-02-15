package edu.umass.cs.txn.txpackets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import org.json.JSONException;
import org.json.JSONObject;

public class TXTakeover extends TXPacket{

    String nodeId;

    public TXTakeover(IntegerPacketType t, String txid,String nodeId) {
        super(t, txid);
        this.nodeId=nodeId;
    }
    @Override
    public boolean needsCoordination(){
        return true;
    }

    @Override
    public JSONObject toJSONObject() throws JSONException {
        JSONObject jsonObject=super.toJSONObject();
        jsonObject.put("nodeId",nodeId);
        return jsonObject;
    }

    public TXTakeover(JSONObject jsonObject) throws JSONException{
        super(jsonObject);
        nodeId=jsonObject.getString("nodeId");
    }

    @Override
    public Object getKey() {
        return txid;
    }


    public String getNewLeader(){return nodeId;}
}
