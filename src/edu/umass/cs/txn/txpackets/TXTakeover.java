package edu.umass.cs.txn.txpackets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import org.json.JSONException;
import org.json.JSONObject;

public class TXTakeover extends TXPacket{

//  Set by the Messenger before sending the Message
    String nodeId;

    public TXTakeover(IntegerPacketType t, String txid,String leader) {
        super(t, txid,leader);

    }
    @Override
    public boolean needsCoordination(){
        return true;
    }

    @Override
    public JSONObject toJSONObject() throws JSONException {
        JSONObject jsonObject=super.toJSONObject();
        assert nodeId !=null;
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

    public void setNewLeader(String nodeId){
        this.nodeId=nodeId;
    }
}
