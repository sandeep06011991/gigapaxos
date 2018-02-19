package edu.umass.cs.txn.txpackets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import org.json.JSONException;
import org.json.JSONObject;

import java.security.Key;
import java.util.Random;

public class TXResult extends TXPacket{
//  opId is used to match packets after they have been recieved in the ProtocolTask
//  Used for identifying within the same key
//  Example diffrent operations
    String opId;
//    Used to demultiplex to protocolExecutor Handler
//    All packets for the same state have the same key
    String key;

    private enum  KEYS{
        OPPACKETTYPE, SUCCESS, KEY, OPID
    }

    boolean success=false;

    IntegerPacketType opPacketType;

    public TXResult(String txid,TXPacket.PacketType packetType,boolean success,String key,String opId) {
        super(PacketType.RESULT, txid);
        opPacketType=packetType;
        this.success=success;
        this.key=key;
        this.opId=opId;
    }
    @Override
    public JSONObject toJSONObject() throws JSONException{
        JSONObject jsonObject=super.toJSONObject();
//        FIXME: Use ENUM to do all this
        jsonObject.put("opPacketType",opPacketType.getInt());
        jsonObject.put("success",success);
        jsonObject.put("key",key);
        jsonObject.put("opId",opId);
        return jsonObject;
    }


    public TXResult(JSONObject jsonObject) throws JSONException{
        super(jsonObject);
        opPacketType= TXPacket.PacketType.intToType.get( jsonObject.getInt("opPacketType"));
        success=jsonObject.getBoolean("success");
        key=jsonObject.getString("key");
        opId=jsonObject.getString("opId");
    }


    public boolean isFailed() {
        return !success;
    }

    @Override
    public String getKey(){
        return key;
    }

    public String getOpId(){return opId;}

    public void setRequestId(long requestId){this.requestId=requestId;}

}
