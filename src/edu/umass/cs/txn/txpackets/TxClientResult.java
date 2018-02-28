package edu.umass.cs.txn.txpackets;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestIdentifier;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import org.json.JSONException;
import org.json.JSONObject;

public class TxClientResult extends JSONPacket implements Request,RequestIdentifier,ClientRequest {

    public boolean success;

    long requestId;

    public TxClientResult(long requestId,boolean success){
        super(TXPacket.PacketType.TX_CLIENT_RESPONSE);
        this.requestId = requestId;
        this.success = success;
    }


    public TxClientResult(JSONObject json) throws JSONException {
        super(json);
        success = json.getBoolean("success");
        requestId = json.getLong("reqID");

    }

    @Override
    public JSONObject toJSONObject() throws JSONException {
        JSONObject jsonObject=super.toJSONObject();
        jsonObject.put("reqID",requestId);
        jsonObject.put("success",success);
        return jsonObject;
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
        return new JSONObject();
    }


    @Override
    public IntegerPacketType getRequestType() {
        return TXPacket.PacketType.TX_CLIENT_RESPONSE ;
    }

    @Override
    public String getServiceName() {
        return "Again irrelevant";
    }

    @Override
    public long getRequestID() {
        return requestId;
    }

    @Override
    public ClientRequest getResponse() {
        throw new RuntimeException("Unncessary ");
    }
}
