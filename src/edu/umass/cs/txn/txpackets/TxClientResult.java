package edu.umass.cs.txn.txpackets;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestIdentifier;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.txn.Transaction;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

public class TxClientResult extends JSONPacket implements Request,RequestIdentifier,ClientRequest {

    public boolean success;

    long requestId;

    InetSocketAddress serverAddr;
    InetSocketAddress clientAddr;

    Set<String> activesPrevious;

    public TxClientResult(Transaction transaction,boolean success){
        super(TXPacket.PacketType.TX_CLIENT_RESPONSE);
        this.requestId = transaction.requestId;
        this.success = success;
        this.serverAddr = transaction.entryServer;
        this.clientAddr = transaction.clientAddr;
    }





    public TxClientResult(JSONObject json) throws JSONException {
        super(json);
        success = json.getBoolean("success");
        requestId = json.getLong("reqID");
        clientAddr = getSocketAddrFromString(json.getString("clientAddr"));
        serverAddr = getSocketAddrFromString(json.getString("serverAddr"));
        if(json.has("Actives")){
            activesPrevious = new HashSet<>();
            JSONArray tt = json.getJSONArray("Actives");
            for(int t=0;t<tt.length();t++){
                activesPrevious.add(tt.getString(t));
            }
        }
//        assert !success && activesPrevious.size()>0;
    }

    @Override
    public JSONObject toJSONObject() throws JSONException {
        JSONObject jsonObject=super.toJSONObject();
        jsonObject.put("reqID",requestId);
        jsonObject.put("success",success);
        jsonObject.put("clientAddr",clientAddr.toString());
        jsonObject.put("serverAddr",serverAddr.toString());
        jsonObject.put("Actives",activesPrevious);

//        assert !success && activesPrevious.size()>0;
        return jsonObject;
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
        return new JSONObject();
    }


    private static InetSocketAddress getSocketAddrFromString(String str){
        str = str.replace("/","");
        String[] a= str.split(":");
        return new InetSocketAddress(a[0],Integer.parseInt(a[1]));
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

    public InetSocketAddress getClientAddr() {
        return clientAddr;
    }

    public InetSocketAddress getServerAddr() {
        return serverAddr;
    }

    public void setActivesPrevious(Set<String> activesPrevious){
        this.activesPrevious = activesPrevious;
    }

    public Set<String> getActivesPrevious(){return  activesPrevious;}
}
