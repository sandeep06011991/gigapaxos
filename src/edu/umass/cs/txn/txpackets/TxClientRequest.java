package edu.umass.cs.txn.txpackets;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.noop.NoopAppRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Random;

public class TxClientRequest extends JSONPacket implements ClientRequest{

    private ArrayList<ClientRequest> requests;
    private final long requestId;

    public InetSocketAddress clientAddr;

    public InetSocketAddress recvrAddr;

    static Random random=new Random();
    private enum Keys {
        REQUESTS,REQUESTID
    }

    public TxClientRequest(ArrayList<ClientRequest> requests){
        // FIXME: Ideally should not refer to not parent classes packet, gives clean implementation
        super(TXPacket.PacketType.TX_CLIENT);
        this.requests=requests;
        this.requestId=random.nextLong();
    }

    @Override
    public ClientRequest getResponse() {
        throw new RuntimeException("Client Response not yet built");
    }

    @Override
    public IntegerPacketType getRequestType() {
        return TXPacket.PacketType.TX_CLIENT;
    }

    @Override
    public String getServiceName() {
//        FIXME: Tx ClientRequests should not be having a service name;
        return "Some irrelevant Service Name is sent";
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
//        FIXME: Check if converting explicitly to a JSON object is required
        JSONObject jsonObject=new JSONObject();
        ArrayList<JSONObject> j=new ArrayList<>();
        for(Request request:requests){
            if (!(request instanceof AppRequest)) {
                throw new RuntimeException("This was only built for NoopAppRequest");
            }
            j.add(((AppRequest)request).toJSONObject());
        }
        jsonObject.put(Keys.REQUESTS.toString(),j);
        jsonObject.put(Keys.REQUESTID.toString(),requestId);
        return jsonObject;
    }

    public TxClientRequest(JSONObject jsonObject) throws JSONException{
        super(jsonObject);
        JSONArray jsonArray=jsonObject.getJSONArray(Keys.REQUESTS.toString());
        requests=new ArrayList<>();
        for(int i=0;i<jsonArray.length();i++){
            requests.add(new AppRequest((JSONObject) jsonArray.get(i)));
        }
        requestId=jsonObject.getLong(Keys.REQUESTID.toString());
    }

    @Override
    public long getRequestID() {
        return requestId;
    }

    public ArrayList<ClientRequest> getRequests() {
        return requests;
    }

}
