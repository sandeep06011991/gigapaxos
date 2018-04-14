package edu.umass.cs.txn.exceptions;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashSet;

public class TxnState {

//  Transaction ID also used as lockID
    public String txId;
//  Checkpointed state before the start of the transaction
//  Also used for rollbacks
    public String state;

    public ArrayList<String> requests = new ArrayList<>();
    public HashSet<Long> requestId = new HashSet<>();

    public  TxnState(String txId,String state){
        this.txId = txId    ;
        this.state = state;
    }

    public  void add_request(ClientRequest request){
        requests.add(request.toString());
        requestId.add(new Long(request.getRequestID()));
    }

    public TxnState(JSONObject jsonObject) throws JSONException{
        this.txId = jsonObject.getString("txId");
        this.state =jsonObject.getString("state");
        if(jsonObject.has("requests")){
            JSONArray jsonArray= jsonObject.getJSONArray("requests");
            JSONArray jsonArray1= jsonObject.getJSONArray("requestId");

            for(int i=0;i<jsonArray.length();i++ ){
                requests.add(jsonArray.getString(i));
                requestId.add(jsonArray1.getLong(i));
            }
        }

    }


    public JSONObject toJSONObject() {
        try {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("txId", txId);
            jsonObject.put("state",state);
            if(requests.size()!=0) {
                jsonObject.put("requests", requests);
                jsonObject.put("requestId",requestId);
            }
            return jsonObject;
        }catch(Exception e){
            throw new RuntimeException("Unsuccessfull in creating json object");
        }
    }

    @Override
    public String toString() {
        return toJSONObject().toString();
    }
}
