package edu.umass.cs.txn.txpackets;
import edu.umass.cs.txn.Transaction;
import org.json.JSONException;
import org.json.JSONObject;

public class TXInitRequest extends TXPacket{
    public Transaction transaction;


    public TXInitRequest(Transaction transaction) {
        super(PacketType.TX_INIT, transaction.getTXID());
        this.transaction=transaction;
    }

    public TXInitRequest(JSONObject jsonObject) throws JSONException{
        super(jsonObject);
        this.transaction= new  Transaction(jsonObject.getJSONObject("transaction"));
    }

    @Override
    public boolean needsCoordination(){
        return true;
    }


    @Override
    public JSONObject toJSONObject() throws JSONException {
        JSONObject jsonObject=super.toJSONObject();
        jsonObject.put("transaction",transaction.toJSONObject());
        return jsonObject;
    }

    @Override
    public Object getKey() {
        return txid;
    }
}
