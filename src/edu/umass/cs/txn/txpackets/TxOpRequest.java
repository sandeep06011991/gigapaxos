package edu.umass.cs.txn.txpackets;

import edu.umass.cs.reconfiguration.examples.AppRequest;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.txn.interfaces.TxOp;

import java.util.Random;

/**
 * @author arun
 *
 */
public class TxOpRequest extends TXPacket implements TxOp {

	private static enum Keys {
		REQUEST,
	}

	public Request request;

	public long requestId;

	/**
	 * @param txid
	 * @param request
	 */
	public TxOpRequest(String txid, Request request) {
		super(TXPacket.PacketType.TX_OP_REQUEST, txid);
		this.request = request;
		requestId = new Random().nextLong();

	}

	/**
	 * The parser should be able to convert a serialized request to either an
	 * app request or client reconfiguration packet as appropriate.
	 * 
	 * @param json
	 * @param parser
	 * @throws JSONException
	 * @throws RequestParseException
	 */
	public TxOpRequest(JSONObject json, AppRequestParser parser)
			throws JSONException, RequestParseException {
		super(json);
		requestId=json.getLong("reqId");
		this.request = parser
				.getRequest(json.getString(Keys.REQUEST.toString()));
	}

	//@Override
	public String getTxID() {
		return this.txid;
	}

	@Override
	public boolean handleResponse(Request response) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isBlocking() {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject jsonObject=super.toJSONObject();
		if(request !=null){
			jsonObject.put(Keys.REQUEST.toString(),((AppRequest)request).toJSONObject());
		}
		jsonObject.put("reqId",requestId);
		return jsonObject;
	}

	public TxOpRequest(JSONObject jsonObject)throws JSONException{
		super(jsonObject);
		JSONObject jsonObject1=jsonObject.getJSONObject(Keys.REQUEST.toString());
		request=new AppRequest(jsonObject1);
		requestId=jsonObject.getLong("reqId");
	}

	public String getServiceName(){
		return this.request.getServiceName();
	}


	@Override
	public Object getKey() {
		return txid+"Execute";
	}

	@Override
	public long getRequestID() {
		// TODO Auto-generated method stub
		return this.requestId ;
	}

	@Override
	public boolean needsCoordination() {
		return true;
	}

}
