package edu.umass.cs.txn.txpackets;

import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.txn.interfaces.TxOp;

/**
 * @author arun
 *
 */
public class TxOpRequest extends TXPacket implements TxOp {

	private static enum Keys {
		REQUEST,
	}

	private final Request request;

	/**
	 * @param txid
	 * @param request
	 */
	public TxOpRequest(String txid, Request request) {
		super(TXPacket.PacketType.TX_OP_REQUEST, txid);
		this.request = request;
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
		if(request instanceof AppRequest){
			jsonObject.put(Keys.REQUEST.toString(),((AppRequest) request).toJSONObjectImpl());
		}
		return jsonObject;
	}

	public TxOpRequest(JSONObject jsonObject)throws JSONException{
		super(jsonObject);
		String req=jsonObject.getString(Keys.REQUEST.toString());
		try {
			request=NoopApp.staticGetRequest(req);
		}catch(Exception ex){
			throw new JSONException("Request could not be parsed");
		}
		}

	public String getServiceName(){
		return this.request.getServiceName();
	}
}
