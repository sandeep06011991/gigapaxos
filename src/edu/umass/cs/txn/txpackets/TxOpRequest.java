package edu.umass.cs.txn.txpackets;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.txn.interfaces.TxOp;
import redis.clients.jedis.Client;

import java.util.Random;

/**
 * @author arun
 *
 */
public class TxOpRequest extends TXPacket  {
	private static enum Keys {
		REQUEST,OPID
	}

	public ClientRequest request;


	public int opId=-10000;
	/**
	 * @param txid
	 * @param request
	 */
	public TxOpRequest(String txid, ClientRequest request,int opId) {
		super(TXPacket.PacketType.TX_OP_REQUEST, txid);
		this.request = request;
		this.opId = opId;
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
//		FIXME App Request parser should be fixed
		super(json);

		this.request =(ClientRequest) parser
				.getRequest(json.getString(Keys.REQUEST.toString()));
		this.opId	 =  json.getInt(Keys.OPID.toString());
	}


	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject jsonObject=super.toJSONObject();

		if((request !=null) && (request instanceof JSONPacket)){
			jsonObject.put(Keys.REQUEST.toString(),((JSONPacket)request).toJSONObject());
//			jsonObject.put(Keys.REQUEST.toString(),((AppRequest)request).toJSONObject());
		}else{
			throw new RuntimeException("TxOpRequests must be JSON objects");
		}
	 	jsonObject.put(Keys.OPID.toString(),opId);
		return jsonObject;
	}

	public TxOpRequest(JSONObject jsonObject)throws JSONException{
		super(jsonObject);
		throw new RuntimeException("Should not be called without parser");
//		FIXME: Should not be App Specific
//		super(jsonObject);
//		JSONObject jsonObject1=jsonObject.getJSONObject(Keys.REQUEST.toString());
//		request=new AppRequest(jsonObject1);
//		this.opId	 =  jsonObject.getInt(Keys.OPID.toString());
	}

	public String getServiceName(){
		return this.request.getServiceName();
	}


	@Override
	public Object getKey() {
		return txid;
	}


}
