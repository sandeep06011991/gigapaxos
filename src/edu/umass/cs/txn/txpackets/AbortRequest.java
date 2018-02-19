package edu.umass.cs.txn.txpackets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author arun
 * 
 *
 *
 */
public class AbortRequest extends TXPacket {


	public AbortRequest( String txid) {
		super(PacketType.ABORT_REQUEST, txid);
	}

	@Override
	public Object getKey() {
		return "Abort"+txid;
	}
}
