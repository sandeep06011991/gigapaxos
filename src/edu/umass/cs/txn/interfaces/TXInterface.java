package edu.umass.cs.txn.interfaces;


import edu.umass.cs.gigapaxos.interfaces.Request;

/**
 * @author arun
 * 
 * The Iterable<TxOp> interface should return an iterator over the
 * individual ordered operations constituting this transaction. 
 *
 */
public interface TXInterface extends TXRequest {
	/**
	 * @return Transaction ID.
	 */
	public String getTXID();
}
