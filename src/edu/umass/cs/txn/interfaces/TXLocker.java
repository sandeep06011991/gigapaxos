package edu.umass.cs.txn.interfaces;

import edu.umass.cs.txn.exceptions.TXException;

/**
 * @author arun
 *
 */
public interface TXLocker {
	/**
	 * A blocking call that returns upon successfully locking {@code lockID}
	 * or throws a {@link TXException}. Locking a group involves synchronously
	 * checkpointing its state and maintaining in memory its locked status.
	 * 
	 * @param lockID
	 * @throws TXException
	 */
	//Why is this void??
	public boolean lock(String serviceName,String lockID,String state) throws TXException;

	public boolean	isLocked(String lockID);

	public boolean unlock(String serviceName,String lockID) throws TXException;
}
