package edu.umass.cs.txn;

import edu.umass.cs.txn.exceptions.TXException;
import edu.umass.cs.txn.interfaces.TXLocker;

import java.util.HashMap;

/**
 * @author arun
 *
 */
public class TXLockerMap implements TXLocker {

	/**
	 * A blocking call that returns upon successfully locking {@code lockID} or
	 * throws a {@link TXException}. Locking a group involves synchronously
	 * checkpointing its state and maintaining in memory its locked status.
	 * 
	 * @param lockID
	 * @throws TXException
	 */

	private HashMap<String,String> txMap=new HashMap<>();
	private HashMap<String,String> stateMap=new HashMap<>();

	@Override
	public boolean lock(String serviceName,String lockID,String state) throws TXException {
		if(!(txMap.containsKey(serviceName))){
			txMap.put(serviceName,lockID);
			stateMap.put(serviceName,state);
			return true;
		}
		return false;
	}

	/**
	 * Acquires the locks in the order specified by {@code lockIDs}.
	 * 
	 * @param lockIDs
	 * @throws TXException
	 */
	public void lock(String[] lockIDs) throws TXException {
		throw new RuntimeException("Unimplemented");
	}

	/**
	 * A blocking call that returns upon successfully release {@code lockID} or
	 * throws a {@link TXException} .
	 * 
	 * @param lockID
	 * @throws TXException
	 */
	public boolean unlock(String serviceName,String lockID) throws TXException {
		if(!(txMap.containsKey(serviceName))){
			String lckID=txMap.get(serviceName);
			if(lckID.equals(lockID)){
				stateMap.remove(serviceName);
				txMap.remove(serviceName);
				return true;
			}
		}
		return false;
	}

	/**
	 * Releases the locks in the order specified by {@code lockIDs}.
	 * 
	 * @param lockIDs
	 * @throws TXException
	 */
	public void unlock(String[] lockIDs) throws TXException {
		throw new RuntimeException("Unimplemented");
	}


	public boolean isLocked(String serviceName){
		if(txMap.containsKey(serviceName)){
			return true;
		}
		return false;
	}
}
