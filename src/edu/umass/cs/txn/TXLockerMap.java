package edu.umass.cs.txn;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.txn.exceptions.TXException;
import edu.umass.cs.txn.interfaces.TXLocker;
import org.omg.SendingContext.RunTime;

import java.util.HashMap;
import java.util.HashSet;

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

	private AbstractReplicaCoordinator app;

	TXLockerMap(AbstractReplicaCoordinator abstractReplicaCoordinator){
		app = abstractReplicaCoordinator;
	}

//	<Service name, Tx ID>
	private HashMap<String,String> txMap=new HashMap<>();
//	<Service name , State string>
	private HashMap<String,String> stateMap=new HashMap<>();
//	FIXME: Get clearance from Arun
//	FIXME: More than one request possible, make this a HashSet<Long>
//	<Service name, request ID>
//	request ID is added to this list after recieving a valid request
	private HashMap<String,HashSet<Long>> allowedRequests = new HashMap<>();

	@Override
	public boolean lock(String serviceName,String lockID){
		if(txMap.containsKey(serviceName)){
			String currlockId=txMap.get(serviceName);
			return currlockId.equals(lockID);
		}
		if(!(txMap.containsKey(serviceName))){
			String state=app.checkpoint(serviceName);
			txMap.put(serviceName,lockID);
			stateMap.put(serviceName,state);
			return true;
		}
		return false;
	}


	/**
	 * A blocking call that returns upon successfully release {@code lockID} or
	 * throws a {@link TXException} .
	 * 
	 * @param lockID
	 * @throws TXException
	 */
	public boolean unlock(String serviceName,String lockID)  {
		if((txMap.containsKey(serviceName))){
			String lckID=txMap.get(serviceName);
			if(lckID.equals(lockID)){
				stateMap.remove(serviceName);
				txMap.remove(serviceName);
				if(allowedRequests.containsKey(serviceName)){
					allowedRequests.remove(serviceName);
				}
				return true;
			}
		}
		return false;
	}
/* Returns
* 	handled: false if this is a new request
* 	and true if the request is already handled
* 	FIXME: allowRequest should be a void not a boolean return
*   should throw an exception when a repeated request is given
*
*/
	public boolean allowRequest(long requestId,String txID,String serviceName){
		if(txMap.containsKey(serviceName) && txMap.get(serviceName).equals(txID)){
			if(!allowedRequests.containsKey(serviceName)){
				HashSet set = new HashSet();
				set.add(new Long(requestId));
				allowedRequests.put(serviceName,set);
				return false;
			}else{
				HashSet set= allowedRequests.get(serviceName);
				if(set.contains(requestId)){return  true;}
				set.add(requestId);
				return false;
			}
		}
		throw new RuntimeException("Only a locked group recieves TxOP request");
	}

	public boolean isAllowedRequest(ClientRequest clientRequest){
		String serviceName=clientRequest.getServiceName();
		if(!isLocked(serviceName)){return true;}
		if(allowedRequests.containsKey(serviceName)&& allowedRequests.get(serviceName).contains(clientRequest.getRequestID())){
//			System.out.println("Request Is allowed");
			return true;
		}
		return false;
	}

	/*Is the service name locked by any transaction
	* used to filter incoming requests */
	public boolean isLocked(String serviceName){
		if(txMap.containsKey(serviceName)){
			return true;
		}
		return false;
	}

	/*Is this */
	public boolean isLockedByTxn(String serviceName, String txId){
		if(txMap.containsKey(serviceName)){
			String _txId=txMap.get(serviceName);
			return txId.equals(_txId);
		}
		return false;
	}
}
