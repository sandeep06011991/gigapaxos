package edu.umass.cs.txn.protocol;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.txn.Transaction;
import edu.umass.cs.txn.txpackets.*;
import org.omg.SendingContext.RunTime;

import java.util.*;

public class TxLockProtocolTask<NodeIDType> extends
        TransactionProtocolTask<NodeIDType>{

//    The lock protocol has 3 options to transition into
//    1) AbortProtocol: If the locks could not be acquired
//    2) Execute Protocol: If the locks could be acquired
//    3) onTakeOver: If a secondary timesout


    TreeSet<String> awaitingLock;

    public TxLockProtocolTask(Transaction transaction,ProtocolExecutor protocolExecutor,Set<String> leaderActives)
    {
        super(transaction,protocolExecutor,leaderActives,null);
        awaitingLock = transaction.getLockList();
    }

    @Override
    public TransactionProtocolTask onStateChange(TxStateRequest request) {
        if(request.getState() == TxState.ABORTED){
//            This would have been started by the same primary
//            if the primary had changed, there would be a take over message in between
            if(previousLeaderActives==null)previousLeaderActives = request.getQuorum();
            return new TxAbortProtocolTask(transaction,protocolExecutor,leaderActives,previousLeaderActives);
        }
        if(request.getState() == TxState.COMMITTED){
//            This happens when the server is recovering
            return new TxCommitProtocolTask(transaction,protocolExecutor,leaderActives,null);
        }

        throw new RuntimeException("Safety Violation"+request.toString());
    }

    @Override
    public TransactionProtocolTask onTakeOver(TXTakeover request,boolean isPrimary) {
        if(isPrimary){
/*      FIXME: Why is this possible
            Check request retry mechanism with arun
            Assumption: requests with same reqID are ignored.
 *      If a node recieves an old take over message */
            return  null;
        }
        return new TxSecondaryProtocolTask(transaction,TxState.INIT,getProtocolExecutor(),leaderActives,previousLeaderActives);
    }


    @Override
    public GenericMessagingTask<NodeIDType, ?>[]
    handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType,TXPacket.PacketType, String>[] ptasks) {
        if((event instanceof TXResult)
                &&(((TXResult) event).opPacketType==TXPacket.PacketType.LOCK_REQUEST)){
            TXResult txResult=(TXResult)event;
            String opId=txResult.getOpId();
            //FIXME: Is there a better way to map lock opId to Lock requests
            if(txResult.isFailed()){
            TxStateRequest stateRequest = new TxStateRequest(this.transaction.getTXID(),TxState.ABORTED, this.transaction.getLeader());
            assert txResult.getPrevLeaderQuorumIfFailed().size() >0;
            stateRequest.setFailedActives(txResult.getPrevLeaderQuorumIfFailed());
                System.out.println("Locks"  +   txResult.getOpId()+  " must be busy");
                return getMessageTask(stateRequest);
            }
            if(awaitingLock.remove(opId)){
                System.out.println("Lock "+opId +" "+txResult.isFailed());
            }
        if(awaitingLock.isEmpty()){
                System.out.println("All locks recieved");
                this.cancel();
                ptasks[0]=new TxExecuteProtocolTask(this.transaction,getProtocolExecutor(),leaderActives,previousLeaderActives);
            }
        }
        return null;
    }

    @Override
    public GenericMessagingTask<NodeIDType,?>[] start() {
        ArrayList<Request> requests=new ArrayList<>();
        for (String t : awaitingLock) {
//          Low Priority: cleaner method exists
            LockRequest lockRequest = new LockRequest(t, transaction.getTXID(),transaction.getLeader(),leaderActives);
            requests.add(lockRequest);

            }
        System.out.println("Begin Locking");
        return getMessageTask(requests);

    }


    @Override
    public Set<TXPacket.PacketType> getEventTypes()
    {   Set<TXPacket.PacketType> txPackets=new HashSet<>();
        txPackets.add(TXPacket.PacketType.RESULT);
        return txPackets;
    }

    @Override
        public String getKey() {
            return transaction.getTXID();
        }


}
