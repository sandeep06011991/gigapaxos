package edu.umass.cs.txn.protocol;

import com.sun.org.apache.regexp.internal.RE;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.txn.Transaction;
import edu.umass.cs.txn.txpackets.*;

import java.util.*;

public  class TxCommitProtocolTask<NodeIDType> extends
        TransactionProtocolTask<NodeIDType>{

    TreeSet<String> awaitingUnLock;

    public TxCommitProtocolTask(Transaction t, ProtocolExecutor protocolExecutor)
    {
        super(t,protocolExecutor);
    }



    @Override
    public TransactionProtocolTask onStateChange(TxStateRequest request) {
        if(request.getState() == TxState.COMPLETE){
            System.out.println("Commit Sequence complete!!!!!!!!!!!!!!!!");
            return null;
        }
        if(request.getState() == TxState.COMMITTED){
            return new TxCommitProtocolTask(transaction,protocolExecutor);
        }
        System.out.println("As already decision is made to Commit,cannot "+request.getState());
        return new TxCommitProtocolTask(transaction,protocolExecutor);


//      throw new RuntimeException("To change state from Commit to"+ request.getState()+"is a safety violation");
    }

    @Override
    public TransactionProtocolTask onTakeOver(TXTakeover request, boolean isPrimary) {
        if(isPrimary){
//            FIXME: study request retry of gigapaxos to fix this
            return null;
        }
        return new TxSecondaryProtocolTask(transaction,TxState.COMMITTED,getProtocolExecutor());
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[]
    handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType,TXPacket.PacketType, String>[] ptasks) {
        if((event instanceof TXResult) &&((TXResult)event).opPacketType==TXPacket.PacketType.UNLOCK_REQUEST){
            TXResult txResult = (TXResult)event;

            if(awaitingUnLock.contains(txResult.getOpId()) && !txResult.isFailed()){
                System.out.println("Unlocks recieved");
                awaitingUnLock.remove(txResult.getOpId());
            }
            if(awaitingUnLock.isEmpty()){
                System.out.println("All unlocks recieved");
                ArrayList<Request> re= new ArrayList<>();
                TxStateRequest request=new TxStateRequest(transaction.getTXID(),TxState.COMPLETE,transaction.getLeader());
                TxClientResult response = new TxClientResult(transaction,true);
                re.add(request);
                re.add(response);
                return getMessageTask(re);
            }

        }
        return null;
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] start() {
//        FIXME: Forgot to clean this code
        awaitingUnLock = transaction.getLockList();
        if(awaitingUnLock.isEmpty()){return  getMessageTask(new TxStateRequest(transaction.getTXID(),TxState.COMPLETE,transaction.getLeader()));}
        ArrayList<Request> requests=new ArrayList<>();
        for (String t : awaitingUnLock) {
//          Low Priority: cleaner method exists
            UnlockRequest unlockRequest= new UnlockRequest(t, transaction.getTXID(),true,transaction.getLeader());
            requests.add(unlockRequest);

        }
        System.out.println("Begin UnLocking");

        return getMessageTask(requests);
    }


    @Override
    public Set<TXPacket.PacketType> getEventTypes()
    {   Set<TXPacket.PacketType> txPackets=new HashSet<>();
        txPackets.add(LockRequest.PacketType.UNLOCK_REQUEST);
        txPackets.add(TXPacket.PacketType.RESULT);
        return txPackets;
    }

    @Override
    public String getKey() {
        return transaction.getTXID();
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] restart() {
//            FIXME: To build an exponential back off
            return start();
        }
}
