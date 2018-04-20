package edu.umass.cs.txn.protocol;

import com.sun.org.apache.regexp.internal.RE;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.txn.Transaction;
import edu.umass.cs.txn.exceptions.ResponseCode;
import edu.umass.cs.txn.txpackets.*;

import java.util.*;

public  class TxCommitProtocolTask<NodeIDType> extends
        TransactionProtocolTask<NodeIDType>{

    TreeSet<String> awaitingUnLock;

    boolean respondToClient = true;

    public TxCommitProtocolTask(Transaction t, ProtocolExecutor protocolExecutor)
    {
        super(t,protocolExecutor);
    }



    @Override
    public void onStateChange(TxStateRequest request) {
        if(request.getState() == TxState.COMPLETE){
//            System.out.println("Commit Sequence complete!!!!!!!!!!!!!!!!"+transaction.getTXID());
            this.cancel();
        }

    }


    @Override
    public GenericMessagingTask<NodeIDType, ?>[]
    handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType,TXPacket.PacketType, String>[] ptasks) {
        if((event instanceof TXResult) &&((TXResult)event).opPacketType==TXPacket.PacketType.UNLOCK_REQUEST){
            TXResult txResult = (TXResult)event;

            if(awaitingUnLock.contains(txResult.getOpId()) && !txResult.isFailed()){
                awaitingUnLock.remove(txResult.getOpId());
            }
            if(awaitingUnLock.isEmpty()){
//                System.out.println("All locks recieved");
                ArrayList<Request> re= new ArrayList<>();
                TxStateRequest request=new TxStateRequest(transaction.getTXID(),TxState.COMPLETE,transaction.getLeader(),ResponseCode.COMPLETE,null);
                re.add(request);
                return getMessageTask(re);
            }

        }
        return null;
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] start() {
//        FIXME: Forgot to clean this code
        awaitingUnLock = transaction.getLockList();
        if(awaitingUnLock.isEmpty()){
            return  getMessageTask(new TxStateRequest(transaction.getTXID(),TxState.COMPLETE,transaction.getLeader()
                            ,ResponseCode.COMPLETE,null));
        }
        ArrayList<Request> requests=new ArrayList<>();
        for (String t : awaitingUnLock) {
//          Low Priority: cleaner method exists
            UnlockRequest unlockRequest= new UnlockRequest(t, transaction.getTXID(),true,transaction.getLeader());
            requests.add(unlockRequest);

        }
        if(respondToClient) {
            TxClientResult response = new TxClientResult(transaction, ResponseCode.COMMITTED,null);
            respondToClient = false;
            requests.add(response);
        }

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
