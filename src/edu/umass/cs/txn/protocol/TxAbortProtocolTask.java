package edu.umass.cs.txn.protocol;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.txn.Transaction;
import edu.umass.cs.txn.txpackets.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class TxAbortProtocolTask<NodeIDType>
            extends TransactionProtocolTask<NodeIDType> {
//    FIXME: There should be a retry mechanism
    TreeSet<String> unlockList;

    public TxAbortProtocolTask(Transaction transaction, ProtocolExecutor protocolExecutor) {
        super(transaction, protocolExecutor);
        unlockList = transaction.getLockList();
    }

    @Override
    public TransactionProtocolTask onStateChange(TxStateRequest request) {
        if(TxState.COMPLETE == request.getState()){
            System.out.println("Abort sequence completed");
            return null;
        }
        throw new RuntimeException("Safety Violation");
        /*  Only a primary can initite state change and if if there is a another primary, a state change request
             * would be processed before this is handled */
    }

    @Override
    public TransactionProtocolTask onTakeOver(TXTakeover request, boolean isPrimary) {
        if(isPrimary){throw new RuntimeException("Safety Violation");}
        /*A primary would not issue a take over */
        return new TxSecondaryProtocolTask(transaction, TxState.ABORTED,protocolExecutor);
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType, TXPacket.PacketType, String>[] ptasks) {
        if((event instanceof TXResult) && (((TXResult) event).opPacketType == TXPacket.PacketType.UNLOCK_REQUEST)){
            TXResult txResult=(TXResult)event;
            String opId= txResult.getOpId();
            if(unlockList.remove(opId)){
                System.out.println("Unlocked "+opId);
            }
        }
        if(unlockList.isEmpty()){
            TxClientResult result=new TxClientResult(transaction.requestId,false,transaction.entryServer,transaction.clientAddr);
            System.out.println("Complete and Cancelled");
            return getMessageTask(result);
        }
        return null;
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] start() {
        ArrayList<Request> requests = new ArrayList<>();
        for(String t: unlockList ){
            UnlockRequest unlockRequest = new UnlockRequest(t,transaction.getTXID(),false);
            requests.add(unlockRequest);
        }
        System.out.println("Abort Sequence initiated");

        return getMessageTask(requests);
    }

    @Override
    public Set<TXPacket.PacketType> getEventTypes() {
        Set<TXPacket.PacketType> txPackets = new HashSet<>();
        txPackets.add(TXPacket.PacketType.RESULT);
        return txPackets;
    }

    @Override
    public String getKey() {
        return transaction.getTXID();
    }

}
