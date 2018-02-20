package edu.umass.cs.txn.protocol;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.txn.Transaction;
import edu.umass.cs.txn.txpackets.*;

import java.util.*;

public  class TxCommitProtocolTask<NodeIDType> extends
        TransactionProtocolTask<NodeIDType>{

    TreeSet<String> awaitingUnLock;

    public TxCommitProtocolTask(Transaction t,ProtocolExecutor protocolExecutor)
    {
        super(t,protocolExecutor);
    }



    @Override
    public TransactionProtocolTask onStateChange(TxStateRequest request) {
        throw new RuntimeException("Commit Protocol is the end, no more request to see");
    }

    @Override
    public TransactionProtocolTask onTakeOver(TXTakeover request, boolean isPrimary) {
        if(isPrimary){throw new RuntimeException("Safety VIOLATION");}
        return new TxSecondaryProtocolTask(transaction,TxState.COMMITTED,getProtocolExecutor());
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[]
    handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType,TXPacket.PacketType, String>[] ptasks) {
        if((event instanceof TXResult) &&((TXResult)event).getTXPacketType()==TXPacket.PacketType.UNLOCK_REQUEST){
            TXResult txResult = (TXResult)event;
            if(awaitingUnLock.contains(txResult.getOpId()) && !txResult.isFailed()){
                awaitingUnLock.remove(txResult.getOpId());
            }
            if(awaitingUnLock.isEmpty()){
                System.out.println("YIPPEE COMMIT PROTOCOL COMPLETE");
                this.cancel();
            }

        }
        return null;
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] start() {
//        FIXME: Forgot to clean this code
        awaitingUnLock = transaction.getLockList();
        ArrayList<Request> requests=new ArrayList<>();
        for (String t : awaitingUnLock) {
//          Low Priority: cleaner method exists
            UnlockRequest unlockRequest= new UnlockRequest(t, transaction.getTXID());
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

}
