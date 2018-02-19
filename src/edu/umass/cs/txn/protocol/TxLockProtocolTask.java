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


    TreeSet<String> awaitingLock;

    public TxLockProtocolTask(Transaction transaction){
        super(transaction);
    }

    @Override
    public TransactionProtocolTask onStateChange(TxStateRequest request) {
        if(request.getState()== TxState.COMMITTED){
            return new TxCommitProtocolTask(transaction);
        }else{
            throw new RuntimeException("Abort Unimplemented");
        }
    }

    @Override
    public TransactionProtocolTask onTakeOver(TXTakeover request,boolean isPrimary) {
        if(isPrimary){throw new RuntimeException("Safety Violation");}
        return new TxSecondaryProtocolTask(transaction,TxState.INIT);
    }


    @Override
    public GenericMessagingTask<NodeIDType, ?>[]
    handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType,TXPacket.PacketType, String>[] ptasks) {
        if((event instanceof TXResult)&&(((TXResult) event).getTXPacketType()==TXPacket.PacketType.LOCK_REQUEST)){
            TXResult txResult=(TXResult)event;
            String opId=txResult.getOpId();
//FIXME: Is there a better way to map lock opId to Lock requests
            if(awaitingLock.remove(opId)){
                System.out.println("Lock "+opId +" "+txResult.isFailed());
            }

            if(awaitingLock.isEmpty()){
                System.out.println("All locks recieved");
                ProtocolExecutor.cancel(this);
                ptasks[0]=new TxExecuteProtocolTask(this.transaction);
            }
        }
        return null;
    }

    @Override
    public GenericMessagingTask<NodeIDType,?>[] start() {
        if(awaitingLock==null){
            awaitingLock = transaction.getLockList();
        }
        ArrayList<Request> requests=new ArrayList<>();
        for (String t : awaitingLock) {
//          Low Priority: cleaner method exists
            LockRequest lockRequest = new LockRequest(t, transaction.getTXID());
            requests.add(lockRequest);
            System.out.println("Begin Locking");
            }
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
