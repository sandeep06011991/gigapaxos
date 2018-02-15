package edu.umass.cs.txn.protocol;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.txn.Transaction;
import edu.umass.cs.txn.txpackets.LockRequest;
import edu.umass.cs.txn.txpackets.TXPacket;
import edu.umass.cs.txn.txpackets.TXResult;

import java.util.*;

public class TxLockProtocolTask<NodeIDType> extends
        TransactionProtocolTask<NodeIDType>{

    TreeSet<String> awaitingLock=null;

    public TxLockProtocolTask(Transaction transaction){
        super(transaction);
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[]
    handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType,TXPacket.PacketType, String>[] ptasks) {
        //Fix:
        if(event instanceof TXResult){
            TXResult txResult=(TXResult)event;
            String opId=txResult.getOpId();
            if(awaitingLock.remove(opId)){
                System.out.println("Lock "+opId +" "+txResult.isFailed());
            }
        }
        if(awaitingLock.isEmpty()){
            System.out.println("All locks recieved");
            ProtocolExecutor.enqueueCancel(this.getKey());
            ptasks[0]=new TxExecuteProtocolTask<NodeIDType>(this.transaction);
        }
        return null;
    }

    @Override
    public GenericMessagingTask<NodeIDType, TxLockProtocolTask>[] start() {
        if(awaitingLock==null){
            awaitingLock = transaction.getLockList();
        }
        ArrayList<Integer> integers = new ArrayList<Integer>(1);
        GenericMessagingTask<NodeIDType, TxLockProtocolTask>[] mtasks = new GenericMessagingTask[awaitingLock.size()];
        int i=0;
        for (String t : awaitingLock) {
//          Low Priority: cleaner method exists
            Request lockRequest = new LockRequest(t, transaction);
            ArrayList<Request> obj = new ArrayList(1);
            obj.add(lockRequest);
            GenericMessagingTask<NodeIDType, TxLockProtocolTask> temp =
                    new GenericMessagingTask(integers.toArray(), obj.toArray());
            mtasks[i]=temp;
            i++;
            System.out.println("Begin Locking");
            }
        return mtasks;
    }


    @Override
    public Set<TXPacket.PacketType> getEventTypes()
    {   Set<TXPacket.PacketType> txPackets=new HashSet<>();
        txPackets.add(LockRequest.PacketType.LOCK_REQUEST);
        txPackets.add(TXPacket.PacketType.RESULT);
        return txPackets;
    }

    @Override
        public String getKey() {
            return transaction.getTXID()+"Lock";
        }


}
