package edu.umass.cs.txn.protocol;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.txn.Transaction;
import edu.umass.cs.txn.txpackets.LockRequest;
import edu.umass.cs.txn.txpackets.TXInitRequest;
import edu.umass.cs.txn.txpackets.TXPacket;

import java.util.*;

public class TxLockProtocolTask<NodeIDType> implements
        ProtocolTask<NodeIDType, TXPacket.PacketType, String>{

    Transaction transaction;

    public TxLockProtocolTask(Transaction transaction){
        this.transaction=transaction;
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[]
    handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType,TXPacket.PacketType, String>[] ptasks) {
        if(event instanceof LockRequest){
            ptasks[0]=new TxExecuteProtocolTask<NodeIDType>(this.transaction);
        }
        ProtocolExecutor.enqueueCancel(this.getKey());
        System.out.println("Lock Phase Complete");
        return null;
    }

    @Override
    public GenericMessagingTask<NodeIDType, TxLockProtocolTask>[] start() {

        TreeSet<String> tt = transaction.getLockList();
        ArrayList<Integer> integers = new ArrayList<Integer>(1);
        GenericMessagingTask<NodeIDType, TxLockProtocolTask>[] mtasks = new GenericMessagingTask[tt.size()];
        int i=0;
        for (String t : tt) {
            Request lockRequest = new LockRequest(t, transaction);
            ArrayList<Request> obj = new ArrayList(1);
            obj.add(lockRequest);
            GenericMessagingTask temp =
                    new GenericMessagingTask<NodeIDType, TxLockProtocolTask>(integers.toArray(), obj.toArray());
            mtasks[i]=temp;
            System.out.println("Begin Locking");
            }
        return mtasks;
    }


    @Override
    public Set<TXPacket.PacketType> getEventTypes()
    {   Set<TXPacket.PacketType> txPackets=new HashSet<>();
        txPackets.add(LockRequest.PacketType.LOCK_REQUEST);
        return txPackets;
    }

    @Override
        public String getKey() {
            return transaction.getTXID()+"Lock";
        }


}
