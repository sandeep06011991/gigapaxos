package edu.umass.cs.txn.protocol;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.txn.Transaction;
import edu.umass.cs.txn.txpackets.LockRequest;
import edu.umass.cs.txn.txpackets.TXPacket;
import edu.umass.cs.txn.txpackets.UnlockRequest;

import java.util.*;

public class TxCommitProtocolTask<NodeIDType> extends
        TransactionProtocolTask<NodeIDType>{

    public TxCommitProtocolTask(Transaction t){
        super(t);
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[]
    handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType,TXPacket.PacketType, String>[] ptasks) {
        System.out.println("YIPPEE COMMIT PROTOCOL COMPLETE");
        if(event instanceof UnlockRequest){
            ProtocolExecutor.enqueueCancel(this.getKey());
        }
        return null;
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] start() {
        TreeSet<String> tt = transaction.getLockList();
        ArrayList<Integer> integers = new ArrayList<Integer>(1);
        int i=0;
        GenericMessagingTask<NodeIDType, TxLockProtocolTask>[] mtasks = new GenericMessagingTask[tt.size()];
        for (String t : tt) {
            Request unlockRequest = new UnlockRequest(t, transaction.getTXID());
            ArrayList<Request> obj = new ArrayList(1);
            obj.add(unlockRequest);
            GenericMessagingTask temp =
                    new GenericMessagingTask<NodeIDType, TxLockProtocolTask>(integers.toArray(), obj.toArray());
            System.out.println(unlockRequest.getServiceName());
            mtasks[i]=temp;
            i++;
            System.out.println("Begin Unlocking");
        }
        return mtasks;
//        return new GenericMessagingTask[0];
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
        return transaction.getTXID()+"Unlock";
    }

}
