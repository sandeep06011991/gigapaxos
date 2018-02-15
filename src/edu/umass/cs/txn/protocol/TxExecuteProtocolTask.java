package edu.umass.cs.txn.protocol;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.txn.DistTransactor;
import edu.umass.cs.txn.Transaction;
import edu.umass.cs.txn.interfaces.TxOp;
import edu.umass.cs.txn.txpackets.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class TxExecuteProtocolTask<NodeIDType>
        extends TransactionProtocolTask<NodeIDType> {

    int count;

    public TxExecuteProtocolTask(Transaction transaction){
        super(transaction);
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[]
    handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType,TXPacket.PacketType, String>[] ptasks) {
        count--;
        if(count==0) {
            TxStateRequest stateRequest = new TxStateRequest(this.transaction.getTXID(), "COMMIT");
            System.out.println("Execute Phase Complete");
            return getMessageTask(stateRequest);
        }else{
            return null;
        }
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] start() {
//      Fix: right now only one request per service name.
//      Easy to extend this, do it on light days
        System.out.println("Execute initiated");
        ArrayList<TxOp> txOps=transaction.getTxOps();
        count=txOps.size();
        GenericMessagingTask<NodeIDType, TxLockProtocolTask>[] mtasks = new GenericMessagingTask[count];
        int i=0;
        for(TxOp txOp:txOps){
            GenericMessagingTask temp = new GenericMessagingTask<NodeIDType,TxOp>((NodeIDType) "Something",txOp);
            mtasks[i]=temp;
            i++;
        }
        return mtasks;
    }

    @Override
    public Set<TXPacket.PacketType> getEventTypes() {
        Set<TXPacket.PacketType> set=new HashSet<>();
        set.add(TXPacket.PacketType.TX_OP_REQUEST);
        set.add(TXPacket.PacketType.RESULT);
        return set;
    }

    @Override
    public String getKey() {
//     Fix: Why  should there be seperate keys going forward.
        return transaction.getTXID()+"Execute";
    }
}
