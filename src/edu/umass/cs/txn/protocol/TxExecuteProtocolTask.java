package edu.umass.cs.txn.protocol;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.txn.Transaction;
import edu.umass.cs.txn.interfaces.TxOp;
import edu.umass.cs.txn.txpackets.LockRequest;
import edu.umass.cs.txn.txpackets.TXPacket;
import edu.umass.cs.txn.txpackets.TxOpRequest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class TxExecuteProtocolTask<NodeIDType>
        implements ProtocolTask<NodeIDType, TXPacket.PacketType, String> {

    public Transaction transaction;

    public TxExecuteProtocolTask(Transaction transaction){
        this.transaction=transaction;
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[]
    handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType,TXPacket.PacketType, String>[] ptasks) {
        if(event instanceof TxOpRequest){
            ptasks[0]=new TxExecuteProtocolTask<NodeIDType>(this.transaction);
        }
        ProtocolExecutor.enqueueCancel(this.getKey());
        System.out.println("Execute Phase Complete");

        return null;
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] start() {
        ArrayList<TxOp> txOps=transaction.getTxOps();
        TxOp txOp=txOps.get(0);
        TxOpRequest txOpRequest=new TxOpRequest(transaction.getTXID(),txOp);
        ArrayList<TxOpRequest> send=new ArrayList<>();
        send.add(txOpRequest);
        GenericMessagingTask temp = new GenericMessagingTask<NodeIDType, TxLockProtocolTask>((Object[] )null, send.toArray());
        return temp.toArray();
    }

    @Override
    public Set<TXPacket.PacketType> getEventTypes() {
        Set<TXPacket.PacketType> set=new HashSet<>();
        set.add(TXPacket.PacketType.TX_OP_REQUEST);
        return set;
    }

    @Override
    public String getKey() {
        return transaction.getTXID()+"Execute";
    }
}
