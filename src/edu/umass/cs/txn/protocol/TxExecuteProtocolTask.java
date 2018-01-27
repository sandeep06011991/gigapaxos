package edu.umass.cs.txn.protocol;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.txn.Transaction;
import edu.umass.cs.txn.txpackets.TXPacket;

import java.util.Set;

public class TxExecuteProtocolTask<NodeIDType>
        implements ProtocolTask<NodeIDType, TXPacket.PacketType, String> {

    public Transaction transaction;

    public TxExecuteProtocolTask(Transaction transaction){
        this.transaction=transaction;
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[]
    handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType, TXPacket.PacketType, String>[] ptasks) {
        return new GenericMessagingTask[0];
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] start() {
        return new GenericMessagingTask[0];
    }

    @Override
    public Set<TXPacket.PacketType> getEventTypes() {
        return null;
    }

    @Override
    public String getKey() {
        return null;
    }
}
