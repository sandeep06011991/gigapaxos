package edu.umass.cs.txn.protocol;


import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.txn.txpackets.TXPacket;

import java.util.Set;

public class TxCommitProtocolTask<NodeIDType>
        implements ProtocolTask<NodeIDType, TXPacket, String> {

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] handleEvent(ProtocolEvent<TXPacket, String> event, ProtocolTask<NodeIDType, TXPacket, String>[] ptasks) {
        return new GenericMessagingTask[0];
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] start() {
        return new GenericMessagingTask[0];
    }

    @Override
    public Set<TXPacket> getEventTypes() {
        return null;
    }

    @Override
    public String getKey() {
        return null;
    }
}
