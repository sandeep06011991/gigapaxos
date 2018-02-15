package edu.umass.cs.txn.protocol;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.SchedulableProtocolTask;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.txn.DistTransactor;
import edu.umass.cs.txn.Transaction;
import edu.umass.cs.txn.txpackets.TXPacket;
import edu.umass.cs.txn.txpackets.TXTakeover;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class TxSecondaryProtocolTask<NodeIDType> implements
        SchedulableProtocolTask<NodeIDType, TXPacket.PacketType, String> {

    String state;

    Transaction transaction;

    public TxSecondaryProtocolTask(Transaction transaction,String state){
       this.transaction=transaction;
       this.state=state;
    }

    public String getState() {
        return state;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType, TXPacket.PacketType, String>[] ptasks) {
        // Fix:Just change state
        return null;
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] start() {
        return null;
    }

    @Override
    public Set<TXPacket.PacketType> getEventTypes() {
        Set<TXPacket.PacketType> txPackets=new HashSet<>();
        txPackets.add(TXPacket.PacketType.TX_STATE_REQUEST);
        return txPackets;
    }

    @Override
    public String getKey() {
        return transaction.getTXID();
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] restart() {
        AbstractReplicaCoordinator coordinator=DistTransactor.getSingleton();
        TXTakeover request=new TXTakeover(TXPacket.PacketType.TX_TAKEOVER,transaction.getTXID(),(String)coordinator.getMyID());
        try {
            coordinator.coordinateRequest(request, null);
        }catch (Exception ex){
            // Silent kill of exception;
            ex.printStackTrace();
            System.out.println("Failed to take over");
        }
        return null;
    }

    static Random random=new Random(10);
    @Override
    public long getPeriod() {
        //Low Priority: optimiza random generation
//        long period = 10000+(random.nextInt()%10000);
//        System.out.println(period);
        return  1000000000;
    }
}
