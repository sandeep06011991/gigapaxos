package edu.umass.cs.txn.protocol;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.SchedulableProtocolTask;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.txn.DistTransactor;
import edu.umass.cs.txn.Transaction;
import edu.umass.cs.txn.txpackets.TXPacket;
import edu.umass.cs.txn.txpackets.TXTakeover;
import edu.umass.cs.txn.txpackets.TxState;
import edu.umass.cs.txn.txpackets.TxStateRequest;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class TxSecondaryProtocolTask<NodeIDType> extends TransactionProtocolTask<NodeIDType> implements
        SchedulableProtocolTask<NodeIDType, TXPacket.PacketType, String>  {

    TxState state;




    public TxSecondaryProtocolTask(Transaction transaction, TxState state
            , ProtocolExecutor protocolExecutor){

        super(transaction,protocolExecutor);
        this.state=state;
    }

    public TxState getState() {
        return state;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    @Override
    public TransactionProtocolTask onStateChange(TxStateRequest request) {
        TxState newState=request.getState();
        if((state != TxState.INIT ) && (newState !=state)){
           throw new RuntimeException("SAFETY VOILATION");
        }
        return new TxSecondaryProtocolTask(transaction,newState,getProtocolExecutor());
    }

    @Override
    public TransactionProtocolTask onTakeOver(TXTakeover request,boolean isPrimary) {
        if(isPrimary){
            return new TxLockProtocolTask<>(transaction,getProtocolExecutor());
        }else{
            return new TxSecondaryProtocolTask(transaction,state,getProtocolExecutor());
        }
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[]
    handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType,TXPacket.PacketType, String>[] ptasks) {
        throw new RuntimeException("Should never be called");
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
        TXTakeover request=new TXTakeover(TXPacket.PacketType.TX_TAKEOVER,transaction.getTXID());
        return getMessageTask(request);
    }


    @Override
    public long getPeriod() {
//        FIXME: Write a test that Test this getPeriod
//        FIXME: Write a random wait Period generator
        return  1000000000;
    }


}
