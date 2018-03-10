package edu.umass.cs.txn.protocol;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.txn.Transaction;
import edu.umass.cs.txn.txpackets.TXPacket;
import edu.umass.cs.txn.txpackets.TXTakeover;
import edu.umass.cs.txn.txpackets.TxStateRequest;

import java.util.ArrayList;

abstract public class TransactionProtocolTask<NodeIDType> implements
        ProtocolTask<NodeIDType, TXPacket.PacketType, String> {

    Transaction transaction;

    ProtocolExecutor protocolExecutor;

    TransactionProtocolTask(Transaction transaction,ProtocolExecutor protocolExecutor){

        this.transaction=transaction;
        this.protocolExecutor=protocolExecutor;
    }

    public Transaction getTransaction() {
        return transaction;

    }

    public ProtocolExecutor getProtocolExecutor() {
        return protocolExecutor;
    }

    static Object[] dummy={null,null};

    public GenericMessagingTask<NodeIDType, ?>[] getMessageTask(Request request){
        Request[] ls=new Request[1];
        ls[0]=request;
        GenericMessagingTask temp = new GenericMessagingTask<NodeIDType,TxExecuteProtocolTask>(dummy,ls);
        GenericMessagingTask<NodeIDType, TxLockProtocolTask>[] mtasks = new GenericMessagingTask[1];
        mtasks[0]=temp;
        return mtasks;
    }

    public GenericMessagingTask<NodeIDType,?>[] getMessageTask(ArrayList<?> requests){
//    FIXME:Dont know why I have to do this wierd
        GenericMessagingTask<NodeIDType,?>[] ret=new GenericMessagingTask[requests.size()];
        int i=0;
        ArrayList<Integer> integers=new ArrayList<>(1);
        for(Object request:requests){
            ArrayList<Object> req=new ArrayList<>();
            req.add(request);
            ret[i]=new GenericMessagingTask<>(integers.toArray(),req.toArray());
            i++;
        }
        return ret;
    }

    // Returns the protocol task that must be spawned in place of the current protocol task
//    when state change request is recieved
    public abstract TransactionProtocolTask onStateChange(TxStateRequest request);

    public abstract TransactionProtocolTask onTakeOver(TXTakeover request,boolean isPrimary);

    public void cancel(){
//        FIXME: Is there a better way to cancel a task
        protocolExecutor.remove(this.getKey());
    }
}

