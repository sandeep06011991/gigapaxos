package edu.umass.cs.txn.protocol;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.txn.Transaction;
import edu.umass.cs.txn.txpackets.TXPacket;

import java.util.ArrayList;

abstract class TransactionProtocolTask<NodeIDType> implements
        ProtocolTask<NodeIDType, TXPacket.PacketType, String> {

    Transaction transaction;

    TransactionProtocolTask(Transaction transaction){
    this.transaction=transaction;
    }

    public Transaction getTransaction() {
        return transaction;

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

    public GenericMessagingTask<NodeIDType,?>[] getMessageTask(ArrayList<Request> request){


        GenericMessagingTask temp = new GenericMessagingTask<NodeIDType,TxExecuteProtocolTask>(dummy, request.toArray());
        GenericMessagingTask<NodeIDType, TxLockProtocolTask>[] mtasks = new GenericMessagingTask[1];
        mtasks[0]=temp;
        return mtasks;
    }
}

