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
// FixMe: Build a more efficient Retry mechanism
    ArrayList<String> sent=new ArrayList<>();

    public TxExecuteProtocolTask(Transaction transaction){
        super(transaction);
    }

    @Override
    public TransactionProtocolTask onStateChange(TxStateRequest request) {
        if(request.getState()== TxState.COMMITTED){
            return new TxCommitProtocolTask(transaction);
        }else{
            throw new RuntimeException("Abort Unimplemented");
        }
    }

    @Override
    public TransactionProtocolTask onTakeOver(TXTakeover request,boolean isPrimary) {
        if(isPrimary){throw new RuntimeException("Safety Violation");}
        return new TxSecondaryProtocolTask(transaction,TxState.INIT);
    }


    @Override
    public GenericMessagingTask<NodeIDType, ?>[]
    handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType,TXPacket.PacketType, String>[] ptasks) {
        if((event instanceof TXResult)&&(((TXResult) event).getTXPacketType()==TXPacket.PacketType.TX_OP_REQUEST)){
            TXResult txResult=(TXResult)event;
            System.out.println("Recieved Operation"+txResult.getOpId());
            sent.remove(txResult.getOpId());
            if(sent.isEmpty()){
                TxStateRequest stateRequest = new TxStateRequest(this.transaction.getTXID(),TxState.COMMITTED);
                System.out.println("Execute Phase Complete");
                return getMessageTask(stateRequest);
            }
        }
        return null;
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] start() {
//      Fix: right now only one request per service name.
//      Easy to extend this, do it on light days
        System.out.println("Execute initiated");
//        ArrayList<Request> requests=transaction.getRequests();
        ArrayList<TxOpRequest> txOps=new ArrayList<>();
        int op=0;
        for(Request request:transaction.getRequests()){
            txOps.add(new TxOpRequest(transaction.getTXID(),request,op));
            sent.add(Integer.toString(op));
            op++;
        }
        return getMessageTask(txOps);
    }

    @Override
    public Set<TXPacket.PacketType> getEventTypes() {
        Set<TXPacket.PacketType> set=new HashSet<>();
        set.add(TXPacket.PacketType.RESULT);
        return set;
    }

    @Override
    public String getKey() {
        return transaction.getTXID();
    }
}
