package edu.umass.cs.txn.protocol;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class TxExecuteProtocolTask<NodeIDType>
        extends TransactionProtocolTask<NodeIDType> {
    // FixMe: Build a more efficient Retry mechanism
//    ArrayList<String> sent=new ArrayList<>();
//    Request Ids that are awaiting execution
    ArrayList<Long> toExecuteRequests = new ArrayList<>();
    HashMap<Long,ClientRequest> map = new HashMap<>();

    public TxExecuteProtocolTask(Transaction transaction,ProtocolExecutor protocolExecutor)
    {
        super(transaction,protocolExecutor);
        for(Request r: transaction.getRequests()){
//            FixME: This casting should not be required, change definition in transaction
            toExecuteRequests.add(((ClientRequest)r).getRequestID());
            map.put(((ClientRequest)r).getRequestID(),(ClientRequest) r);
        }
    }

    @Override
    public TransactionProtocolTask onStateChange(TxStateRequest request) {
        if(request.getState()== TxState.COMMITTED){
            return new TxCommitProtocolTask(transaction,protocolExecutor);
        }else{
            assert request.getState() == TxState.ABORTED;
            return new TxAbortProtocolTask(transaction,protocolExecutor);
        }
    }

    @Override
    public TransactionProtocolTask onTakeOver(TXTakeover request,boolean isPrimary) {
        if(isPrimary){throw new RuntimeException("Safety Violation");}
        return new TxSecondaryProtocolTask(transaction,TxState.INIT,getProtocolExecutor());
    }


    @Override
    public GenericMessagingTask<NodeIDType, ?>[]
    handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType,TXPacket.PacketType, String>[] ptasks) {
        if((event instanceof TXResult)&&(((TXResult) event).opPacketType==TXPacket.PacketType.TX_OP_REQUEST)){
            TXResult txResult=(TXResult)event;
            System.out.println("Recieved Operation"+txResult.getOpId());
            assert toExecuteRequests.get(0) == Long.parseLong(txResult.getOpId());
            toExecuteRequests.remove(0);
            if(toExecuteRequests.isEmpty()){
                TxStateRequest stateRequest = new TxStateRequest(this.transaction.getTXID(),TxState.COMMITTED);
                System.out.println("Execute Phase Complete");
                return getMessageTask(stateRequest);
            }else{
                return sendPendingMessage();
            }
        }
        return null;
    }

    private GenericMessagingTask<NodeIDType, ?>[] sendPendingMessage(){
        assert toExecuteRequests.size() > 0;
        Long rqId = toExecuteRequests.get(0);
        ClientRequest toSend = map.get(rqId);
        return  getMessageTask(new TxOpRequest(transaction.getTXID(),toSend));
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] start() {
        return sendPendingMessage();
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
