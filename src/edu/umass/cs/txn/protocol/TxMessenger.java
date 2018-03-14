package edu.umass.cs.txn.protocol;


import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.txn.txpackets.TXPacket;
import edu.umass.cs.txn.txpackets.TXTakeover;
import edu.umass.cs.txn.txpackets.TxClientResult;
import org.json.JSONException;

import java.io.IOException;
import java.net.InetSocketAddress;

public class TxMessenger<NodeIDType,Message> implements Messenger<NodeIDType,Message> {

    ReconfigurableAppClientAsync gpClient;

    public ProtocolExecutor pe;

    public void setProtocolExecutor(ProtocolExecutor pe){
        this.pe=pe;
    }

    AbstractReplicaCoordinator abstractReplicaCoordinator;


    public TxMessenger(ReconfigurableAppClientAsync gpClient, AbstractReplicaCoordinator abstractReplicaCoordinator) {
        this.gpClient = gpClient;
        this.abstractReplicaCoordinator=abstractReplicaCoordinator;
    }
    public void sendObject(Object message) {
        try {
            if(message instanceof TXTakeover){
                System.out.println(getMyID()+ "is attempting a takeover");
                ((TXTakeover) message).setNewLeader((String)getMyID());
            }
            if(message instanceof TxClientResult){
                TxClientResult txClientResult = (TxClientResult) message;
                try {
                    if(!this.abstractReplicaCoordinator.getMessenger().getListeningSocketAddress().equals(txClientResult.getServerAddr())){
                        System.out.println("Indirect response:send to entry server");
                        ((JSONMessenger) (this.abstractReplicaCoordinator.getMessenger())).sendClient(txClientResult.getServerAddr(),
                                txClientResult);

                    }else{
                        ((JSONMessenger) (this.abstractReplicaCoordinator.getMessenger())).sendClient(txClientResult.getClientAddr()
                                ,txClientResult,txClientResult.getServerAddr());

                    }
                }catch(JSONException ex){
                    throw new RuntimeException("Failed to send Response to client");
                }

                return;
            }

            this.gpClient.sendRequest((Request) message, new RequestCallback() {
                @Override

                public void handleResponse(Request response) {
                    if (response instanceof TXPacket) {
//                           System.out.println("Recieved a new TxPacket ");

//                           System.out.println(((TXPacket)response).getKey());
                           TxMessenger.this.pe.handleEvent((TXPacket)response);
                    } else {
                        throw new RuntimeException("Expected TxPacket");
                    }
                }
            });
        }catch(IOException ex){
            ex.printStackTrace();
            System.out.println("Exception");
        }
    }
    @Override
    public void send(GenericMessagingTask<NodeIDType, ?> mtask) throws IOException, JSONException {
        for(int i=0;i<mtask.msgs.length;i++){
            sendObject(mtask.msgs[i]);
        }
    }

    @Override
    public int sendToID(NodeIDType id, Message msg) throws IOException {
        sendObject(msg);
//        throw new RuntimeException("TxMessengerFunction not required");
        return 0;
    }

    @Override
    public int sendToAddress(InetSocketAddress isa, Message msg) throws IOException {
        sendObject(msg);
//        throw new RuntimeException("TxMessengerFunction not required");
        return 0;
    }

    @Override
    public void addPacketDemultiplexer(AbstractPacketDemultiplexer<?> pd) {
        throw new RuntimeException("TxMessengerFunction not required");
//        return 0;

    }

    @Override
    public void precedePacketDemultiplexer(AbstractPacketDemultiplexer<?> pd) {
        throw new RuntimeException("TxMessengerFunction not required");

    }

    @Override
    public NodeIDType getMyID() {
        return (NodeIDType) abstractReplicaCoordinator.getMyID();
//        throw new RuntimeException("TxMessengerFunction not required");
    }

    @Override
    public void stop() {
        throw new RuntimeException("TxMessengerFunction not required");

    }

    @Override
    public NodeConfig<NodeIDType> getNodeConfig() {
        throw new RuntimeException("TxMessengerFunction not required");
//        return null;
    }

    @Override
    public SSLDataProcessingWorker.SSL_MODES getSSLMode() {
        throw new RuntimeException("TxMessengerFunction not required");
//        return null;
    }

    @Override
    public int sendToID(NodeIDType id, byte[] msg) throws IOException {
        throw new RuntimeException("TxMessengerFunction not required");
//        return 0;
    }

    @Override
    public int sendToAddress(InetSocketAddress isa, byte[] msg) throws IOException {
        throw new RuntimeException("TxMessengerFunction not required");
//        return 0;
    }

    @Override
    public boolean isDisconnected(NodeIDType node) {
        throw new RuntimeException("TxMessengerFunction not required");
//        return false;
    }

    @Override
    public InetSocketAddress getListeningSocketAddress() {
        throw new RuntimeException("TxMessengerFunction not required");
//        return null;
    }

    @Override
    public boolean isStopped() {
        throw new RuntimeException("TxMessengerFunction not required");
//        return false;
    }


}
