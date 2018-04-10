package edu.umass.cs.txn.testing;

import edu.umass.cs.gigapaxos.interfaces.*;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.txn.testing.app.*;
import edu.umass.cs.txn.txpackets.TXPacket;
import edu.umass.cs.txn.txpackets.TxClientRequest;
import edu.umass.cs.txn.txpackets.TxClientResult;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Sandeep
 *
 */
public class TxnClient extends ReconfigurableAppClientAsync<Request> {

    private int numResponses = 0;

    static TxnClient client;

    static boolean created = false;

    static void createSomething(){
        if(!created){
            try {
                client = new TxnClient();
                client.sendRequest(new CreateServiceName("Service_name_txn","0"));
                client.sendRequest(new CreateServiceName("name0", "0"));
                client.sendRequest(new CreateServiceName("name1","1"));
//                Fixme:Hack clean this up later
                TimeUnit.SECONDS.sleep(5);
            }catch(Exception e){
                e.printStackTrace();
                throw new RuntimeException("Unable to create");
            }
        }
        created = true;
    }

    void testGetRequest(int finalValue) throws IOException{
        sendRequest(new GetRequest("name0"), new InetSocketAddress("127.0.0.1", 2100), new RequestCallback() {
            @Override
            public void handleResponse(Request response) {
                ResultRequest rr= (ResultRequest)response;
                System.out.println("Result :"+rr.getResult());
                assert  rr.getResult() == finalValue;
                System.out.println("Get Test complete");

            }
        });
    }


    void testMultiLineTxn(){
        createSomething();
        InetSocketAddress entryServer=new InetSocketAddress("127.0.0.1",2100);
        ArrayList<ClientRequest> requests = new ArrayList<>();
        requests.add(new OperateRequest("name0", 10, OperateRequest.Operation.add));
        requests.add(new OperateRequest("name0", 20, OperateRequest.Operation.multiply));
        requests.add(new OperateRequest("name0", 10, OperateRequest.Operation.add));


        try{
            TxClientRequest txClientRequest = new TxClientRequest(requests);
            RequestFuture rd= sendRequest(txClientRequest,entryServer, new RequestCallback() {
                @Override
                public void handleResponse(Request response) {
                    if(response instanceof TxClientResult){
                        try {
                            System.out.println("Transaction status "+((TxClientResult) response).success);
                            testGetRequest(210);
                            System.out.println("Transaction test complete");
                        }catch(Exception e){
                            throw new RuntimeException("Transaction failed");
                        }
                    }
                }
            });

        }catch (Exception e){
            e.printStackTrace();
        }


    }

    void testAborting() throws IOException{
        createSomething();

        InetSocketAddress entryServer=new InetSocketAddress("127.0.0.1",2100);
        ArrayList<ClientRequest> requests = new ArrayList<>();
        requests.add(new OperateRequest("name0", 10, OperateRequest.Operation.add));
        requests.add(new OperateRequest("name1", 20, OperateRequest.Operation.add));
        TxClientRequest txClientRequest = new TxClientRequest(requests);

        ArrayList<ClientRequest> requests1 = new ArrayList<>();
        requests1.add(new GetRequest("name0"));
        TxClientRequest txClientRequest1 = new TxClientRequest(requests1);

        RequestFuture rd= sendRequest(txClientRequest,entryServer, new RequestCallback() {
            @Override
            public void handleResponse(Request response) {
                if(response instanceof TxClientResult){
                    try {
                        System.out.println("Transaction status "+((TxClientResult) response).success);
                        testGetRequest(10);
                        System.out.println("Transaction test complete");
                    }catch(Exception e){
                        throw new RuntimeException("Transaction failed");
                    }
                }
            }

        });

        sendRequest(txClientRequest1, entryServer, new RequestCallback() {
            @Override
            public void handleResponse(Request response) {
                assert response instanceof TxClientResult;
                TxClientResult txClientResult=(TxClientResult)response;
                System.out.println("Transaction 2 status"+txClientResult.success);
            }
        });
    }



    void testBasicCommit() throws IOException{
        createSomething();

        InetSocketAddress entryServer=new InetSocketAddress("hp066.utah.cloudlab.us",2100);
        ArrayList<ClientRequest> requests = new ArrayList<>();
        requests.add(new OperateRequest("name0", 10, OperateRequest.Operation.add));
        requests.add(new OperateRequest("name1", 20, OperateRequest.Operation.add));
        TxClientRequest txClientRequest = new TxClientRequest(requests);
        System.out.println("Attempting Send ");
        RequestFuture rd= sendRequestAnycast(txClientRequest, new RequestCallback() {
            @Override
            public void handleResponse(Request response) {
                if(response instanceof TxClientResult){
                    try {
                        System.out.println("Transaction status "+((TxClientResult) response).success);
                        testGetRequest(10);
                        System.out.println("Transaction test complete");
                    }catch(Exception e){
                        throw new RuntimeException("Transaction failed");
                    }
                }
            }
         });
    }

    /**
     * @throws IOException
     */
    public TxnClient() throws IOException {
        super();
    }


    @Override
    public Request getRequest(String stringified) {
        try {
/*      DEBUG tip: If requests are not being recieved debug here */
            JSONObject jsonObject=new JSONObject(stringified);
            if(jsonObject.getInt("type")==4){
                return new ResultRequest(jsonObject);
            }
            if(jsonObject.getInt("type")==262){
                return new TxClientResult(jsonObject);
            }
        } catch ( JSONException e) {
            // do nothing by designSys
            e.printStackTrace();
        }


        return null;
    }




    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        Set<IntegerPacketType> set = CalculatorTX.staticGetRequestTypes();
        set.add(TXPacket.PacketType.TX_CLIENT_RESPONSE);
        return set;
    }

    /**
     * This simple client creates a bunch of names and sends a bunch of requests
     * to each of them. Refer to the parent class
     * {@link ReconfigurableAppClientAsync} for other utility methods available
     * to this method or to know how to write your own client.
     *w
     * @param args
     * @throws IOException
     */

    public static void main(String args[]) throws  IOException{
        createSomething();
//        client.testGetRequest();
        client.testBasicCommit();
//          client.testMultiLineTxn();
//          client.testAborting();
    }


}
