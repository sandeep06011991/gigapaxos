package edu.umass.cs.txn.testing;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.*;
import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.txn.testing.app.CalculatorTX;
import edu.umass.cs.txn.testing.app.GetRequest;
import edu.umass.cs.txn.testing.app.OperateRequest;
import edu.umass.cs.txn.testing.app.ResultRequest;
import edu.umass.cs.txn.txpackets.TXPacket;
import edu.umass.cs.txn.txpackets.TxClientRequest;
import edu.umass.cs.txn.txpackets.TxClientResult;
import edu.umass.cs.utils.Util;
import org.json.JSONException;
import org.json.JSONObject;

import javax.sound.midi.SysexMessage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
/**
 * @author Sandeep
 *
 */
public class Simulator extends ReconfigurableAppClientAsync<Request> {

    static int noBursts = 100;

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

    int failure =0;

    TxClientRequest getRandomlyCreatedTxRequest(){
        return null;
    }

    void checkQuorumStatus(Set<String> actives){
        synchronized (isAlive){
            int i=0;
            for(String active:actives){
                if(isAlive.get(active).booleanValue())i++;
            }
            if(i>isAlive.size()/2)failure++;
        }
    }

    public void startload(int load){
        RateLimiter rateLimiter = new RateLimiter(load);
        for(int i=0;i<=load*noBursts;i++){
            rateLimiter.record();
            TxClientRequest txClientRequest = getRandomlyCreatedTxRequest();
            try {
                sendRequestAnycast(txClientRequest, new RequestCallback() {

                    @Override
                    public void handleResponse(Request response) {
                        if (response instanceof TxClientResult) {
                            try {

                                TxClientResult t = (TxClientResult) response;
                                System.out.println(t.getActivesPrevious().toString());
                                System.out.println("Transaction status " + ((TxClientResult) response).success);
//                        testGetRequest(10);
                                System.out.println("Transaction test complete");
                            } catch (Exception e) {
                                throw new RuntimeException("Transaction failed");
                            }
                        }
                    }
                });
            }catch (Exception ex){
                System.out.println("Recieved an exception");
            }

        }


    }

    void testGetRequest(int finalValue) throws IOException {
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

        sendRequest(txClientRequest1, entryServer, new TimeoutRequestCallback() {
            @Override
            public long getTimeout() {
                return 600*1000;
            }


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

                        TxClientResult t = (TxClientResult)response;
                        System.out.println(t.getActivesPrevious().toString());
                        System.out.println("Transaction status "+((TxClientResult) response).success);
//                        testGetRequest(10);
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
    public Simulator() throws IOException {
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
                System.out.println(stringified);
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

    public void testForQuorum(){

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
    static Random rand = new Random();

    static long getRandom(int exp){
        return Math.round(Math.log(1- rand.nextDouble())*(exp)*-1);
    }


    static class SimulateFailure extends Thread{

        final String activeID;

        final int ettf;

        void runCommand(String cmd){
        try {
            Process p = Runtime.getRuntime().exec(new String[]{cmd});
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = input.readLine()) != null) {
                System.out.println(line);
            }
            input.close();
        } catch(IOException ex){
            System.out.println(activeID + "Could not execute");
        }
        }

        SimulateFailure(String activeID,int ettf ){
         this.activeID = activeID;
         this.ettf = ettf;
        }

        public void run() {
            try {
                while(true) {
                    long fail = getRandom(ettf);
                    System.out.println("System" + activeID + " started with ettf=" + fail);
                    Thread.sleep(fail);
                    System.out.println("System " + activeID + " Killed");
                    Thread.sleep(10000);
                }
            }catch(InterruptedException ex){
                System.out.println("Interrupted");
            }
        }
    }

    protected static String DEFAULT_RECONFIGURATOR_PREFIX = "active.";

    static Map<String, InetSocketAddress> actives = new HashMap<String, InetSocketAddress>();

    static Map<String,Boolean> isAlive = new HashMap<>();

    static{
        Properties config = PaxosConfig.getAsProperties();

        Set<String> keys = config.stringPropertyNames();
        for (String key : keys) {
            if (key.trim().startsWith(DEFAULT_RECONFIGURATOR_PREFIX)) {
                actives .put(key.replaceFirst(DEFAULT_RECONFIGURATOR_PREFIX, ""),
                        Util.getInetSocketAddressFromString(config
                                .getProperty(key)));
            }
        }

    }

    public static void main(String args[]) throws  IOException{
        createSomething();
        int ettf = 40000;
        for(String key:actives.keySet()){
            new SimulateFailure(key,ettf).start();
        }

//        client.testGetRequest(10);
//        client = new TxnClient();
        client.testBasicCommit();
//          client.testMultiLineTxn();
//          client.testAborting();
    }


}
