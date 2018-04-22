package edu.umass.cs.txn.testing;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.*;
import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.txn.exceptions.ResponseCode;
import edu.umass.cs.txn.testing.app.CalculatorTX;
import edu.umass.cs.txn.testing.app.OperateRequest;
import edu.umass.cs.txn.testing.app.ResultRequest;
import edu.umass.cs.txn.txpackets.TXPacket;
import edu.umass.cs.txn.txpackets.TxClientRequest;
import edu.umass.cs.txn.txpackets.TxClientResult;
import edu.umass.cs.utils.Util;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
/**
 * @author Sandeep
 *
 */
public class Simulator extends ReconfigurableAppClientAsync<Request> {

    /*Building the Simulator
    0. Test every kind of Tx Response Code
    1. Locally run bursts of requests - No Failure, No Contention, Try to pass as many as possible Measure throughput
    Run on Cluster -> Kind of a throughput experiment. No Failures.
    Objective is to have a reasonable transaction throughput
    2. Do the same with some contention and check if quorum details are being recieved
    3. Start Failining machines and do the same



    * */
    static int load =30;

    static int noBursts = 5;
//  sets of transactions sent

    static int maxGroups = 100;
//max groups created
    static int recieved = 0;
//to wait till groups are complete

    static int txSize = 3;
//  TxSize is the size of the load

    int failure =0;
// Total failure status

    static int stabilizeTime = 60;
//    Wait 2 minute after starting everythin

    static TxnClient client;

    static boolean created = false;

    static HashMap<ResponseCode,Integer> results = new HashMap<>();

   static  String cmdAppend1 = "./bin/gpServer.sh";
   static String cmdAppend2= "-DgigapaxosConfig=src/edu/umass/cs/txn/testing/gigapaxos.properties ";

    HashMap<String,Boolean> isAlive;
    static  void createSomething(){
        if(!created) {
            try {
                client = new TxnClient();
                Set<InetSocketAddress> quorum = new HashSet<>();
                for (String key : actives.keySet()) {
                    quorum.add(actives.get(key));
                }
                Object something = new Object();
                client.sendRequest(new CreateServiceName("Service_name_txn", "0", quorum));
                for (int i = 1; i <= maxGroups; i++) {
                    client.sendRequest(new CreateServiceName("name" + i, Integer.toString(i)), new RequestCallback() {
                        @Override
                        public void handleResponse(Request response) {
                            synchronized(something) {
                                recieved++;
                                if (recieved == maxGroups) notify();
                            }
                        }
                    });
                }
                synchronized(something){
                    something.wait(60000);
                }
                System.out.println("created a total of "+recieved+"groups");

            }catch(Exception e){
                e.printStackTrace();
                throw new RuntimeException("Unable to create");
            }
        }
        created = true;
    }


    TxClientRequest getRandomlyCreatedTxRequest(){
        Random random = new Random();
        ArrayList<ClientRequest> clientRequests=new ArrayList<>();
        Set<Integer> prev = new HashSet<>();
        int t;
        for(int i=0;i<txSize;i++){
            t = random.nextInt(maxGroups)+1;
            while(prev.contains(t)){
                t = random.nextInt(maxGroups)+1;
            }
            String name = "name"+t;
            clientRequests.add(new OperateRequest(name,10, OperateRequest.Operation.add));
        }
        TxClientRequest txClientRequest = new TxClientRequest(clientRequests);
        return txClientRequest;
    }

    void checkQuorumStatus(Set<String> actives){
        synchronized (isAlive){
            failure++;

//            int i=0;
//            for(String active:actives){
//                if(isAlive.get(active).booleanValue())i++;
//            }
//            if(i<=isAlive.size()/2)failure++;
        }
    }
    synchronized static void updateRPE(ResponseCode rpe){

        if(!results.containsKey(rpe)){
            results.put(rpe,1);
        }else{
            results.put(rpe,results.get(rpe).intValue()+1);
        }
    }

    public float startload(int load){
        createSomething();
        RateLimiter rateLimiter = new RateLimiter(load);
        System.out.println("Start load");
        Timer timer = new Timer();
        for(int i=0;i<=load*noBursts;i++){
            rateLimiter.record();
            TxClientRequest txClientRequest = getRandomlyCreatedTxRequest();
//            System.out.println("Request ID"+txClientRequest.getRequestID());
            try {
                sendRequestAnycast(txClientRequest,new TimeoutRequestCallback() {
                    @Override
                    public long getTimeout() {
                        return 1000000;
                    }

                    @Override
                    public void handleResponse(Request response) {
                        if (response instanceof TxClientResult) {
                            TxClientResult t = (TxClientResult) response;
//                          if(t.success)checkQuorumStatus(t.getActivesPrevious());
                            updateRPE(t.getRpe());
//                            if(t.getRpe()!= ResponseCode.COMMITTED)checkQuorumStatus(t.getActivesPrevious());
                        }
                    }
                });
            }catch (Exception ex){
                System.out.println("Recieved an exception");
            }

        }
        System.out.print("Begin Wait");
        try{
            TimeUnit.SECONDS.sleep(60);
        }catch(Exception ex){

        }
        System.out.println("The end");
        return (failure/(load*noBursts)*100);
    }

    /**
     * @throws IOException
     */

    public Simulator(HashMap<String,Boolean> isAlive) throws IOException {
        super();
        this.isAlive =isAlive;
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
    static Random rand = new Random();

    static long getRandom(int exp){
        return Math.round(Math.log(1- rand.nextDouble())*(exp)*-1);
    }


    static class SimulateFailure extends Thread{

        final String activeID;

        final int ettf;

        HashMap<String,Boolean> isAlive;

        void runCommand(String[] cmd){
        try {
            Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = input.readLine()) != null) {
                System.out.println(line);
            }
            input.close();
        } catch(IOException ex){
            ex.printStackTrace();
            System.out.println(activeID + "Could not execute");
        }
        }

        String[] getStartCommand(){

            return new String[]{cmdAppend1,cmdAppend2,"start",activeID};

        }

        String[] getStopCommand(){
            return new String[]{cmdAppend1,cmdAppend2,"stop",activeID};
        }

        SimulateFailure(String activeID,int ettf,HashMap<String,Boolean> isAlive ) {
            this.activeID = activeID;
            this.ettf = ettf;
            this.isAlive = isAlive;
//            runCommand(new String[]{"ls","bin/gpServer.sh"});
            runCommand(getStopCommand());

        }

        public void run() {
            try {
                synchronized (isAlive){
                    isAlive.put(activeID,new Boolean(true));
                }
               runCommand(getStartCommand());
               Thread.sleep(stabilizeTime*1000);
//                while(true) {
//                    long fail = getRandom(ettf);
//                    Thread.sleep(fail*1000);
//                    synchronized (isAlive){
//                        isAlive.put(activeID,new Boolean(false));
//                    }
//                    runCommand(getStopCommand());
//                    System.out.println("System " + activeID + " Killed");
//                    Thread.sleep(10000);// MTTR
//                    runCommand(getStartCommand());
//                    synchronized (isAlive){
//                        isAlive.put(activeID,new Boolean(true));
//                    }
//                }
            }catch(InterruptedException ex){
                System.out.println("Interrupted");
            }
        }
    }

    protected static String DEFAULT_RECONFIGURATOR_PREFIX = "active.";

    static Map<String, InetSocketAddress> actives = new HashMap<String, InetSocketAddress>();


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

    public static void main(String args[]) throws  IOException,InterruptedException{
        createSomething();
        BufferedWriter writer = new BufferedWriter(new FileWriter("results"));
        ArrayList<SimulateFailure> threads= new ArrayList<>();
        int ettfs[] = {100};
        HashMap<String,Boolean> isAlive = new HashMap<>();
        float failurePercentage =new Simulator(isAlive).startload(load);
        System.out.println("Failure %"+failurePercentage);
//        for(int ettf:ettfs){
//            while(!threads.isEmpty()){
//                SimulateFailure sm = threads.get(0);
//                sm.interrupt();
//            }
//            for(String key:actives.keySet()){
//                new SimulateFailure(key,ettf,isAlive).start();
//            }
//
//            TimeUnit.SECONDS.sleep(120);
//            float failurePercentage =new Simulator(isAlive).startload(10);
            System.out.println(results.size()+" : Collected results");
            for(ResponseCode r:results.keySet()){
                System.out.println(r+"   :   "+results.get(r));
            }

//        }
//        writer.close();
    }


}
