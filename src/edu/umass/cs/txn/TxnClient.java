package edu.umass.cs.txn;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Set;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.NoopAppClient;
import edu.umass.cs.txn.interfaces.TxOp;
import edu.umass.cs.txn.txpackets.TXInitRequest;
import edu.umass.cs.txn.txpackets.TxClientRequest;
import edu.umass.cs.txn.txpackets.TxOpRequest;
import org.json.JSONException;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParserBytes;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import redis.clients.jedis.Client;

/**
 * @author arun
 *
 */
public class TxnClient extends ReconfigurableAppClientAsync<Request> implements AppRequestParserBytes{

    private int numResponses = 0;

    static TxnClient client;

    /**
     * @throws IOException
     */
    public TxnClient () throws IOException {
        super();
    }

    private synchronized void incrNumResponses() {
        this.numResponses++;
    }

    private static final long INTER_REQUEST_TIME = 50;

    private void testSendBunchOfRequests(String name, int numRequests)
            throws IOException {
        System.out.println("Created " + name
                + " and beginning to send test requests");
        new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < numRequests; i++) {
                    long reqInitime = System.currentTimeMillis();
                    try {
                        InetSocketAddress entryServer=new InetSocketAddress("127.0.0.1",2100);
                        ArrayList<ClientRequest> requests=new ArrayList<>();
                        requests.add(new AppRequest("name0",
                                "request_value" + i,
                                AppRequest.PacketType.DEFAULT_APP_REQUEST,
                                false));
                        requests.add(new AppRequest("name1",
                                "request_value" + i+1,
                                AppRequest.PacketType.DEFAULT_APP_REQUEST,
                                false));
                        TxClientRequest txClientRequest=new TxClientRequest(requests);

                        sendRequest(txClientRequest,entryServer, new RequestCallback() {
                            @Override
                            public void handleResponse(Request response) {
                                System.out.print("Delivered");
                            }

                        });
                    }catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    @Override
    public Request getRequest(String stringified) {
        try {
            return NoopApp.staticGetRequest(stringified);
        } catch (RequestParseException | JSONException e) {
            // do nothing by design
        }
        return null;
    }

    @Override
    public Request getRequest(byte[] message, NIOHeader header)
            throws RequestParseException {
        return NoopApp.staticGetRequest(message, header);
    }


    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return NoopApp.staticGetRequestTypes();
    }

    /**
     * This simple client creates a bunch of names and sends a bunch of requests
     * to each of them. Refer to the parent class
     * {@link ReconfigurableAppClientAsync} for other utility methods available
     * to this method or to know how to write your own client.
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        client = new TxnClient ();
        final int numNames = 1;
        final int numReqs = 1;
        String namePrefix = "Service_name_txn";
        String initialState = "some_default_initial_state";
        try {
            client.sendRequest(new CreateServiceName("name0", "value0"));
            client.sendRequest(new CreateServiceName("name1","value1"));
        }catch(Exception ex){
            System.out.println("Unable to create");
        }
        for (int i = 0; i < numNames; i++) {
            final String name = namePrefix;
//            client.testSendBunchOfRequests(name,1);
//                    + ((int) (Math.random() * Integer.MAX_VALUE));
//            System.out.println("Creating name " + name);
            client.sendRequest(new CreateServiceName(name, initialState),
                    new RequestCallback() {

                        @Override
                        public void handleResponse(Request response) {
                            try {
                                    client.testSendBunchOfRequests(name,
                                            numReqs);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    });
        }
        synchronized (client) {
            long maxWaitTime = numReqs * INTER_REQUEST_TIME + 4000, waitInitTime = System
                    .currentTimeMillis();
            while (client.numResponses < numNames * numReqs
                    && (System.currentTimeMillis() - waitInitTime < maxWaitTime))
                try {
                    client.wait(maxWaitTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
        System.out.println("\n" + client + " successfully created " + numNames
                + " names, sent " + numNames * numReqs
                + " requests, and received " + client.numResponses
                + " responses.");
        client.close();
    }

}
