package edu.umass.cs.txn.testing;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.reconfiguration.testing.TESTReconfigurationMain;
import edu.umass.cs.utils.DefaultTest;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.io.IOException;

public class SerializabilityTest extends DefaultTest {


    @Test
    public void testSomething() throws IOException,InterruptedException{
//        FIXME: build a test framework and think of some tests
        try {
              TESTReconfigurationMain.startLocalServers();
        }catch (Exception ex){

        }
        System.out.println("Hahaha Runnign Something");

    }
    public static void main(String[] args) throws IOException,
            InterruptedException {

        Result result = JUnitCore.runClasses(SerializabilityTest.class);
        for (Failure failure : result.getFailures()) {
            failure.getException().printStackTrace();
        }
    }
}
