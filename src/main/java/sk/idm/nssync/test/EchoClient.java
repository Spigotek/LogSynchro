package sk.idm.nssync.test;

import java.io.*;
import java.net.*;
import java.util.concurrent.Callable;

public class EchoClient implements Callable<Object> {

    String hostName = "localhost";
    int portNumber = 9876;

    @Override
    public Object call() throws Exception {
        try {
            Socket echoSocket = new Socket(hostName, portNumber);
            PrintWriter out = new PrintWriter(echoSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(echoSocket.getInputStream()));
            Thread.sleep(2000);
            System.out.println("one");
            out.println("one");
            Thread.sleep(2000);
            out.println("two");
            System.out.println("two");
            Thread.sleep(2000);
            System.out.println("three");
            out.println("three");
            out.println("exit");
            String sss;
            while ((sss = in.readLine()) != null) {
                System.out.println("echo: " + sss);
            }
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host " + hostName);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to " + hostName);
            System.exit(1);
        } finally {
            System.out.println("quit");
        };
        return null;
    }
}
