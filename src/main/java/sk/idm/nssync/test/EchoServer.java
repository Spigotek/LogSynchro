package sk.idm.nssync.test;

import java.net.*;
import java.io.*;
import java.util.concurrent.Callable;

public class EchoServer implements Callable<Object> {
    public int portNumber = 9876;

    @Override
    public Object call() throws Exception {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(portNumber);
            Socket clientSocket = serverSocket.accept();
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            String inputLine;
            while (!(inputLine = in.readLine()).equals("exit")) {
                System.out.println("server read: " + inputLine);
                out.println(inputLine);
                if (inputLine.equals("exit")) {
                    break;
                }
            }
            System.out.println("end");
        } catch (IOException e) {
            System.out.println("Exception caught when trying to listen on port " + portNumber + " or listening for a connection");
            System.out.println(e.getMessage());
        } finally {
            System.out.println("server quit");
            serverSocket.close();
        }
        return null;
    }
}
