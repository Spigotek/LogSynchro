/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sk.idm.nssync.test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author mhajducek
 */
public class TestSocket {

    public static void main(String[] args) throws InterruptedException  {
        ExecutorService s = Executors.newFixedThreadPool(2);
        Set<Callable<Object>> futures = new HashSet<>();
        futures.add(new EchoServer());
        //futures.add(new EchoClient());

        System.out.println("1");
        s.invokeAll(futures, 1, TimeUnit.MINUTES);
        
        System.out.println("2");
        s.shutdown();
        System.out.println("3");
    }
}
