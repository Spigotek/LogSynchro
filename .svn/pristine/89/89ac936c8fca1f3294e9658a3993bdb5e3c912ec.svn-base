/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sk.idm.nssync.elastic;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import sk.idm.nssync.Config;
import sk.idm.nssync.NSSyncException;

/**
 *
 * @author mhajducek
 */
public class ConnectionFactory {

    private static final Logger log = LogManager.getLogger(ConnectionFactory.class);
    private static Client client = null;

    private String esurl01;
    private String esurl02;
    private String esurl03;
    private String clusterName;

    public ConnectionFactory(Config conf) {
        esurl01 = conf.esurl01;
        esurl02 = conf.esurl02;
        esurl03 = conf.esurl03;
        clusterName = conf.clustername;
    }

    public ConnectionFactory(String... nodes) throws NSSyncException {
        if (nodes.length < 2) {
            throw new NSSyncException("Error creating elasticsearch connection");
        } else {
            clusterName = nodes[0];
            esurl01 = nodes[1];
            if (nodes.length > 1) {
                esurl02 = nodes[2];
            }
            if (nodes.length > 2) {
                esurl03 = nodes[3];
            } else {
                log.warn("Too much nodes defined, ignoring");
            }
        }
    }


}
