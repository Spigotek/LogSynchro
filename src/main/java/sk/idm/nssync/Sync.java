/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sk.idm.nssync;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import sk.idm.nssync.model.ReadInfo;
import sk.idm.nssync.model.SyncInfo;
import sk.idm.nssync.model.UpdateInfo;

/**
 *
 * @author mhajducek
 */
public class Sync implements Runnable {

    protected static Client client = null;

    private Config conf;

    StringBuffer startDebugMsg = new StringBuffer("\nSTART:\n");

    private static final Logger log = LogManager.getLogger(Sync.class);

    public static void main(String[] args) throws IOException, NSSyncException {
        String configFile = args[0];
        Sync s = new Sync();
        s.conf = new Config(configFile);

        client = s.getClient();

        for (Map.Entry<String, SyncInfo> e : s.conf.tables.entrySet()) {
            s.createIndex(e.getValue().index, s.conf.clustername, s.conf.esurl01, s.conf.esurl02, s.conf.esurl03);
        }
        s.run();
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        Connection conn = null;
        PreparedStatement pstmt = null;
        ExecutorService mainExecutor = Executors.newFixedThreadPool(1 + conf.updatethreads + conf.readthreads);
        try {
            conn = conf.getDbPool().getConnection();

            try {
                boolean checkActiveStatus = checkActiveStatus(conn);
                if (!checkActiveStatus) {
                    log.info("Sync process on " + conf.localhost + " not active quitting.");
                    return;
                }
            } catch (SQLException ex) {
                log.error("DB Error by initializing", ex);
                throw ex;
            } catch (NSSyncException ex) {
                log.error("Sync Error by initializing", ex);
                throw ex;
            }

            debugNumberOfRows(conn, "start");

            //fronta, v ktorej sa cita z result setu a zapisuje do Elasticsearch
            BlockingQueue<ReadInfo> readQueue = new ArrayBlockingQueue<>(conf.bulkwritesize * conf.writerthreads);
            //fronta, z ktorej sa vyberaju ID, ktoré su už v Elasticsearch a ide sa robiť update späť do SQL
            BlockingQueue<UpdateInfo> updateQueue = new ArrayBlockingQueue<>(conf.updatequeue);
            Set<Callable<String>> futures = new HashSet<>();
            futures.add(new Reader(readQueue, conf));
            int i;
            for (i = 0; i < conf.writerthreads; i++) {
                futures.add(new Writer(readQueue, updateQueue, conf, i));
            }
            log.debug("Created " + i + " writers");
            for (i = 0; i < conf.updatethreads; i++) {
                futures.add(new Updater(updateQueue, conf, i));
            }
            log.debug("Created " + i + " updaters");
            log.debug("Invoking all threads");
            mainExecutor.invokeAll(futures);
            log.debug("Shutdown");
            mainExecutor.shutdown();
            log.debug("Await termination");
            mainExecutor.awaitTermination(5, TimeUnit.MINUTES);
            log.debug("Terminated");

            try {
                //1. truncate neakt. tabuliek ked su vsetky riadky prenesene
                //2. zmena aktivnej tabulky ak v akt. tabulke je viac riadkov ako max
                if (conn.isClosed()) {
                    throw new SQLException("Connection is closed");
                }

                for (String t : conf.tables.keySet()) {
                    String owner;
                    String activePartition;
                    try (Statement truncateStmt = conn.createStatement()) {
                        //select všetky partition
                        owner = t.substring(0, t.indexOf('.'));
                        //String sql = String.format("select * from %s.nssync_config where table_name = ?", owner);
                        String sql = "select * from dbo.nssync_config where table_name = ?";
                        log.debug(sql);
                        pstmt = conn.prepareStatement(sql);
                        pstmt.setString(1, t);
                        try (ResultSet rs = pstmt.executeQuery()) {
                            activePartition = null;
                            while (rs.next()) {
                                String table = rs.getString("table_name") + rs.getString("active_partition");
                                boolean active = rs.getBoolean("active");

                                if (active) {
                                    //ak je active, zapamatame si nazov aby sme na konci testovali na max rows
                                    activePartition = table;
                                } else {
                                    //ak nie je active a vsetky riadky su export = 1 -> truncate
                                    try (Statement countNotExportedStmt = conn.createStatement()) {
                                        ResultSet rsCount = countNotExportedStmt.executeQuery("select count(*) as count from " + table + " where export is null");
                                        rsCount.next();
                                        long count = rsCount.getLong(1);
                                        rsCount = countNotExportedStmt.executeQuery("select count(*) as count from " + table);
                                        rsCount.next();
                                        long countAll = rsCount.getLong(1);
                                        if (count == 0 && countAll > 0) {
                                            try {
                                                exportTableToZippedCSV(conn, table);
                                                String truncateSql = "truncate table " + table;
                                                log.debug(truncateSql);
                                                truncateStmt.execute(truncateSql);
                                            } catch (IOException e) {
                                                log.error("Error backing up table " + table, e);
                                            }
                                        }
                                    }
                                }
                            }
                        } catch (SQLException ex) {
                            log.error("Error truncating table");
                            throw ex;
                        }
                    } catch (SQLException ex) {
                        log.error("Error truncating table");
                        throw ex;
                    }

                    String sql = String.format("select count(*) from %s", activePartition);
                    try (Statement stmt = conn.createStatement();
                            ResultSet rs = stmt.executeQuery(sql)) {
                        rs.next();
                        long count = rs.getLong(1);
                        if (count > conf.maxRows) {
                            //negacia active bitu -> funguje iba ak su tabulky 2
                            stmt.executeUpdate(String.format("update %s.nssync_config set active = ~active where table_name = '%s'", owner, t));
                        }
                    } catch (SQLException ex) {
                        log.warn("Error changing active partition");
                        throw ex;
                    }
                }
                debugNumberOfRows(conn, "end");
            } catch (SQLException ex) {
                log.error("Error by synchronizing", ex);
                throw ex;
            }
        } catch (InterruptedException ex) {
            log.warn("Task interrupted");
        } catch (SQLException | NSSyncException ex) {
            log.error("Exiting synchronization");
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (Exception ex) {
                log.error("Error cleaning up", ex);
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception ex) {
                log.error("Error cleaning up", ex);
            }
            if (!mainExecutor.isTerminated()) {
                log.error("Timeout canceling non-finished tasks");
            }
            mainExecutor.shutdownNow();
            log.info("shutdown finished");
        }
        log.info("Run time: " + ((System.currentTimeMillis() - startTime) / (1000 * 60)) + " min.");
    }

    private static String joinArrayToString(String[] array, char separator) {
        StringBuilder result = new StringBuilder();
        for (String string : array) {
            result.append(string);
            result.append(separator);
        }
        return (result.length() > 0 ? result.substring(0, result.length() - 1) : "") + '\n';

    }

    private Client getClient() throws MalformedURLException, UnknownHostException {
        URL url1, url2, url3;

        Settings settings = Settings.settingsBuilder().put("cluster.name", conf.clustername).build();
        if (client == null) {
            url1 = new URL(conf.esurl01);
            client = TransportClient.builder().settings(settings).build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(url1.getHost()), url1.getPort()));
            log.debug("Added address: " + url1);

            if (conf.esurl02 != null && !conf.esurl02.isEmpty()) {
                url2 = new URL(conf.esurl02);
                client = TransportClient.builder().settings(settings).build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(url2.getHost()), url2.getPort()));
                log.debug("Added address: " + url2);
            }
            if (conf.esurl03 != null && !conf.esurl03.isEmpty()) {
                url3 = new URL(conf.esurl03);
                client = TransportClient.builder().settings(settings).build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(url3.getHost()), url3.getPort()));
                log.debug("Added address: " + url3);
            }
        }
        return client;
    }

    private void createIndex(String indexName, String... nodes) throws NSSyncException {
        IndicesExistsResponse existsResponse = client.admin().indices().prepareExists(indexName).execute().actionGet();
        if (!existsResponse.isExists()) {
            log.debug("Index {} does not exist.", indexName);
            client.admin().indices().prepareCreate(indexName).execute().actionGet();
            log.debug("Index {} created.", indexName);
        }
    }

    private void debugNumberOfRows(Connection conn, String start) {
        if (!conf.localhost.equals("localhost")) return;
        
        StringBuffer msg = new StringBuffer();
        if (log.isDebugEnabled()) {
            if (!start.equals("start")) {
                log.debug(startDebugMsg);
            }
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("select * from dbo.nssync_config")) {
                while (rs.next()) {
                    msg.append("NUM: ").append(rs.getString("table_name")).append(" ").append(rs.getString("active_partition")).append(" ").append(rs.getString("active")).append('\n');
                }
                if (start.equals("start")) {
                    startDebugMsg.append(msg);
                } else {
                    msg.insert(0, "\nEND:\n");
                }
                StringBuilder endMsg = new StringBuilder(msg);
                try (ResultSet rs2 = stmt.executeQuery("select '02', export, count(*) from dbo.ntevl_mssql02 group by export\n"
                        + "union all\n"
                        + "select '03', export, count(*) from dbo.ntevl_mssql03 group by export;\n"
                        + "")) {
                    msg = new StringBuffer();
                    while (rs2.next()) {
                        msg.append("NUM: ").append(rs2.getString(1)).append(" ").append(rs2.getString(2)).append(" ").append(rs2.getString(3)).append('\n');
                    }
                    if (start.equals("start")) {
                        startDebugMsg.append(msg);
                    } else {
                        log.debug(endMsg.toString() + msg.toString());
                    }
                }
            } catch (SQLException ex) {
                log.warn("Error debug logging", ex);
            }
        }
    }

    private boolean checkActiveStatus(Connection conn) throws NSSyncException, SQLException, InterruptedException {
        boolean ret = false;
        String errMsg = null;

        try {
            conn.setAutoCommit(false);
            //nacitanie aktivneho nodu

            Date activeStart = null;
            String activeHost = null;
            int i = 0;
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("select host, active, start_run, end_run from dbo.nssync_nodes where active = 1")) {
                while (rs.next()) {
                    Timestamp timestamp = rs.getTimestamp("start_run");
                    if (timestamp != null) {
                        activeStart = new java.util.Date(timestamp.getTime());
                    }
                    activeHost = rs.getString("host").trim();
                    i++;
                }
            } catch (SQLException ex) {
                errMsg = "Error getting active node info";
                throw ex;
            }

            long timeFromStart = Long.MAX_VALUE;
            if (activeStart != null) {
                timeFromStart = ((new Date()).getTime() - activeStart.getTime()) / (60 * 1000);
            }
            log.debug("Active node: " + activeHost + " last run: " + activeStart + " (before " + timeFromStart + " min.)");
            if (i > 1) {
                //viac ako jeden nod je nastavený ako aktívny - chyba, koniec
                throw new NSSyncException("Multiple nodes are set as active!");
            } else if (i == 0) {
                log.info("No active node set. Taking activity.");
                //ziadnu nod nie je nastaveny ako aktivny
                //vezmi na seba rolu aktívneho a pokracuj
                try (PreparedStatement pstmt = conn.prepareStatement("update dbo.nssync_nodes set active = true, start_run = getdate()  where host = ?")) {
                    pstmt.setString(1, conf.localhost);
                    pstmt.execute();
                }

                try (PreparedStatement pstmt = conn.prepareStatement("update dbo.nssync_nodes set active = 0 where host != ?")) {
                    pstmt.setString(1, conf.localhost);
                    pstmt.execute();
                }
                conn.commit();
                ret = true;
            } else {
                //práve jeden aktívny nod
                if (conf.localhost.equals(activeHost)) {
                    log.info("Node is active, running...");
                    //toto je active host - zapis cas startu a pokracuj
                    try (PreparedStatement pstmt = conn.prepareStatement("update dbo.nssync_nodes set start_run = getdate()  where host = ?")) {
                        pstmt.setString(1, conf.localhost);
                        pstmt.execute();
                    }
                    conn.commit();
                    ret = true;
                } else {
                    //activehost je iný host
                    log.info("Not active.");
                    if (activeStart == null) {
                        log.info("Active node has never run. Taking activity.");
                        //este nikdy nebezal
                        //prevezmi rolu aktivneho, zapis cas startu a pokracuj
                        try (PreparedStatement pstmt = conn.prepareStatement("update dbo.nssync_nodes set active = 1, start_run = getdate()  where host = ?")) {
                            pstmt.setString(1, conf.localhost);
                            pstmt.execute();
                        }

                        try (PreparedStatement pstmt = conn.prepareStatement("update dbo.nssync_nodes set active = 0 where host != ?")) {
                            pstmt.setString(1, conf.localhost);
                            pstmt.execute();
                        }
                        conn.commit();
                        ret = true;
                    } else {
                        //cas v minutach od zaciatku behu
                        if (timeFromStart > conf.maxRunTimeMin) {
                            //vezmi na seba rolu aktívneho
                            log.info("Active node run is taking more than " + conf.maxRunTimeMin + " mins. Taking activity.");
                            try (PreparedStatement pstmt = conn.prepareStatement("update dbo.nssync_nodes set active = 1, start_run = getdate()  where host = ?")) {
                                pstmt.setString(1, conf.localhost);
                                pstmt.execute();
                            }

                            try (PreparedStatement pstmt = conn.prepareStatement("update dbo.nssync_nodes set active = 0 where host != ?")) {
                                pstmt.setString(1, conf.localhost);
                                pstmt.execute();
                            }
                            conn.commit();
                            ret = true;
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            throw ex;
        } finally {
            conn.setAutoCommit(true);
        }
        return ret;
    }

    private void exportTableToZippedCSV(Connection conn, String table) throws SQLException, FileNotFoundException, IOException {
        String sql = String.format("select * from %s", table);
        DateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
        table = table.replace('.', '_');
        String fileName = table + sdf.format(new Date());

        ZipEntry catalogEntry = new ZipEntry(fileName + ".txt");

        Statement stmt = null;
        ResultSet rs = null;
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(conf.backupPath + "/" + fileName + ".zip");
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            ResultSetMetaData rsmd = rs.getMetaData();
            int colCnt = rsmd.getColumnCount();
            String[] values = new String[colCnt];

            for (int i = 0; i < colCnt; i++) {
                values[i] = rsmd.getColumnName(i + 1);
            }

            try (ZipOutputStream zip = new ZipOutputStream(new BufferedOutputStream(fos))) {
                zip.putNextEntry(catalogEntry);
                zip.write((joinArrayToString(values, ';')).getBytes("UTF-8"));
                values = new String[colCnt];
                while (rs.next()) {
                    for (int i = 0; i < colCnt; i++) {
                        String value = rs.getString(i + 1);
                        values[i] = value;
                    }
                    zip.write(joinArrayToString(values, ';').getBytes("UTF-8"));
                }
                zip.flush();
            }
            log.info("Backup " + fileName + " zip file created");
        } catch (SQLException ex) {

        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    log.warn(ex);
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    log.warn(ex);
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ex) {
                    log.warn("Error cleaning up", ex);
                }
            }
        }
    }
}
