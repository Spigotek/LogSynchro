/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sk.idm.nssync;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sk.idm.nssync.model.ReadInfo;

/**
 *
 * @author mhajducek
 */
class Reader implements Callable {

    private final BlockingQueue<ReadInfo> readQueue;
    private static final Logger log = LogManager.getLogger(Reader.class);
    private final Config conf;

    public Reader(BlockingQueue<ReadInfo> readQueue, Config conf) {
        this.readQueue = readQueue;
        this.conf = conf;
    }

    @Override
    public String call() throws Exception {
        String ret = "error";
        log.debug("Started");
        String select = "";
        String timestampcol, datetimeformat;
        boolean idFromHash;
        List<String> hashcols = new ArrayList<>();
        ResultSet rs = null;
        Statement stmt = null;
        Connection conn = null;
        Map<String, Object> values;
        Statement partSelect = null;

        try {
            //slucka cez vsetky tabulky, ktore sa maju prenasat do Elasticsearch
            for (String key : conf.tables.keySet()) {
                select = conf.tables.get(key).select;
                timestampcol = conf.tables.get(key).timestampcol;
                datetimeformat = conf.tables.get(key).datetimeformat;
                idFromHash = Boolean.parseBoolean(conf.tables.get(key).idFromHash);
                hashcols = Arrays.asList(conf.tables.get(key).hashcols.split(","));

                String table = key;
                if (table.endsWith(";")) {
                    table = table.substring(0, table.length() - 1);
                }

                String index = conf.tables.get(key).index;
                String mapping = conf.tables.get(key).maping;
                conn = conf.getDbPool().getConnection();
                if (conn.isClosed()) {
                    throw new SQLException("Connection is closed");
                }

                String partRsSelect = String.format("select * from %s.nssync_config where table_name = '%s'", table.substring(0, table.indexOf('.')), table);
                partSelect = conn.createStatement();
                log.debug("PARTITION SELECT: " + partRsSelect);
                ResultSet partRs = partSelect.executeQuery(partRsSelect);

                List<String> tables = new ArrayList<>();
                String t;
                while (partRs.next()) {
                    t = table + partRs.getString("active_partition");
                    tables.add(t);
                }

                for (String tt : tables) {
                    String colsStr;
                    colsStr = select.substring(conf.COLS_START.length(), select.indexOf(conf.COLS_END));
                    String perfSelect = select.replaceAll("%TABLE%", tt);
                    log.debug("PERFSELECT: " + perfSelect);
                    stmt = conn.createStatement();
                    log.info("Executing query: " + perfSelect);
                    rs = stmt.executeQuery(perfSelect);
                    int i = 0;
                    String[] cols = colsStr.split(",");
                    while (rs.next()) {
                        values = new HashMap<>();
                        if (i % 100 == 0) {
                            log.info("Processing row: " + i);
                        }
                        boolean errFlag = false;
                        StringBuilder oneLineValue = new StringBuilder();
                        for (String col : cols) {
                            String value = rs.getString(col);
                            if (col.equals(timestampcol)) {
                                try {
                                    try {
                                        if (datetimeformat.equals("epoch")) {
                                            Date epDate = new Date(Long.parseLong((value).trim() + "000"));
                                            value = convertDateTime(epDate);
                                        } else {
                                            value = convertDateTime(value, datetimeformat);
                                        }
                                        values.put(col, value.trim());
                                    } catch (ParseException e) {
                                        //este jeden pokus o konverziu
                                        try {
                                            value = convertDateTime(value, conf.fallbackDateTimeFormat);
                                            values.put(col, value.trim());
                                        } catch (ParseException pe) {
                                            log.error("Error converting date: {}", value);
                                            errFlag = true;
                                        }
                                    } catch (NumberFormatException e) {
                                        log.error("Error converting date: {}", value, e);
                                        errFlag = true;
                                    }
                                } catch (Exception ex) {
                                    log.error("Wrong timestamp skipping row");
                                    errFlag = true;
                                }
                            } else {
                                if (value == null) value = "null";
                                values.put(col, value.trim());
                            }
                            if (hashcols.contains(col)) oneLineValue.append(value);
                        }
                        //id vygenerovany v SQL alebo hash hodnoty

                        String id = rs.getString("id");
                        String idFromHashValue = null;
                        if (!errFlag) {
                            if (idFromHash) {
                                idFromHashValue = stringHash(oneLineValue.toString());
                            }
                            readQueue.put(new ReadInfo(id, values, tt, index, mapping, idFromHashValue));
                        }
                        i++;
                    }
                    log.debug("NUM: " + i + " put in readqueue");
                    log.info("Success for: " + perfSelect);
                }
            }
            log.info("Ended");
            ret = "ok";
        } catch (SQLException e) {
            log.error("Fail for: " + select);
            log.error(e);
        } catch (InterruptedException ex) {
            log.error("Fail for: " + select);
            log.error("Interrupted");
        } finally {
            log.debug("Sending exit message to readQueue");
            try {
                readQueue.put(new ReadInfo());
            } catch (InterruptedException ex) {
                log.warn(ex.getMessage());
            }
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException ex) {
                log.warn(ex.getMessage());
            }
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException ex) {
                log.warn(ex.getMessage());
            }
            try {
                if (partSelect != null) {
                    partSelect.close();
                }
            } catch (SQLException ex) {
                log.warn(ex.getMessage());
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {
                log.warn(ex.getMessage());
            }

        }
        return ret;
    }

    private String convertDateTime(String value, String datetimeformat) throws ParseException {
        String ret;
        try {
            DateFormat dfInput = new SimpleDateFormat(datetimeformat);
            Date date = dfInput.parse(value);
            DateFormat dfOutput = new SimpleDateFormat(conf.targetDateTimeFormat);
            ret = dfOutput.format(date);
        } catch (ParseException e) {
            log.debug("Value: {}, format: {}", value, datetimeformat, e);
            throw e;
        }
        return ret;
    }

    private String convertDateTime(Date value) {
        DateFormat dfOutput = new SimpleDateFormat(conf.targetDateTimeFormat);
        return dfOutput.format(value);
    }

    private static String stringHash(String value) {
        long hash = 7;
        for (int i = 0; i < value.length(); i++) {
            hash = hash * 31 + value.charAt(i);
        }
        String ret = String.valueOf(hash);
        if (ret.indexOf('-') == 0) {
            return ret.substring(1);
        } else {
            return ret;
        }
    }
}
