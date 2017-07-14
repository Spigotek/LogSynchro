/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sk.idm.nssync;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import sk.idm.nssync.model.UpdateInfo;

/**
 *
 * @author mhajducek
 */
class Updater implements Callable {

    private final Config conf;
    private final BlockingQueue<UpdateInfo> updateQueue;
    private final int number;
    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(Updater.class);

    public Updater(BlockingQueue<UpdateInfo> updateQueue, Config conf, int number) {
        this.updateQueue = updateQueue;
        this.conf = conf;
        this.number = number;
    }

    @Override
    public String call() {
        List<String> idsUpd = null;
        String ret = "error";
        String updateSql = null;
        log.debug("Started (" + number + ")");
        String uniqueID = "";
        Connection conn = null;
        //TODO dynamicky menit velkost
        Map<String, PreparedStatement> stmts = new HashMap<>(10);
        boolean exit = false;
        try {
            conn = conf.getDbPool().getConnection();
            conn.setAutoCommit(true);
            UpdateInfo param;

            while (true) {
                param = updateQueue.poll(conf.updatequeuetimeout, TimeUnit.MINUTES);
                if (param == null) {
                    exit = true;
                    log.debug("UpdateQueue size (" + number + ") is empty (exit): " + updateQueue.size());
                    break;
                } else if (param.id == null) {
                    if (updateQueue.size() == 0) {
                        exit = true;
                        log.debug("UpdateQueue size (" + number + ") (exit): " + updateQueue.size());
                        break;
                    }
                } else {
                    int i = 0;
                    idsUpd = new ArrayList<>();
                    for (Map.Entry<String, String> e : param.id.entrySet()) {
                        String id = e.getKey();
                        String tableToUpd = e.getValue();
                        uniqueID = id;//param.line;

                        updateSql = "update " + tableToUpd + " set export=1 where id = ?";
                        if (!stmts.containsKey(tableToUpd)) {
                            stmts.put(tableToUpd, conn.prepareStatement(updateSql));
                            log.debug("Prepare statement: " + updateSql);
                        }

                        stmts.get(tableToUpd).clearParameters();
                        stmts.get(tableToUpd).setString(1, uniqueID);
                        stmts.get(tableToUpd).addBatch();
                        idsUpd.add(i, updateSql + " for: " + uniqueID + " in " + tableToUpd);
                        //log.debug(uniqueID + " added to update batch");
                        i++;
                    }

                    for (Map.Entry<String, PreparedStatement> e : stmts.entrySet()) {
                        String table = e.getKey();
                        Statement s = e.getValue();

                        int[] res = s.executeBatch();
                        checkUpdateCounts(res, idsUpd);
                        int sum = 0;
                        if (log.isDebugEnabled()) {
                            for (int ii : res) {
                                sum += ii;
                            }
                        }
                        log.debug("NUM: " + table + " Batch size: " + i + " " + res.length + " " + sum);

                    }

                    if (!conn.getAutoCommit()) {
                        conn.commit();
                        log.debug("Update batch commited");
                    }
                    log.info("Success (" + number + ") for: " + updateSql + " param: " + uniqueID);
                    log.debug("UpdateQueue size (" + number + "): " + updateQueue.size());
                }
            }
            if (exit) {
                updateQueue.put(new UpdateInfo());
            }
            log.debug("Ended (" + number + ")");
            ret = "ok";
        } catch (BatchUpdateException b) {
            log.error("SQLException: " + b.getMessage());
            log.error("SQLState: " + b.getSQLState());
            log.error("Message: " + b.getMessage());
            log.error("Vendor error code: " + b.getErrorCode());
            log.error("Update counts: ");
            int[] updateCounts = b.getUpdateCounts();
            StringBuffer msg = new StringBuffer();
            for (int i = 0; i < updateCounts.length; i++) {
                msg.append(updateCounts[i]).append(' ');
            }
            checkUpdateCounts(updateCounts, idsUpd);
            log.error(msg);
        } catch (SQLException ex) {
            log.error("Fail (" + number + ") SQL:" + updateSql + " ID:" + uniqueID);
            log.error(ex);
        } catch (InterruptedException | RuntimeException ex) {
            log.error("Fail (" + number + ")", ex);
        } finally {
            try {
                for (Map.Entry<String, PreparedStatement> e : stmts.entrySet()) {
                    PreparedStatement table = e.getValue();
                    if (table != null) {
                        table.close();
                    }
                }
            } catch (Exception ex) {
                log.warn(ex.getMessage());
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception ex) {
                log.warn(ex.getMessage());
            }
        }
        return ret;
    }

    public static void checkUpdateCounts(int[] updateCounts, List<String> idsUpd) {
        int okCount = 0;
        int okSum = 0;
        for (int i = 0; i < updateCounts.length; i++) {
            if (updateCounts[i] > 0) {
                // Successfully executed; the number represents number of affected rows
                okCount++;
                okSum += updateCounts[i];
            } else if (updateCounts[i] == 0) {
                log.debug("OK: no rows updated " + " id: " + idsUpd.get(i));
            } else if (updateCounts[i] == Statement.SUCCESS_NO_INFO) {
                // Successfully executed; number of affected rows not available
                log.debug("OK: updateCount=Statement.SUCCESS_NO_INFO " + " id: " + idsUpd.get(i));
            } else if (updateCounts[i] == Statement.EXECUTE_FAILED) {
                log.debug("updateCount=Statement.EXECUTE_FAILED " + " id: " + idsUpd.get(i));
            }
        }

        log.debug(
                "OK: updateCount=" + okCount + " sum: " + okSum);
    }
}
