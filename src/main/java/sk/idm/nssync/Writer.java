/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sk.idm.nssync;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import sk.idm.nssync.model.ReadInfo;
import sk.idm.nssync.model.UpdateInfo;

/**
 *
 * @author mhajducek
 */
class Writer implements Callable {

    private final BlockingQueue<ReadInfo> readQueue;
    private final BlockingQueue<UpdateInfo> updateQueue;
    private static final Logger log = LogManager.getLogger(Writer.class);
    private final Config conf;
    private final int number;
    private static final String ID_COLUMN = "id";

    public Writer(BlockingQueue<ReadInfo> readQueue, BlockingQueue<UpdateInfo> updateQueue, Config conf, int number) {
        this.readQueue = readQueue;
        this.updateQueue = updateQueue;
        this.conf = conf;
        this.number = number;
    }

    @Override
    public String call() throws Exception {
        Client client = Sync.client;

        String ret = "error";
        log.debug("Started (" + number + ")");
        ReadInfo param = null;
        try {
            //loop for all documents
            while (true) {
                XContentBuilder xcb;
                boolean exit = false;
                BulkRequestBuilder bulkRequest = client.prepareBulk();
                UpdateInfo updInfo = new UpdateInfo(conf.bulkwritesize + 1);
                int j = 0;
                //loop for documents in bulk batch
                for (int i = 0; i <= conf.bulkwritesize; i++) {
                    param = readQueue.poll(conf.readqueuetimeout, TimeUnit.MINUTES);
                    if (param == null || param.id == null) {
                        readQueue.put(new ReadInfo());
                        exit = true;
                        break;
                    }
                    xcb = jsonBuilder();
                    xcb = xcb.startObject();

                    for (String column : param.values.keySet()) {
                        String elasticField;
                        String key = param.table.substring(0, param.table.length() - 2) + column;
                        if (conf.mapColumns.containsKey(key)) {
                            elasticField = conf.mapColumns.get(key);
                        } else {
                            elasticField = column;
                        }
                        if (!column.toLowerCase(Locale.US).equals(ID_COLUMN)) {
                            Object value = param.values.get(column);
                            xcb = xcb.field(elasticField, value);
                        }
                    }
                    xcb = xcb.endObject();
                    bulkRequest.add(client.prepareIndex(param.index, param.mapping, (param.idFromHash != null)?param.idFromHash:param.id).setSource(xcb));
                    updInfo.id.put(param.id, param.table);
                    j++;
                }
                log.debug("NUM: Indexed documents to update (" + number + "): " + j);

                //log.debug("Bulk request: " + ((xcb == null) ? "null" : xcb.string()));
                if (j > 0) {
                    BulkResponse response = bulkRequest.get();
                    if (response.hasFailures()) {
                        for (BulkItemResponse b : response) {
                            if (b.isFailed()) {
                                log.error("Error indexing id (" + number + "): " + b.getId() + " message: " + b.getFailureMessage());
                                updInfo.id.remove(b.getId());
                            }
                        }
                    }

                    log.debug("UpdateQueue put (" + number + "): " + updInfo.id.size());
                    updateQueue.put(updInfo);
                    log.debug("UpdateQueue size (" + number + "): " + updateQueue.size());

                    log.debug("ReadQueue remaining size (" + number + "): " + readQueue.remainingCapacity());
                    log.info("Success (" + number + ") for: " + ((param != null) ? param.id : "null"));
                }
                if (exit) {
                    exitUpdater(updateQueue);
                    break;
                }
            }

            log.debug("Ended (" + number + ")");
            ret = "ok";
        } catch (InterruptedException | IOException ex) {
            log.error("Fail (" + number + ") for: " + ((param != null) ? param.id : "null"));
            log.error("Error: ", ex);
        } finally {
            exitUpdater(updateQueue);
            client.close();
        }
        return ret;
    }

    private static boolean exitUpdate = false;

    private void exitUpdater(BlockingQueue<UpdateInfo> q) throws InterruptedException {
        if (!exitUpdate) {
            exitUpdate = true;
            updateQueue.put(new UpdateInfo());
            log.debug("UpdateQueue size (exit) (" + number + "):" + updateQueue.size());
        }
    }
}
