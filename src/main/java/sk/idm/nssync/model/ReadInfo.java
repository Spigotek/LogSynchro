package sk.idm.nssync.model;

import java.util.Map;

public class ReadInfo {

    public String id;
    public String idFromHash;
    public String table;
    public String index;
    public String mapping;
    public Map<String, Object> values;

    public ReadInfo() {
        this(null, null, null, null, null, null);
    }
    
    public ReadInfo(String id, Map<String, Object> values, String table, String index, String mapping, String idFromHash) {
        this.id = id;
        this.idFromHash = idFromHash;
        this.index = index;
        this.mapping = mapping;
        this.values = values;
        this.table = table;
    }
}
