package atoma.core.internal.cluster;

import atoma.api.cluster.GroupMember;

import java.util.Map;

public class MapGroupMember implements GroupMember {

    private final Map<String, Object> data;

    public MapGroupMember(Map<String, Object> data) {
        this.data = data;
    }

    @Override
    public String getId() {
        return (String) data.get("id");
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, String> getMetadata() {
        return (Map<String, String>) data.get("metadata");
    }
}
