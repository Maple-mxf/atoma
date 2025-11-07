package atoma.storage.mongo.command;

import atoma.api.cluster.GroupMember;
import org.bson.Document;

import java.util.Map;

/**
 * A concrete implementation of GroupMember backed by a BSON Document.
 */
public class DocumentGroupMember implements GroupMember {

    private final Document doc;

    public DocumentGroupMember(Document doc) {
        this.doc = doc;
    }

    @Override
    public String getId() {
        return doc.getString("id");
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, String> getMetadata() {
        return doc.get("metadata", Map.class);
    }
}
