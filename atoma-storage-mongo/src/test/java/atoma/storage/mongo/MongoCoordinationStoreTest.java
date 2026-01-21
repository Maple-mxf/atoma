package atoma.storage.mongo;

import atoma.storage.mongo.MongoCoordinationStore;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MongoCoordinationStoreTest {

    @Mock
    private MongoClient mockMongoClient;

    @Mock
    private MongoDatabase mockMongoDatabase;

    @Mock
    private ChangeStreamIterable<Document> mockChangeStreamIterable;

    @Mock
    private MongoChangeStreamCursor<ChangeStreamDocument<org.bson.Document>> mockCursor;


    private MongoCoordinationStore store;

    @BeforeEach
    void setUp() {
        when(mockMongoClient.getDatabase(any())).thenReturn(mockMongoDatabase);
        when(mockMongoDatabase.watch()).thenReturn(mockChangeStreamIterable);
        when(mockChangeStreamIterable.fullDocument(any())).thenReturn(mockChangeStreamIterable);
        when(mockChangeStreamIterable.fullDocumentBeforeChange(any())).thenReturn(mockChangeStreamIterable);
        when(mockChangeStreamIterable.cursor()).thenReturn(mockCursor);

        store = new MongoCoordinationStore(mockMongoClient, "testDb");
    }

    @Test
    void get_shouldReturnEmptyOptional() {
        Optional<atoma.api.coordination.Resource> resource = store.get("anyId");
        assertFalse(resource.isPresent());
    }
}
