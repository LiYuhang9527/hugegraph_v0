package com.bypass;

import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.UnitTestBase;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.structure.HugeProperty;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class BypassTest {

    protected static String TABLE_NAME = UnitTestBase.DEFAULT_TEST_TABLE;
    private static final String PD_ADDRESS = "127.0.0.1:8686";
    public HgStoreClient storeClient;
    protected PDClient pdClient;

    @Before
    public void setup() throws Exception {
        storeClient = HgStoreClient.create(PDConfig.of(PD_ADDRESS)
                                                   .setEnableCache(true));
        pdClient = storeClient.getPdClient();

        HgStoreSession session = storeClient.openSession(TABLE_NAME);
        session.dropTable(TABLE_NAME);
        session.truncate();
    }
    @Test
    public void myTest() {
        byte[] owner = {51, 58, 108, 105, 121, 117, 104, 97, 110, 103, 50, 50, 50};
        byte[] key = {-116, 51, 58, 108, 105, 121, 117, 104, 97, 110, 103, 50, 50, 50};
        HgOwnerKey hgOwnerKey = new HgOwnerKey(owner, key);
        byte[] value = {8, 3, 3, 1, 11, 108, 105, 121, 117, 104, 97, 110, 103, 50, 50, 50, 2, 29,
                        3, 7, 66, 101, 105, 106, 105, 110, 103};
        String s1 = new String(owner);
        String s2 = new String(key);
        String s3 = new String(value);
        System.out.println(s1 + s2 + s3);
        // HgStoreSession graph = storeClient.openSession("hugegraph/g");
        // graph.put("g+v", hgOwnerKey, value);

    }

    @Test
    public void myTest2() {
        MyVertex myVertex = new MyVertex(new IdGenerator.LongId(3));
        myVertex.addProperty(new PropertyKey(null, new IdGenerator.LongId(1), "name"), "liyuhang");
        //myVertex.addProperty(new PropertyKey(null, new IdGenerator.LongId(2), "age"), "29");
        //myVertex.addProperty(new PropertyKey(null, new IdGenerator.LongId(3), "city"), "beijing");


        BackendEntry entry = writeVertex(myVertex);

        byte[] owner = getInsertOwner(entry);
        ArrayList<BackendEntry.BackendColumn> columns = new ArrayList<>(entry.columns());
        for (int i = 0; i < columns.size(); i++) {
            BackendEntry.BackendColumn col = columns.get(i);
            HgStoreSession hgStoreSession = storeClient.openSession("hugegraph/g");
            hgStoreSession.put("g+v", HgOwnerKey.of(owner, col.name), col.value);
        }

    }

    public BackendEntry writeVertex(MyVertex vertex) {

        BinaryBackendEntry entry = newBackendEntry(vertex);

        int propsCount = vertex.sizeOfProperties();
        BytesBuffer buffer = BytesBuffer.allocate(8 + 16 * propsCount);

        // Write vertex label
        buffer.writeId(vertex.id());

        // Write all properties of the vertex
        this.formatProperties(vertex.getProperties(), buffer);


        // Fill column
        byte[] name = entry.id().asBytes();
        entry.column(name, buffer.bytes());

        return entry;
    }

    protected void formatProperties(Collection<HugeProperty<?>> props,
                                    BytesBuffer buffer) {
        // Write properties size
        buffer.writeVInt(props.size());

        // Write properties data
        for (HugeProperty<?> property : props) {
            PropertyKey pkey = property.propertyKey();
            buffer.writeVInt(SchemaElement.schemaId(pkey.id()));
            buffer.writeProperty(pkey, property.value());
        }
    }

    protected final BinaryBackendEntry newBackendEntry(MyVertex vertex) {
        return newBackendEntry(vertex.type(), vertex.id());
    }

    protected BinaryBackendEntry newBackendEntry(HugeType type, Id id) {
        if (type.isVertex()) {
            BytesBuffer buffer = BytesBuffer.allocate(2 + 1 + id.length());
            writePartitionedId(HugeType.VERTEX, id, buffer);
            return new BinaryBackendEntry(type, new BinaryBackendEntry.BinaryId(buffer.bytes(), id));
        }

        if (type.isEdge()) {
            E.checkState(id instanceof BinaryBackendEntry.BinaryId,
                         "Expect a BinaryId for BackendEntry with edge id");
            return new BinaryBackendEntry(type, (BinaryBackendEntry.BinaryId) id);
        }


        BytesBuffer buffer = BytesBuffer.allocate(1 + id.length());
        byte[] idBytes = buffer.writeId(id).bytes();
        return new BinaryBackendEntry(type, new BinaryBackendEntry.BinaryId(idBytes, id));
    }

    private void writePartitionedId(HugeType type, Id id, BytesBuffer buffer) {
        buffer.writeId(id);
    }

    protected short getPartition(HugeType type, Id id) {
        return 0;
    }

    public byte[] getInsertOwner(BackendEntry entry) {
        // To adapt label index hashing, do not focus on a single partition
        if (entry.type().isLabelIndex() && (entry.columns().size() == 1)) {
            Iterator<BackendEntry.BackendColumn> iterator = entry.columns().iterator();
            while (iterator.hasNext()) {
                BackendEntry.BackendColumn next = iterator.next();
                return next.name;
            }
        }

        Id id = entry.type().isIndex() ? entry.id() : entry.originId();
        return getOwnerId(id);
    }

    protected byte[] getOwnerId(Id id) {
        if (id instanceof BinaryBackendEntry.BinaryId) {
            id = ((BinaryBackendEntry.BinaryId) id).origin();
        }
        if (id != null && id.edge()) {
            id = ((EdgeId) id).ownerVertexId();
        }
        return id != null ? id.asBytes() :
               HgStoreClientConst.ALL_PARTITION_OWNER;
    }
}
