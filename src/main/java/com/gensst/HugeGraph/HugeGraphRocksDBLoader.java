package com.gensst.HugeGraph;

import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.structure.GraphElement;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.schema.PropertyKey;
import com.genfile.HBase.HugeGraph.direct.struct.HugeType;
import com.genfile.HBase.HugeGraph.direct.util.BytesBuffer;
import com.genfile.HBase.HugeGraph.direct.util.GraphSchema;
import com.genfile.HBase.HugeGraph.direct.util.Id;
import com.genfile.HBase.HugeGraph.direct.util.IdGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class HugeGraphRocksDBLoader {
    static HugeClient client;
    boolean bypassServerHBase = true;
    GraphSchema graphSchema;
    private static final int edgeLogicPartitions = 16;
    private static final int vertexLogicPartitions = 8;

    public static void main(String[] args) {
        HugeGraphRocksDBLoader ins = new HugeGraphRocksDBLoader();
        ins.initGraph();
    }

    // 初始化HugeGraph客户端并构建图模式
    void initGraph() {
        // If connect failed will throw an exception.
        client = HugeClient.builder("http://localhost:8081", "hugegraph").build();

        // 构建Schema
        SchemaManager schema = client.schema();

        schema.propertyKey("name").asText().ifNotExist().create();
        schema.propertyKey("age").asInt().ifNotExist().create();
        schema.propertyKey("lang").asText().ifNotExist().create();
        schema.propertyKey("date").asText().ifNotExist().create();
        schema.propertyKey("price").asText().ifNotExist().create();

        schema.vertexLabel("person")
                .properties("name", "age")
                .useCustomizeStringId()
                .enableLabelIndex(false)
                .ifNotExist()
                .create();

        schema.vertexLabel("personB")
                .properties("price")
                .nullableKeys("price")
                .useCustomizeNumberId()
                .enableLabelIndex(false)
                .ifNotExist()
                .create();

        schema.vertexLabel("software")
                .properties("name", "lang", "price")
                .useCustomizeStringId()
                .enableLabelIndex(false)
                .ifNotExist()
                .create();

        schema.edgeLabel("knows")
                .link("person", "person")
                .properties("date")
                .enableLabelIndex(false)
                .ifNotExist()
                .create();

        schema.edgeLabel("created")
                .link("person", "software")
                .properties("date")
                .enableLabelIndex(false)
                .ifNotExist()
                .create();

        graphSchema = new GraphSchema(client);//schema 缓存到内存 对象中 共享
        writeGraphElements();
        client.close();
    }

    // 这个方法构造一些顶点和边，并决定是直接写入HBase还是通过HugeGraph服务器写入。
    private void writeGraphElements() {
        GraphManager graph = client.graph();

        // construct some vertexes & edges
        /**
         * ID生成策略：主键模式 生成primaykey
         */
        Vertex peter = new Vertex("person");
        peter.property("name", "peter");
        peter.property("age", 35);
        peter.id("peter");

        /**
         * ID生成策略：自定义字符串ID
         */

        Vertex lop = new Vertex("software");
        lop.property("name", "lop");
        lop.property("lang", "java");
        lop.property("price", "328");
        lop.id("lop");

        /**
         * ID生成策略：自定义数字主键
         */
        Vertex vadasB = new Vertex("personB");
        vadasB.property("price", "120");
        vadasB.id(12345);

        Edge peterCreateLop = new Edge("created").source(peter).target(lop)
                .property("date", "2017-03-24");

        List<Vertex> vertices = new ArrayList<Vertex>(){{
            add(peter);add(vadasB);add(lop);
        }};


        List<Edge> edges = new ArrayList<Edge>(){{
            add(peterCreateLop);
        }};

        // Old way: encode to json then send to server
        if (bypassServerHBase) {
            writeDirectly(vertices, edges);
        } else {
            writeByServer(graph, vertices, edges);
        }
    }

    // 这个方法直接将顶点和边转换为字节数组并写入HBase
    /* we transfer the vertex & edge into bytes array
     * TODO: use a batch and send them together
     * */
    void writeDirectly(List<Vertex> vertices, List<Edge> edges) {
        for (Vertex vertex : vertices) {
            byte[] rowkey = getKeyBytes(vertex);
            byte[] values = getValueBytes(vertex);
            sendRpcToHBase("vertex", rowkey, values);
        }

        for (Edge edge: edges) {
            byte[] rowkey = getKeyBytes(edge);
            byte[] values = getValueBytes(edge);
            sendRpcToHBase("edge", rowkey, values);
        }
    }

    boolean sendRpcToRocksDB(String type, byte[] rowkey, byte[] values) {
        // here we call the rpc
        boolean flag = false;

        return flag;
    }


    // 这个方法用于将数据写入HBase
    boolean putHBase(String type, byte[] rowkey, byte[] values) throws IOException {
        Configuration config = HBaseConfiguration.create();
        // Our json records sometimes are very big, we have
        // disable the maxsize check on the keyvalue.
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2181");


        Connection conn = ConnectionFactory.createConnection(config);
        Table htable = null ;
        if (type.equals("vertex")) {
            htable = conn.getTable(TableName.valueOf("hugegraph12p:g_v"));
        } else if (type.equals("edge")) {
            htable = conn.getTable(TableName.valueOf("hugegraph12p:g_oe"));
        }

        Put put = new Put(rowkey);
        put.addColumn(Bytes.toBytes("f"),
                Bytes.toBytes(""),
                values);
        htable.put(put);
        htable.close();


        return true;
    }

    boolean sendRpcToHBase(String type, byte[] rowkey, byte[] values) {
        boolean flag = false;
        try {
            flag = putHBase(type, rowkey, values);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }

    // 这个方法通过HugeGraph服务器将顶点和边写入图
    void writeByServer(GraphManager graph, List<Vertex> vertices, List<Edge> edges) {
        System.out.println("=========before add graph vertex=======");
        for(Vertex vertex : vertices){
            System.out.println(vertex.toString());
        }
        vertices = graph.addVertices(vertices);
        System.out.println("=========after add graph vertex=======");
        for(Vertex vertex : vertices){
            System.out.println(vertex.toString());
        }

        System.out.println("=========before add graph edge=======");
        for (Edge edge:edges) {
            System.out.println(edge.toString());
        }
        edges = graph.addEdges(edges, false);
        System.out.println("=========after add graph edge=======");
        for (Edge edge:edges) {
            System.out.println(edge.toString());
        }

    }


    byte[] getKeyBytes(GraphElement e) {
        byte[] array = null;
        if(e.type() == "vertex" && e.id() != null){
            BytesBuffer buffer = BytesBuffer.allocate(2 + 1 + e.id().toString().length());
            buffer.writeShort(getPartition(HugeType.VERTEX, IdGenerator.of(e.id())));
            buffer.writeId(IdGenerator.of(e.id()));
            array = buffer.bytes();
        }else if ( e.type() == "edge" ){
            BytesBuffer buffer = BytesBuffer.allocate(BytesBuffer.BUF_EDGE_ID);
            Edge edge = (Edge)e;
            buffer.writeShort(getPartition(HugeType.EDGE, IdGenerator.of(edge.sourceId())));
            buffer.writeId(IdGenerator.of(edge.sourceId()));
            buffer.write(HugeType.EDGE_OUT.code());
            buffer.writeId(IdGenerator.of(graphSchema.getEdgeLabel(e.label()).id()));//出现错误
            buffer.writeStringWithEnding("");
            buffer.writeId(IdGenerator.of(edge.targetId()));
            array = buffer.bytes();
        }
        return array;
    }

    short getPartition(HugeType type, Id id) {
        int hashcode = Arrays.hashCode(id.asBytes());
        short partition = 1;
        if (type.isEdge()) {
            partition = (short) (hashcode % edgeLogicPartitions);
        } else if (type.isVertex()) {
            partition = (short) (hashcode % vertexLogicPartitions);
        }
        return partition > 0 ? partition : (short) -partition;
    }


    byte[] getValueBytes(GraphElement e) {
        byte[] array = null;
        if(e.type() == "vertex"){
            int propsCount = e.properties().size() ;//vertex.sizeOfProperties();
            BytesBuffer buffer = BytesBuffer.allocate(8 + 16 * propsCount);
            buffer.writeId(IdGenerator.of(graphSchema.getVertexLabel(e.label()).id()));
            buffer.writeVInt(propsCount);
            for(Map.Entry<String, Object> entry : e.properties().entrySet()){
                PropertyKey propertyKey = graphSchema.getPropertyKey(entry.getKey());
                buffer.writeVInt(propertyKey.id().intValue());
                buffer.writeProperty(propertyKey.dataType(),entry.getValue());
            }
            array = buffer.bytes();
        } else if ( e.type() == "edge" ){
            int propsCount =  e.properties().size();
            BytesBuffer buffer = BytesBuffer.allocate(4 + 16 * propsCount);
            buffer.writeVInt(propsCount);
            for(Map.Entry<String, Object> entry : e.properties().entrySet()){
                PropertyKey propertyKey = graphSchema.getPropertyKey(entry.getKey());
                buffer.writeVInt(propertyKey.id().intValue());
                buffer.writeProperty(propertyKey.dataType(),entry.getValue());
            }
            array = buffer.bytes();
        }

        return array;
    }
}
