package com.bypass;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.structure.HugeProperty;
import org.apache.hugegraph.structure.HugeVertexProperty;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.collection.CollectionFactory;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;

import java.util.Collection;

public class MyVertex {
    private Id id;

    private MutableIntObjectMap<HugeProperty<?>> properties;
    private static final MutableIntObjectMap<HugeProperty<?>> EMPTY_MAP =
            CollectionFactory.newIntObjectMap();

    public MyVertex(Id id) {
        this.id = id;
        this.properties = EMPTY_MAP;
    }

    public Id id() {
        return this.id;
    }

    public HugeType type() {
        return HugeType.VERTEX;
    }

    public Collection<HugeProperty<?>> getProperties() {
        return this.properties.values();
    }

    public int sizeOfProperties() {
        return this.properties.size();
    }

    public <V> HugeProperty<V> addProperty(PropertyKey pkey, V value) {
        return this.addProperty(pkey, value, false);
    }

    public <V> HugeProperty<V> addProperty(PropertyKey pkey, V value,
                                           boolean notify) {
        HugeProperty<V> prop = null;
        switch (pkey.cardinality()) {
            case SINGLE:
                prop = this.newProperty(pkey, value);

                this.setProperty(prop);
                break;
            default:
                assert false;
                break;
        }
        return prop;
    }

    public <V> HugeProperty<?> setProperty(HugeProperty<V> prop) {
        PropertyKey pkey = prop.propertyKey();

        E.checkArgument(this.properties.containsKey(intFromId(pkey.id())) ||
                        this.properties.size() < BytesBuffer.MAX_PROPERTIES,
                        "Exceeded the maximum number of properties");
        return this.properties.put(intFromId(pkey.id()), prop);
    }

    public static int intFromId(Id id) {
        E.checkArgument(id instanceof IdGenerator.LongId,
                        "Can't get number from %s(%s)", id, id.getClass());
        return ((IdGenerator.LongId) id).intValue();
    }

    protected <V> HugeVertexProperty<V> newProperty(PropertyKey pkey, V val) {
        return new HugeVertexProperty<>(null, pkey, val);
    }



}
