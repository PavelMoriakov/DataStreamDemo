package com.epam.serializer;

import com.epam.model.Person;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PersonSerializerSchema implements DeserializationSchema<Person> {
    @Override
    public Person deserialize(byte[] message) throws IOException {
        return SerializationUtils.deserialize(message);
    }

    @Override
    public boolean isEndOfStream(Person nextElement) {
        return nextElement != null;
    }

    @Override
    public TypeInformation<Person> getProducedType() {
        return TypeInformation.of(Person.class);
    }
}
