package com.epam;

import com.epam.model.Person;
import com.epam.serializer.PersonSerializerSchema;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;


public class PersonStreamingJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(30000);

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"persons-group");

        FlinkKafkaConsumer<Person> personFlinkKafkaConsumer = new FlinkKafkaConsumer<>("persons", new PersonSerializerSchema(), props);

        env.addSource(personFlinkKafkaConsumer)
                .map(person -> Tuple2.of(person.getName(), person.getLastName()))
                .flatMap(new RichFlatMapFunction<Tuple2<String, String>, Tuple1<String>>() {

                    @Override
                    public void flatMap(Tuple2<String, String> value, Collector<Tuple1<String>> out) throws Exception {
                        out.collect(Tuple1.of(value.f0 + " " + value.f1));
                    }
                }).print();

        env.execute("Person Job");

    }
}
