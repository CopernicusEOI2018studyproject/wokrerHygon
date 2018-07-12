package org.notgroupb.workerHygon;


import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.notgroupb.formats.HygonDataPoint;
import org.notgroupb.formats.OutputDataPoint;
import org.notgroupb.formats.deserialize.OutputDataPointDeserializer;
import org.notgroupb.formats.serialize.DataPointSerializer;


/**
 * 
 */
public class FloodingIndicator {

    public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-workerhygon");
        // props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		final StreamsBuilder builder = new StreamsBuilder();

		// #####################################################
		// create source
		KStream<String, HygonDataPoint> sourceHygon = builder.stream("HygonData");

		// parse source to distinguish between flooding and no flooding
		KStream<String, OutputDataPoint> outputStream = sourceHygon
				.map((KeyValueMapper<String, HygonDataPoint, KeyValue<String, OutputDataPoint>>) (key, value) -> {
					KeyValue<String, OutputDataPoint> result = null;

                    double score = 0.0;
                    // score = 10 * (element.getMhw() / element.getMeasurement()); 
                    // score = (int) (100 * ((value.getMhw() / value.getMeasurement()) - 1));
                    // score = (int) (((value.getMeasurement() - value.getMhw())));

                    double currentMeas = value.getMeasurement();
                    // // flooding indicators
                    double mnw = value.getMnw();
                    double mhw = value.getMhw();
                    double avg = value.getAverage();
                    //double lvl1 = value.getLevel1();
                    //double lvl2 = value.getLevel2();
                    //double lvl3 = value.getLevel3();
                    
                    ScoreHygon scoreHydro = new ScoreHygon();
                    
                    scoreHydro.initScore(mhw, avg, mnw);
                    
                    score = scoreHydro.score(currentMeas);

                    OutputDataPoint opHygon = new OutputDataPoint();
                    opHygon.setLat(value.getLat());
                    opHygon.setLon(value.getLon());
                    opHygon.setScore(score);

                    result = new KeyValue<>(key, opHygon);
					return result;
                });
        
        outputStream.to("HygonOutput",
				Produced.with(
						Serdes.String(),
						Serdes.serdeFrom(new DataPointSerializer<OutputDataPoint>(), new OutputDataPointDeserializer()))
        );

		// #####################################################

		final Topology topology = builder.build();

		// // print topology and show source and sink
		// System.out.println(topology.describe());

		final KafkaStreams streams = new KafkaStreams(topology, props);
		final CountDownLatch latch = new CountDownLatch(1);

		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}
		System.exit(0);
    }

}
