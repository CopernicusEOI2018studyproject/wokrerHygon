package org.notgroupb.workerHygon;


import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.notgroupb.formats.HygonDataPoint;
import org.notgroupb.formats.OutputDataPoint;
import org.notgroupb.formats.deserialize.HygonDataPointDeserializer;
import org.notgroupb.formats.deserialize.OutputDataPointDeserializer;
import org.notgroupb.formats.serialize.DataPointSerializer;

import com.beust.jcommander.Parameter;


/**
 * 
 */
public class workerHygonApplication {

	@Parameter(names={"--bootstrap-servers", "-bs"})
	static String BOOTSTRAP_SERVER = "localhost:9092";

	public static void main(String[] args) throws Exception {

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "workerHygon");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		final StreamsBuilder builder = new StreamsBuilder();

		KStream<String, HygonDataPoint> sourceHygon = builder.stream("HygonData",
				Consumed.with(Serdes.String(), 
						Serdes.serdeFrom(new DataPointSerializer<HygonDataPoint>(), new HygonDataPointDeserializer()))
				);

		// parse source to distinguish between flooding and no flooding
		sourceHygon.map((KeyValueMapper<String, HygonDataPoint, KeyValue<String, OutputDataPoint>>) (key, value) -> {
			KeyValue<String, OutputDataPoint> result = null;

			double score = 0.0;
			// flooding indicators
			double mnw = value.getMnw();
			double mhw = value.getMhw();
			double avg = value.getAverage();

			ScoreHygon scoreHydro = new ScoreHygon();
			scoreHydro.initScore(mhw, avg, mnw);
			score = scoreHydro.score(value.getMeasurement());

			OutputDataPoint output = new OutputDataPoint();
			output.setLat(value.getLat());
			output.setLon(value.getLon());
			output.setScore(score);
			output.setName(value.getName());
			output.setRecordTime(value.getRecordTime());

			result = new KeyValue<>(key, output);
			return result;
		})
		.filterNot((Predicate<String,OutputDataPoint>)(k,v) -> {
			return v.getScore() == 0;
		})
		.to("OutputEvents",		
				Produced.with(
						Serdes.String(),
						Serdes.serdeFrom(new DataPointSerializer<OutputDataPoint>(), new OutputDataPointDeserializer())
						)
		);

		// #####################################################

		final Topology topology = builder.build();
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
