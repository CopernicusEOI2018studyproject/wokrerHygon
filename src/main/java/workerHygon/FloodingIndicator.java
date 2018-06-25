/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package workerHygon;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
// new imports
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.notgroupb.dataPreformatter.formats.DataPointDeserializer;
import org.notgroupb.dataPreformatter.formats.DataPointSerializer;
import org.notgroupb.dataPreformatter.formats.HygonDataPoint;
// import stream processing utilities
import org.notgroupb.dataPreformatter.formats.OutputDataPoint;


/**
 * In this example, we implement a simple WordCount program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text,
 * split each text line into words and then compute the word occurence histogram, write the continuous updated histogram
 * into a topic "streams-wordcount-output" where each record is an updated count of a single word.
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

                    int score = 0;
                    // score = 10 * (element.getMhw() / element.getMeasurement()); 
                    // score = (int) (100 * ((element.getMhw() / element.getMeasurement()) - 1));
                    score = (int) (((value.getMeasurement() - value.getMhw())));

                    // // flooding indicators
                    // element.getMnw();
                    // element.getMhw();
                    // element.getAverage();
                    // element.getLevel1();
                    // element.getLevel2();
                    // element.getLevel3();

                    if (score < 0) {
                        score = 0;
                    }
                    if (score > 100) {
                        score = 100;
                    }

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
                Serdes.serdeFrom(new DataPointSerializer<OutputDataPoint>(), new DataPointDeserializer<OutputDataPoint>())
            )
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
