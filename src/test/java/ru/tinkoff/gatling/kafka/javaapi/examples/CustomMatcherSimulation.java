package ru.tinkoff.gatling.kafka.javaapi.examples;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import org.apache.kafka.clients.producer.ProducerConfig;
import ru.tinkoff.gatling.kafka.javaapi.protocol.KafkaProtocolBuilderNew;
import ru.tinkoff.gatling.kafka.protocol.KafkaProtocol;
import ru.tinkoff.gatling.kafka.request.KafkaProtocolMessage;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.gatling.javaapi.core.CoreDsl.atOnceUsers;
import static io.gatling.javaapi.core.CoreDsl.scenario;
import static ru.tinkoff.gatling.kafka.javaapi.KafkaDsl.kafka;

public class CustomMatcherSimulation extends Simulation {
    private static class MyCustomMatcher implements KafkaProtocol.KafkaMatcher {
        @Override
        public byte[] requestMatch(KafkaProtocolMessage msg) {
            // do something with the produced message and extract the values you are interested in
            return "Custom Message".getBytes(); // just returning something
        }

        @Override
        public byte[] responseMatch(KafkaProtocolMessage msg) {
            // do something with the consumed message and extract the values you are interested in
            return "Custom Message".getBytes(); // just returning something
        }
    }

    private final KafkaProtocolBuilderNew kafkaProtocolMatchByKafkaMatcher = kafka().requestReply()
            .producerSettings(
                    Map.of(
                            ProducerConfig.ACKS_CONFIG, "1",
                            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
                    )
            )
            .consumeSettings(
                    Map.of(
                            "bootstrap.servers", "localhost:9092"
                    )
            )
            .timeout(Duration.ofSeconds(5))
    .matchByKafkaMatcher(new MyCustomMatcher());

    private final AtomicInteger c = new AtomicInteger(0);
    private final Iterator<Map<String, Object>> feeder =
            Stream.generate((Supplier<Map<String, Object>>) () -> Collections.singletonMap("kekey", c.incrementAndGet())
            ).iterator();

    private final ScenarioBuilder scn = scenario("Basic")
    .feed(feeder)
    .exec(
            kafka("ReqRep").requestReply()
            .requestTopic("test.t")
            .replyTopic("test.t")
        .send("#{kekey}", """
                { "m": "dkf" }
                """, String.class, String.class));

    {
        setUp(
                scn.injectOpen(atOnceUsers(1)))
                .protocols(kafkaProtocolMatchByKafkaMatcher)
                .maxDuration(Duration.ofSeconds(120));
    }

}
