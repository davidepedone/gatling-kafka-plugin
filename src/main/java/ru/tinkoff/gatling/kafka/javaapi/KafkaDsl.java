package ru.tinkoff.gatling.kafka.javaapi;

import static io.gatling.javaapi.core.internal.Expressions.*;

import io.gatling.core.check.Check;
import io.gatling.core.check.CheckMaterializer;
import io.gatling.javaapi.core.CheckBuilder;
import ru.tinkoff.gatling.kafka.javaapi.checks.KafkaCheckType;
import ru.tinkoff.gatling.kafka.javaapi.protocol.*;
import ru.tinkoff.gatling.kafka.javaapi.request.builder.*;
import ru.tinkoff.gatling.kafka.request.KafkaProtocolMessage;
import scala.Function1;

import static io.gatling.javaapi.core.internal.Converters.toScalaFunction;

public final class KafkaDsl {

    public static KafkaProtocolBuilderBase kafka() {
        return new KafkaProtocolBuilderBase();
    }

    public static KafkaRequestBuilderBase kafka(String requestName) {
        return new KafkaRequestBuilderBase(ru.tinkoff.gatling.kafka.Predef.kafka(toStringExpression(requestName)), requestName);
    }

    @SuppressWarnings("rawtypes")
    public static CheckBuilder simpleCheck(Function1<KafkaProtocolMessage, Boolean> f, String errorMessage) {
        return new CheckBuilder() {

            @Override
            public io.gatling.core.check.CheckBuilder<?, ?> asScala() {
                return new io.gatling.core.check.CheckBuilder() {

                    @Override
                    public Check build(CheckMaterializer materializer) {
                        return ru.tinkoff.gatling.kafka.Predef.simpleCheck(toScalaFunction(f::apply), errorMessage);
                    }
                };
            }

            @Override
            public CheckType type() {
                return KafkaCheckType.Simple;
            }
        };
    }

}
