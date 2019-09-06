package com.zhisheng.connectors.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import com.zhisheng.connectors.akka.utils.ReceiverActor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.Collections;

/**
 * Desc:
 * Created by zhisheng on 2019-06-08
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class AkkaSource extends RichSourceFunction<Object> {

    private static final Logger LOG = LoggerFactory.getLogger(AkkaSource.class);

    private static final long serialVersionUID = 1L;

    // --- Fields set by the constructor

    private final Class<?> classForActor;

    private final String actorName;

    private final String urlOfPublisher;

    private final Config configuration;

    // --- Runtime fields
    private transient ActorSystem receiverActorSystem;
    private transient ActorRef receiverActor;

    protected transient boolean autoAck;

    /**
     * Creates {@link AkkaSource} for Streaming
     *
     * @param actorName      Receiver Actor name
     * @param urlOfPublisher tcp url of the publisher or feeder actor
     */
    public AkkaSource(String actorName, String urlOfPublisher, Config configuration) {
        super();
        this.classForActor = ReceiverActor.class;
        this.actorName = actorName;
        this.urlOfPublisher = urlOfPublisher;
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        receiverActorSystem = createDefaultActorSystem();

        if (configuration.hasPath("akka.remote.auto-ack") &&
                configuration.getString("akka.remote.auto-ack").equals("on")) {
            autoAck = true;
        } else {
            autoAck = false;
        }
    }

    @Override
    public void run(SourceFunction.SourceContext<Object> ctx) throws Exception {
        LOG.info("Starting the Receiver actor {}", actorName);
        receiverActor = receiverActorSystem.actorOf(
                Props.create(classForActor, ctx, urlOfPublisher, autoAck), actorName);

        LOG.info("Started the Receiver actor {} successfully", actorName);
        Await.result(receiverActorSystem.whenTerminated(), Duration.Inf());
    }

    @Override
    public void close() {
        LOG.info("Closing source");
        if (receiverActorSystem != null) {
            receiverActor.tell(PoisonPill.getInstance(), ActorRef.noSender());
            receiverActorSystem.terminate();
        }
    }

    @Override
    public void cancel() {
        LOG.info("Cancelling akka source");
        close();
    }

    /**
     * Creates an actor system with default configurations for Receiver actor.
     *
     * @return Actor System instance with default configurations
     */
    private ActorSystem createDefaultActorSystem() {
        String defaultActorSystemName = "receiver-actor-system";

        Config finalConfig = getOrCreateMandatoryProperties(configuration);

        return ActorSystem.create(defaultActorSystemName, finalConfig);
    }

    private Config getOrCreateMandatoryProperties(Config properties) {
        if (!properties.hasPath("akka.actor.provider")) {
            properties = properties.withValue("akka.actor.provider",
                    ConfigValueFactory.fromAnyRef("akka.remote.RemoteActorRefProvider"));
        }

        if (!properties.hasPath("akka.remote.enabled-transports")) {
            properties = properties.withValue("akka.remote.enabled-transports",
                    ConfigValueFactory.fromAnyRef(Collections.singletonList("akka.remote.netty.tcp")));
        }
        return properties;
    }
}
