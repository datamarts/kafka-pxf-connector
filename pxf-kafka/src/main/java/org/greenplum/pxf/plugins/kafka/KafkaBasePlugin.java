/**
 * Copyright © 2022 DATAMART LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.greenplum.pxf.plugins.kafka;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class KafkaBasePlugin extends BasePlugin {

    private static final String KAFKA_PROPERTY_PREFIX = "kafka.property.";
    private static final String KAFKA_SERVERS_PROPERTY_NAME = "kafka.bootstrap.servers";
    private static final String KAFKA_SERVERS_OPTION_NAME = "BOOTSTRAP_SERVERS";
    private static final String KAFKA_BATCH_SIZE_PROPERTY_NAME = "kafka.batch.size";
    private static final int KAFKA_BATCH_SIZE_DEFAULT_VALUE = 1;

    private static final String KAFKA_TOPIC_AUTO_CREATE_FLAG_PROPERTY_NAME = "kafka.topic.auto.create";
    private static final String KAFKA_TOPIC_AUTO_CREATE_FLAG_OPTION_NAME = "TOPIC_AUTO_CREATE_FLAG";
    private static final boolean KAFKA_TOPIC_AUTO_CREATE_FLAG_DEFAULT_VALUE = true;
    private static final int TOPIC_DEFAULT_PARTITION_NUMBER = 1;
    private static final short TOPIC_DEFAULT_REPLICATION_FACTOR = 1;

    private static final String BUFFER_SIZE_PROPERTY_NAME = "kafka.buffer.size";
    private static final int DEFAULT_BUFFER_SIZE = 1000;

    protected int bufferSize = DEFAULT_BUFFER_SIZE;
    protected final Map<String, Object> kafkaProps = new HashMap<>();
    protected String topic;
    protected int batchSize;


    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);

        // Required metadata
        topic = context.getDataSource();
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("Topic must be provided");
        }

        // Required parameter, also can be auto-overwritten by user options
        String servers = configuration.get(KAFKA_SERVERS_PROPERTY_NAME);
        if (StringUtils.isBlank(servers)) {
            throw new IllegalArgumentException(String.format(
                    "Required parameter %s is missing or empty in kafka-site.xml and option %s is not specified in table definition.",
                    KAFKA_SERVERS_PROPERTY_NAME, KAFKA_SERVERS_OPTION_NAME)
            );
        }

        String bufferSize = configuration.get(BUFFER_SIZE_PROPERTY_NAME);
        if (StringUtils.isNotBlank(bufferSize)) {
            this.bufferSize = Integer.parseInt(bufferSize);
        }

        LOG.debug("Bootstrap servers: '{}'", servers);

        batchSize = configuration.getInt(KAFKA_BATCH_SIZE_PROPERTY_NAME, KAFKA_BATCH_SIZE_DEFAULT_VALUE);

        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        for (Map.Entry<String, String> property : configuration) {
            if (property.getKey().startsWith(KAFKA_PROPERTY_PREFIX)) {
                kafkaProps.put(property.getKey().substring(KAFKA_PROPERTY_PREFIX.length()), property.getValue());
            }
        }

        LOG.debug("Kafka properties: {}", kafkaProps);
        LOG.debug("Incoming columns: {}", context.getTupleDescription());

        if (!topicExists(servers, topic)) {
            throw new IllegalArgumentException(String.format(
                    "Topic [%s] doesn't exist and parameter %s/option %s flag is set to FALSE",
                    topic, KAFKA_TOPIC_AUTO_CREATE_FLAG_PROPERTY_NAME, KAFKA_TOPIC_AUTO_CREATE_FLAG_OPTION_NAME));
        }
    }

    protected boolean topicExists(String servers, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        try (AdminClient adminClient = AdminClient.create(props)) {
            boolean topicAutoCreateFlag = configuration.getBoolean(KAFKA_TOPIC_AUTO_CREATE_FLAG_PROPERTY_NAME, KAFKA_TOPIC_AUTO_CREATE_FLAG_DEFAULT_VALUE);
            boolean topicExists = adminClient.listTopics().names().get().contains(topic);
            if (topicExists) {
                return true;
            }

            if (topicAutoCreateFlag) {
                adminClient.createTopics(Collections.singleton(new NewTopic(topic, TOPIC_DEFAULT_PARTITION_NUMBER, TOPIC_DEFAULT_REPLICATION_FACTOR)));
                return true;
            }

            return false;
        } catch (Exception e) {
            LOG.error("Cannot acquire list of topics for servers: [{}]", servers, e);
            throw new RuntimeException(String.format("Cannot acquire list of topics for server: [%s]", servers), e.getCause());
        }
    }
}
