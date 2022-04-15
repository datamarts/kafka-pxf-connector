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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.greenplum.pxf.plugins.kafka.codec.AvroListEncoder;
import org.greenplum.pxf.plugins.kafka.codec.AvroReflectEncoder;
import org.greenplum.pxf.plugins.kafka.model.KafkaMessageKey;

import java.util.List;
import java.util.Map;

public class KafkaProducerSupplier {
    public KafkaProducer<KafkaMessageKey, List<GenericRecord>> produce(Map<String, Object> kafkaProps, Schema schema) {
        return new KafkaProducer<>(kafkaProps, new AvroReflectEncoder<>(KafkaMessageKey.getSchema()), new AvroListEncoder(schema));
    }
}
