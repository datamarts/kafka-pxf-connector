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
package org.greenplum.pxf.plugins.kafka.codec;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.List;

public class AvroListEncoder extends SpecificDataConfigurer implements Serializer<List<GenericRecord>> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroListEncoder.class);
    private static final String SERIALIZATION_ERROR_MESSAGE = "AVRO serialization error";

    private final Schema schema;

    public AvroListEncoder(Schema schema) {
        this.schema = schema;
    }

    @Override
    public byte[] serialize(String topic, List<GenericRecord> data) {
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            writer.create(schema, baos);
            for (GenericRecord record : data) {
                writer.append(record);
            }
            writer.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            LOG.error(SERIALIZATION_ERROR_MESSAGE, e);
            throw new RuntimeException(SERIALIZATION_ERROR_MESSAGE, e);
        }
    }
}
