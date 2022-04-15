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
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;

public class AvroReflectEncoder<T> implements Serializer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroReflectEncoder.class);
    public static final String SERIALIZE_ERROR_MESSAGE = "Can't serialize record [%s] by schema [%s]";

    private final Schema schema;
    private final ReflectData reflectData = new ReflectData();

    public AvroReflectEncoder(Schema schema) {
        this.schema = schema;
    }

    @Override
    public byte[] serialize(String topic, T data) {
        try (DataFileWriter<T> dataFileWriter = new DataFileWriter<T>(reflectData.createDatumWriter(schema))) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            dataFileWriter.create(schema, outputStream);
            dataFileWriter.append(data);
            dataFileWriter.flush();
            return outputStream.toByteArray();
        } catch (Exception e) {
            String message = String.format(SERIALIZE_ERROR_MESSAGE, data, schema);
            LOG.error(message, e);
            throw new RuntimeException(message, e);
        }
    }
}
