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
package org.greenplum.pxf.plugins.kafka.model;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

import java.io.Serializable;
import java.util.stream.Collectors;

public class KafkaMessageKey implements Serializable {
    private final String tableName;
    private final int streamNumber;
    private final int streamTotal;
    private int chunkNumber;
    private boolean isLastChunk;

    public KafkaMessageKey(String tableName, int streamNumber, int streamTotal, int chunkNumber, boolean isLastChunk) {
        this.tableName = tableName;
        this.streamNumber = streamNumber;
        this.streamTotal = streamTotal;
        this.chunkNumber = chunkNumber;
        this.isLastChunk = isLastChunk;
    }

    public static Schema getSchema() {
        Schema schema = Schema.createRecord("key", null, null, false);
        schema.setFields(ReflectData.get().getSchema(KafkaMessageKey.class).getFields().stream()
                .map(f -> new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal())).collect(Collectors.toList()));
        return schema;
    }

    public String getTableName() {
        return tableName;
    }

    public int getStreamNumber() {
        return streamNumber;
    }

    public int getStreamTotal() {
        return streamTotal;
    }

    public int getChunkNumber() {
        return chunkNumber;
    }

    public void setChunkNumber(int chunkNumber) {
        this.chunkNumber = chunkNumber;
    }

    public boolean isLastChunk() {
        return isLastChunk;
    }

    public void setLastChunk(boolean lastChunk) {
        isLastChunk = lastChunk;
    }
}
