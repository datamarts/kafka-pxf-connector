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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.plugins.kafka.model.KafkaMessageKey;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("RedundantThrows")
public class KafkaAccessor extends KafkaBasePlugin implements Accessor {
    private final Object monitor = new Object();
    private final AtomicLong activeTasks = new AtomicLong();
    private final AtomicLong totalTasks = new AtomicLong();
    private final AtomicLong errorCount = new AtomicLong();
    private final AtomicReference<Throwable> firstException = new AtomicReference<>(null);
    private final KafkaProducerSupplier kafkaProducerSupplier;
    private KafkaMessageKey kafkaMessageKey;
    private KafkaProducer<KafkaMessageKey, List<GenericRecord>> producer;
    private List<GenericRecord> currentBatch;
    private int chunkCounter = 0;

    public KafkaAccessor() {
        kafkaProducerSupplier = new KafkaProducerSupplier();
    }

    public KafkaAccessor(KafkaProducerSupplier kafkaProducerSupplier) {
        this.kafkaProducerSupplier = kafkaProducerSupplier;
    }

    @Override
    public boolean openForRead() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public OneRow readNextObject() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeForRead() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean openForWrite() throws Exception {
        try {
            LOG.info("Opening [{}] for write in [{}]. Segment: {}, total: {}",
                    context.getProfile(), topic, context.getSegmentId(), context.getTotalSegments());

            Schema schema = AvroUtils.createSchema(context.getTupleDescription());
            context.setMetadata(schema);
            kafkaMessageKey = new KafkaMessageKey(topic, context.getSegmentId(), context.getTotalSegments(), chunkCounter, false);
            producer = kafkaProducerSupplier.produce(kafkaProps, schema);
            currentBatch = new ArrayList<>(batchSize);
            return true;
        } catch (Exception e) {
            LOG.error("Failed opening [{}] for write in [{}]. Segment: {}, total: {}",
                    context.getProfile(), topic, context.getSegmentId(), context.getTotalSegments(), e);
            safelyClose();
            throw new IllegalStateException("Failed to open for write", e);
        }
    }

    @Override
    public boolean writeNextObject(OneRow onerow) throws Exception {
        waitUntilBufferFreed();
        currentBatch.add((GenericRecord) onerow.getData());

        if (currentBatch.size() >= batchSize) {
            writeBatch(false);
        }

        if (errorCount.get() > 0) {
            throw new IllegalStateException("Some of the tasks completed exceptionally", firstException.get());
        }

        return true;
    }

    @Override
    public void closeForWrite() throws Exception {
        try {
            writeBatch(true);

            while (activeTasks.get() > 0 && errorCount.get() == 0) {
                Thread.sleep(100L);
            }

            if (errorCount.get() > 0) {
                LOG.error("Failed [{}] for write in [{}]. Errors/total: {}/{}, segment: {}, total: {}",
                        context.getProfile(), topic, errorCount.get(), totalTasks.get(), context.getSegmentId(), context.getTotalSegments(), firstException.get());
                throw new IllegalStateException("Some of the tasks completed exceptionally", firstException.get());
            }

            LOG.info("Closing [{}] for write in [{}]. All futures complete: {}, segment: {}, total: {}",
                    context.getProfile(), topic, totalTasks.get(), context.getSegmentId(), context.getTotalSegments());
        } finally {
            safelyClose();
            LOG.info("Closed [{}] for write in [{}], segment: {}, total: {}",
                    context.getProfile(), topic, context.getSegmentId(), context.getTotalSegments());
        }
    }

    private void writeBatch(boolean last) {
        if (currentBatch.size() > 0 || last) {
            int currentChunk = chunkCounter++;
            LOG.debug("Batch write [{}] in [{}]. Chunk: {}, batchSize: {}, last: {}", context.getProfile(), topic, currentChunk, currentBatch.size(), last);
            kafkaMessageKey.setChunkNumber(currentChunk);
            kafkaMessageKey.setLastChunk(last);

            totalTasks.incrementAndGet();
            activeTasks.incrementAndGet();
            producer.send(new ProducerRecord<>(topic, kafkaMessageKey, currentBatch), (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("Failed producing task [{}] in [{}]", context.getProfile(), topic, exception);
                    firstException.compareAndSet(null, exception);
                    errorCount.incrementAndGet();
                }

                activeTasks.decrementAndGet();
                notifyIfBufferFreed();
            });
            currentBatch.clear();
        }
    }

    protected void waitUntilBufferFreed() throws InterruptedException {
        while (activeTasks.get() >= bufferSize) {
            synchronized (monitor) {
                if (activeTasks.get() >= bufferSize) {
                    monitor.wait();
                }
            }
        }
    }

    protected void notifyIfBufferFreed() {
        synchronized (monitor) {
            if (activeTasks.get() <= bufferSize / 10) {
                monitor.notify();
            }
        }
    }

    private void safelyClose() {
        if (producer != null) {
            try {
                producer.flush();
            } catch (Exception e) {
                LOG.error("Could not properly flush producer", e);
            }

            try {
                producer.close();
            } catch (Exception e) {
                LOG.error("Could not properly close producer", e);
            }
        }
    }
}
