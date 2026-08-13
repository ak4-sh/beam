/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.nexmark;

import java.io.Serializable;
import java.util.Optional;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Instant;

/** Assigns Beam event time from the already-decoded HoloStream event. */
public final class HoloStreamTimestampPolicyFactory
    implements TimestampPolicyFactory<byte[], Event>, Serializable {

  @Override
  public TimestampPolicy<byte[], Event> createTimestampPolicy(
      TopicPartition topicPartition, Optional<Instant> previousWatermark) {
    return new TimestampPolicy<byte[], Event>() {
      private Instant watermark =
          previousWatermark.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE);

      @Override
      public Instant getTimestampForRecord(
          PartitionContext context, KafkaRecord<byte[], Event> record) {
        Event event = record.getKV().getValue();
        long timestampMs = eventTimestampMs(event, record);
        Instant timestamp = new Instant(timestampMs);
        if (timestamp.isAfter(watermark)) {
          watermark = timestamp;
        }
        return timestamp;
      }

      @Override
      public Instant getWatermark(PartitionContext context) {
        return watermark;
      }
    };
  }

  private static long eventTimestampMs(Event event, KafkaRecord<byte[], Event> record) {
    if (event != null && event.newAuction != null) {
      return event.newAuction.dateTime;
    }
    if (event != null && event.bid != null) {
      return event.bid.dateTime;
    }
    if (event != null && event.newPerson != null) {
      return event.newPerson.dateTime;
    }
    throw new IllegalArgumentException(
        "Missing HoloStream Nexmark value at "
            + record.getTopic()
            + "-"
            + record.getPartition()
            + "@"
            + record.getOffset());
  }
}
