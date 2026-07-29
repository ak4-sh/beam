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
import org.apache.beam.sdk.nexmark.HoloStreamEventDecoder.EventType;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Instant;

/** Assigns Beam event time from a deserialized HoloStream event, not Kafka append time. */
public final class HoloStreamTimestampPolicyFactory
    implements TimestampPolicyFactory<byte[], Event>, Serializable {
  private final EventType eventType;

  public HoloStreamTimestampPolicyFactory(EventType eventType) {
    this.eventType = eventType;
  }

  @Override
  public TimestampPolicy<byte[], Event> createTimestampPolicy(
      TopicPartition topicPartition, Optional<Instant> previousWatermark) {
    return new TimestampPolicy<byte[], Event>() {
      private Instant watermark =
          previousWatermark.orElse(
              org.apache.beam.sdk.transforms.windowing.BoundedWindow.TIMESTAMP_MIN_VALUE);

      @Override
      public Instant getTimestampForRecord(
          PartitionContext context, KafkaRecord<byte[], Event> record) {
        Event event = record.getKV().getValue();
        long timestampMs;
        switch (eventType) {
          case AUCTION:
            if (event == null || event.newAuction == null) {
              throw wrongEventType(record);
            }
            timestampMs = event.newAuction.dateTime;
            break;
          case BID:
            if (event == null || event.bid == null) {
              throw wrongEventType(record);
            }
            timestampMs = event.bid.dateTime;
            break;
          case PERSON:
            if (event == null || event.newPerson == null) {
              throw wrongEventType(record);
            }
            timestampMs = event.newPerson.dateTime;
            break;
          default:
            throw new IllegalArgumentException("Unsupported HoloStream event type " + eventType);
        }
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

      private IllegalArgumentException wrongEventType(KafkaRecord<byte[], Event> record) {
        return new IllegalArgumentException(
            "Expected HoloStream "
                + eventType
                + " at "
                + record.getTopic()
                + "-"
                + record.getPartition()
                + "@"
                + record.getOffset());
      }
    };
  }
}
