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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Optional;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaTimestampType;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.nexmark.HoloStreamEventDecoder.EventType;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.joda.time.Instant;
import org.junit.Test;

/** Golden and failure-path tests for the HoloStream MUS Kafka contract. */
public class HoloStreamEventDecoderTest {
  private static byte[] hex(String value) {
    byte[] result = new byte[value.length() / 2];
    for (int i = 0; i < result.length; i++) {
      result[i] = (byte) Integer.parseInt(value.substring(i * 2, i * 2 + 2), 16);
    }
    return result;
  }

  @Test
  public void decodesGoAuctionFixtureAndConvertsNanoseconds() throws Exception {
    HoloStreamEventDecoder.DecodedEvent decoded =
        HoloStreamEventDecoder.decode(
            HoloStreamEventDecoder.EventType.AUCTION,
            hex("d00f094b4d4c5845544c554a0b4a544c4144574354515142ca01dc0480897a80a98ea68d1dd00f140000"));

    Event event = decoded.getEvent();
    assertTrue(event.newAuction != null);
    assertEquals(1L, decoded.getEventTimestampMs());
    assertEquals(1L, event.newAuction.dateTime);
    assertEquals(0L, decoded.getTupleTimestampNs());
  }

  @Test
  public void decodesGoBidFixture() throws Exception {
    HoloStreamEventDecoder.DecodedEvent decoded =
        HoloStreamEventDecoder.decode(
            HoloStreamEventDecoder.EventType.BID, hex("d40fd00f8acf9b1f80a4e8030000"));
    assertTrue(decoded.getEvent().bid != null);
  }

  @Test
  public void kafkaDeserializerDecodesValueOnceAtSourceBoundary() {
    HoloStreamEventDeserializer deserializer = new HoloStreamEventDeserializer();
    deserializer.configure(
        Collections.singletonMap(
            HoloStreamEventDeserializer.EVENT_TYPE_CONFIG, EventType.BID.name()),
        false);

    Event event =
        deserializer.deserialize("nexmark-bid", hex("d40fd00f8acf9b1f80a4e8030000"));
    assertTrue(event.bid != null);
  }

  @Test
  public void kafkaKeyDeserializerNormalizesNullKey() {
    HoloStreamKafkaKeyDeserializer deserializer = new HoloStreamKafkaKeyDeserializer();
    deserializer.configure(Collections.emptyMap(), true);

    assertArrayEquals(new byte[0], deserializer.deserialize("nexmark-bid", null));
    assertArrayEquals(
        new byte[] {1, 2, 3},
        deserializer.deserialize("nexmark-bid", new byte[] {1, 2, 3}));
  }

  @Test(expected = SerializationException.class)
  public void kafkaDeserializerRejectsWrongTupleType() {
    HoloStreamEventDeserializer deserializer = new HoloStreamEventDeserializer();
    deserializer.configure(
        Collections.singletonMap(
            HoloStreamEventDeserializer.EVENT_TYPE_CONFIG, EventType.BID.name()),
        false);

    deserializer.deserialize(
        "nexmark-bid",
        hex(
            "d00f094b4d4c5845544c554a0b4a544c4144574354515142ca01dc0480897a80a98ea68d1dd00f140000"));
  }

  @Test
  public void timestampPolicyUsesAlreadyDeserializedEvent() {
    Event event = new Event(new Bid(1L, 2L, 3L, 1234L, ""));
    KafkaRecord<byte[], Event> record =
        new KafkaRecord<>(
            "nexmark-bid",
            2,
            7L,
            9999L,
            KafkaTimestampType.LOG_APPEND_TIME,
            null,
            null,
            event);
    TimestampPolicy<byte[], Event> policy =
        new HoloStreamTimestampPolicyFactory(EventType.BID)
            .createTimestampPolicy(new TopicPartition("nexmark-bid", 2), Optional.empty());

    assertEquals(new Instant(1234L), policy.getTimestampForRecord(null, record));
    assertEquals(new Instant(1234L), policy.getWatermark(null));
    assertSame(event, record.getKV().getValue());
  }

  @Test(expected = IllegalArgumentException.class)
  public void timestampPolicyRejectsEventFromWrongTopicType() {
    Event event = new Event(new Bid(1L, 2L, 3L, 1234L, ""));
    KafkaRecord<byte[], Event> record =
        new KafkaRecord<>(
            "nexmark-auction",
            0,
            0L,
            9999L,
            KafkaTimestampType.LOG_APPEND_TIME,
            null,
            null,
            event);
    TimestampPolicy<byte[], Event> policy =
        new HoloStreamTimestampPolicyFactory(EventType.AUCTION)
            .createTimestampPolicy(new TopicPartition("nexmark-auction", 0), Optional.empty());

    policy.getTimestampForRecord(null, record);
  }

  @Test(expected = java.io.IOException.class)
  public void rejectsTrailingBytes() throws Exception {
    HoloStreamEventDecoder.decode(
        HoloStreamEventDecoder.EventType.BID, hex("d40fd00f8acf9b1f80a4e803000001"));
  }
}
