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

import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.nexmark.HoloStreamEventDecoder.EventType;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/** Topic-aware Kafka value deserializer for HoloStream MUS-encoded Nexmark events. */
public final class HoloStreamEventDeserializer implements Deserializer<Event> {
  public static final String AUCTION_TOPIC_CONFIG = "beam.nexmark.holostream.auction.topic";
  public static final String BID_TOPIC_CONFIG = "beam.nexmark.holostream.bid.topic";

  private String auctionTopic;
  private String bidTopic;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    if (isKey) {
      throw new IllegalArgumentException("HoloStreamEventDeserializer is value-only");
    }
    auctionTopic = requiredString(configs, AUCTION_TOPIC_CONFIG);
    bidTopic = requiredString(configs, BID_TOPIC_CONFIG);
    if (auctionTopic.equals(bidTopic)) {
      throw new IllegalArgumentException("HoloStream Auction and Bid topics must be different");
    }
  }

  @Override
  public Event deserialize(String topic, byte[] payload) {
    EventType eventType = eventTypeForTopic(topic);
    try {
      return HoloStreamEventDecoder.decode(eventType, payload).getEvent();
    } catch (IOException error) {
      throw new SerializationException(
          "Failed to decode HoloStream " + eventType + " value from topic " + topic, error);
    }
  }

  private EventType eventTypeForTopic(String topic) {
    if (auctionTopic == null || bidTopic == null) {
      throw new SerializationException(
          "HoloStreamEventDeserializer was not configured before use");
    }
    if (auctionTopic.equals(topic)) {
      return EventType.AUCTION;
    }
    if (bidTopic.equals(topic)) {
      return EventType.BID;
    }
    throw new SerializationException("Unexpected HoloStream topic " + topic);
  }

  private static String requiredString(Map<String, ?> configs, String name) {
    Object value = configs.get(name);
    if (value == null || value.toString().trim().isEmpty()) {
      throw new IllegalArgumentException("Missing Kafka deserializer config " + name);
    }
    return value.toString();
  }

  @Override
  public void close() {}
}
