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

/** Kafka value deserializer for HoloStream MUS-encoded Nexmark events. */
public final class HoloStreamEventDeserializer implements Deserializer<Event> {
  public static final String EVENT_TYPE_CONFIG = "beam.nexmark.holostream.event.type";

  private EventType eventType;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    if (isKey) {
      throw new IllegalArgumentException("HoloStreamEventDeserializer is value-only");
    }
    Object configuredType = configs.get(EVENT_TYPE_CONFIG);
    if (configuredType == null) {
      throw new IllegalArgumentException("Missing Kafka deserializer config " + EVENT_TYPE_CONFIG);
    }
    try {
      eventType = EventType.valueOf(configuredType.toString());
    } catch (IllegalArgumentException error) {
      throw new IllegalArgumentException(
          "Invalid " + EVENT_TYPE_CONFIG + " value " + configuredType, error);
    }
  }

  @Override
  public Event deserialize(String topic, byte[] payload) {
    if (eventType == null) {
      throw new SerializationException(
          "HoloStreamEventDeserializer was not configured before use");
    }
    try {
      return HoloStreamEventDecoder.decode(eventType, payload).getEvent();
    } catch (IOException error) {
      throw new SerializationException(
          "Failed to decode HoloStream " + eventType + " value from topic " + topic, error);
    }
  }

  @Override
  public void close() {}
}
