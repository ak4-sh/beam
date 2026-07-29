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

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Kafka key deserializer which makes HoloStream's intentionally absent keys coder-safe.
 *
 * <p>Beam's {@code KafkaRecordCoder} delegates to {@code ByteArrayCoder}, which rejects null
 * arrays when Nemo serializes records between the source and the first transform. The adapter
 * never uses the key, so an empty byte array is the non-null representation needed at this
 * boundary.
 */
public final class HoloStreamKafkaKeyDeserializer implements Deserializer<byte[]> {
  private static final byte[] EMPTY_KEY = new byte[0];

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    if (!isKey) {
      throw new IllegalArgumentException("HoloStreamKafkaKeyDeserializer is key-only");
    }
  }

  @Override
  public byte[] deserialize(String topic, byte[] payload) {
    return payload == null ? EMPTY_KEY : payload;
  }

  @Override
  public void close() {}
}
