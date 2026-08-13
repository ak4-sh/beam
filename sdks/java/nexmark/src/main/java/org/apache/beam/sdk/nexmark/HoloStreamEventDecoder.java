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

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Person;

/** Decodes the MUS tuple wire format emitted by HoloStream's KafkaCollector. */
public final class HoloStreamEventDecoder {
  /** HoloStream tuple type carried by a Kafka topic. */
  public enum EventType {
    PERSON,
    AUCTION,
    BID
  }

  /** Beam event plus source metadata encoded in the HoloStream tuple. */
  public static final class DecodedEvent {
    private final Event event;
    private final long eventTimestampMs;
    private final long tupleTimestampNs;

    private DecodedEvent(Event event, long eventTimestampNs, long tupleTimestampNs) {
      this.event = event;
      this.eventTimestampMs = nanosToMillis(eventTimestampNs);
      this.tupleTimestampNs = tupleTimestampNs;
    }

    public Event getEvent() {
      return event;
    }

    public long getEventTimestampMs() {
      return eventTimestampMs;
    }

    public long getTupleTimestampNs() {
      return tupleTimestampNs;
    }
  }

  private HoloStreamEventDecoder() {}

  public static DecodedEvent decode(EventType type, byte[] payload) throws IOException {
    if (payload == null) {
      throw new IOException("null HoloStream payload");
    }
    ByteArrayInputStream input = new ByteArrayInputStream(payload);
    Event event;
    long eventTimestampNs;
    switch (type) {
      case AUCTION:
        long id = readSignedLong(input);
        String itemName = readString(input);
        String description = readString(input);
        long initialBid = readSignedLong(input);
        long reserve = readSignedLong(input);
        long dateTimeNs = readSignedLong(input);
        long expiresNs = readSignedLong(input);
        long seller = readSignedLong(input);
        long category = readSignedLong(input);
        String auctionExtra = readString(input);
        long dateTimeMs = nanosToMillis(dateTimeNs);
        long expiresMs = nanosToMillis(expiresNs);
        if (expiresMs < dateTimeMs) {
          throw new IOException("auction expires before it starts");
        }
        event =
            new Event(
                new Auction(
                    id,
                    itemName,
                    description,
                    initialBid,
                    reserve,
                    dateTimeMs,
                    expiresMs,
                    seller,
                    category,
                    auctionExtra));
        eventTimestampNs = dateTimeNs;
        break;
      case BID:
        long auction = readSignedLong(input);
        long bidder = readSignedLong(input);
        long price = readSignedLong(input);
        long bidDateTimeNs = readSignedLong(input);
        String bidExtra = readString(input);
        event =
            new Event(
                new Bid(
                    auction,
                    bidder,
                    price,
                    nanosToMillis(bidDateTimeNs),
                    bidExtra));
        eventTimestampNs = bidDateTimeNs;
        break;
      case PERSON:
        long personId = readSignedLong(input);
        String name = readString(input);
        String email = readString(input);
        String creditCard = readString(input);
        String city = readString(input);
        String state = readString(input);
        long personDateTimeNs = readSignedLong(input);
        String personExtra = readString(input);
        event =
            new Event(
                new Person(
                    personId,
                    name,
                    email,
                    creditCard,
                    city,
                    state,
                    nanosToMillis(personDateTimeNs),
                    personExtra));
        eventTimestampNs = personDateTimeNs;
        break;
      default:
        throw new IOException("unsupported HoloStream event type " + type);
    }

    // KafkaCollector serializes the tuple's internal timestamp after all model fields.
    // Nexmark event time comes from the model field above.
    long tupleTimestampNs = readSignedLong(input);
    if (input.available() != 0) {
      throw new IOException("trailing bytes after HoloStream tuple: " + input.available());
    }
    return new DecodedEvent(event, eventTimestampNs, tupleTimestampNs);
  }

  private static long nanosToMillis(long nanos) {
    return TimeUnit.NANOSECONDS.toMillis(nanos);
  }

  private static long readSignedLong(ByteArrayInputStream input) throws IOException {
    long encoded = readUnsignedLong(input);
    return (encoded >>> 1) ^ -(encoded & 1L);
  }

  private static String readString(ByteArrayInputStream input) throws IOException {
    long encodedLength = readUnsignedLong(input);
    if (encodedLength > Integer.MAX_VALUE || encodedLength > input.available()) {
      throw new IOException("invalid HoloStream string length " + encodedLength);
    }
    byte[] bytes = new byte[(int) encodedLength];
    int read = input.read(bytes, 0, bytes.length);
    if (read != bytes.length) {
      throw new EOFException("truncated HoloStream string");
    }
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static long readUnsignedLong(ByteArrayInputStream input) throws IOException {
    long result = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      int next = input.read();
      if (next < 0) {
        throw new EOFException("truncated HoloStream varint");
      }
      long bits = next & 0x7fL;
      if (shift == 63 && bits > 1) {
        throw new IOException("HoloStream varint overflow");
      }
      result |= bits << shift;
      if ((next & 0x80) == 0) {
        return result;
      }
    }
    throw new IOException("unterminated HoloStream varint");
  }
}
