package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 8/17/2015
 */
public class OWALPageChangesLogCollector implements OWALChanges {
  private static final int PAGE_SIZE = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024 + 2 * OWOWCache.PAGE_PADDING;

  public static final int    CHUNK_SIZE     = 64;
  public static final int    LOG_START_SIZE = 128;
  private             byte[] logBytes       = new byte[LOG_START_SIZE];
  private             int    logCursor      = 0;
  private       byte[][] pageChunks;
  private final int      pageSize;

  public OWALPageChangesLogCollector() {
    this(PAGE_SIZE);
  }

  public OWALPageChangesLogCollector(int pageSize) {
    this.pageSize = pageSize;
  }

  public void setLongValue(ODirectMemoryPointer pointer, int offset, long value) {
    byte[] data = new byte[OLongSerializer.LONG_SIZE];
    OLongSerializer.INSTANCE.serializeNative(value, data, 0);

    updateData(pointer, offset, data);
  }

  public void setIntValue(ODirectMemoryPointer pointer, int offset, int value) {
    byte[] data = new byte[OIntegerSerializer.INT_SIZE];
    OIntegerSerializer.INSTANCE.serializeNative(value, data, 0);

    updateData(pointer, offset, data);
  }

  public void setShortValue(ODirectMemoryPointer pointer, int offset, short value) {
    byte[] data = new byte[OShortSerializer.SHORT_SIZE];
    OShortSerializer.INSTANCE.serializeNative(value, data, 0);

    updateData(pointer, offset, data);
  }

  public void setByteValue(ODirectMemoryPointer pointer, int offset, byte value) {
    byte[] data = new byte[] { value };

    updateData(pointer, offset, data);
  }

  public void setBinaryValue(ODirectMemoryPointer pointer, int offset, byte[] value) {
    updateData(pointer, offset, value);
  }

  public void moveData(ODirectMemoryPointer pointer, int from, int to, int len) {
    byte[] buff = new byte[len];
    readData(pointer, from, buff);
    updateData(pointer, to, buff);
  }

  public long getLongValue(ODirectMemoryPointer pointer, int offset) {
    byte[] data = new byte[OLongSerializer.LONG_SIZE];

    readData(pointer, offset, data);

    return OLongSerializer.INSTANCE.deserializeNative(data, 0);
  }

  public int getIntValue(ODirectMemoryPointer pointer, int offset) {
    byte[] data = new byte[OIntegerSerializer.INT_SIZE];

    readData(pointer, offset, data);

    return OIntegerSerializer.INSTANCE.deserializeNative(data, 0);
  }

  public short getShortValue(ODirectMemoryPointer pointer, int offset) {
    byte[] data = new byte[OShortSerializer.SHORT_SIZE];

    readData(pointer, offset, data);

    return OShortSerializer.INSTANCE.deserializeNative(data, 0);
  }

  public byte getByteValue(ODirectMemoryPointer pointer, int offset) {
    byte[] data = new byte[1];

    readData(pointer, offset, data);

    return data[0];
  }

  public byte[] getBinaryValue(ODirectMemoryPointer pointer, int offset, int len) {
    byte[] data = new byte[len];
    readData(pointer, offset, data);

    return data;
  }

  public void applyChanges(ODirectMemoryPointer pointer) {
    if (pageChunks == null)
      return;
    for (int i = 0; i < pageChunks.length; i++) {
      byte[] chunk = pageChunks[i];
      if (chunk != null) {
        if (i < pageChunks.length - 1) {
          pointer.set(((long) i) * CHUNK_SIZE, chunk, 0, chunk.length);
        } else {
          final int wl = Math.min(chunk.length, pageSize - (pageChunks.length - 1) * CHUNK_SIZE);
          pointer.set(((long) i) * CHUNK_SIZE, chunk, 0, wl);
        }
      }
    }
  }

  public PointerWrapper wrap(ODirectMemoryPointer pointer) {
    return new PointerWrapper(this, pointer);
  }

  public int serializedSize() {
    return logCursor + OIntegerSerializer.INT_SIZE;
  }

  public int toStream(int offset, byte[] stream) {
    OIntegerSerializer.INSTANCE.serializeLiteral(logCursor, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;
    System.arraycopy(logBytes, 0, stream, offset, logCursor);
    return offset;
  }

  public int fromStream(int offset, byte[] stream) {
    int logSize = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, offset);
    logSize += offset;
    int readChunk = offset;
    /*
    while (readChunk < logSize) {
      int pos = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, readChunk);
      readChunk += OIntegerSerializer.INT_SIZE;
      int size = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, readChunk);
      readChunk += OIntegerSerializer.INT_SIZE;
      byte[] change = new byte[size];
      applyChanges();
    }*/

    return logSize + OIntegerSerializer.INT_SIZE;
  }

  private void readData(ODirectMemoryPointer pointer, int offset, byte[] data) {
    if (pageChunks == null) {
      if(pointer != null)
        pointer.get(offset, data, 0, data.length);
      return;
    }
    int chunkIndex = offset / CHUNK_SIZE;
    int chunkOffset = offset - chunkIndex * CHUNK_SIZE;

    int read = 0;

    while (read < data.length) {
      byte[] chunk = pageChunks[chunkIndex];

      final int rl = Math.min(CHUNK_SIZE - chunkOffset, data.length - read);
      if (chunk == null) {
        if (pointer != null) {
          if (chunkIndex < pageChunks.length - 1) {
            pointer.get(((long) chunkIndex * CHUNK_SIZE)+ chunkOffset, data, read, rl);
          } else {
            final int chunkSize = Math.min(CHUNK_SIZE, pageSize - (pageChunks.length - 1) * CHUNK_SIZE);

            assert chunkSize <= CHUNK_SIZE;
            assert chunkSize > 0;

            final int toRead = Math.min(rl, chunkSize);
            pointer.get(((long) chunkIndex * CHUNK_SIZE) + chunkOffset, data, read, toRead);
          }
        }
      } else
        System.arraycopy(chunk, chunkOffset, data, read, rl);

      read += rl;
      chunkOffset = 0;
      chunkIndex++;
    }
  }

  private void updateData(ODirectMemoryPointer pointer, int offset, byte[] data) {
    updateChunks(pointer, offset, data);
    logChange(offset, data);
  }

  private void logChange(int offset, byte[] data) {
    int requiredSize = data.length + OIntegerSerializer.INT_SIZE + OIntegerSerializer.INT_SIZE;
    if (requiredSize > (logBytes.length - logCursor)) {
      int newSize = logBytes.length;
      do {
        newSize *= 2;
      } while (requiredSize > newSize - logCursor);
      byte newBytes[] = new byte[newSize];
      System.arraycopy(logBytes, 0, newBytes, 0, logCursor);
      logBytes = newBytes;
    }
    OIntegerSerializer.INSTANCE.serializeLiteral(offset, logBytes, logCursor);
    logCursor += OIntegerSerializer.INT_SIZE;
    OIntegerSerializer.INSTANCE.serializeLiteral(data.length, logBytes, logCursor);
    logCursor += OIntegerSerializer.INT_SIZE;
    System.arraycopy(data, 0, logBytes, logCursor, data.length);
    logCursor += data.length;
  }

  private void updateChunks(ODirectMemoryPointer pointer, int offset, byte[] data) {
    if (pageChunks == null) {
      pageChunks = new byte[(pageSize + (CHUNK_SIZE - 1)) / CHUNK_SIZE][];
    }
    int chunkIndex = offset / CHUNK_SIZE;
    int chunkOffset = offset - chunkIndex * CHUNK_SIZE;

    int written = 0;

    while (written < data.length) {
      byte[] chunk = pageChunks[chunkIndex];

      if (chunk == null) {
        if (pointer != null) {
          if (chunkIndex < pageChunks.length - 1)
            chunk = pointer.get(((long) chunkIndex) * CHUNK_SIZE, CHUNK_SIZE);
          else {
            final int chunkSize = Math.min(CHUNK_SIZE, pageSize - (pageChunks.length - 1) * CHUNK_SIZE);
            chunk = new byte[CHUNK_SIZE];

            assert chunkSize <= CHUNK_SIZE;
            assert chunkSize > 0;

            System.arraycopy(pointer.get(((long) chunkIndex * CHUNK_SIZE), chunkSize), 0, chunk, 0, chunkSize);
          }
        } else {
          chunk = new byte[CHUNK_SIZE];
        }

        pageChunks[chunkIndex] = chunk;
      }

      final int wl = Math.min(CHUNK_SIZE - chunkOffset, data.length - written);
      System.arraycopy(data, written, chunk, chunkOffset, wl);

      written += wl;
      chunkOffset = 0;
      chunkIndex++;
    }
  }
}
