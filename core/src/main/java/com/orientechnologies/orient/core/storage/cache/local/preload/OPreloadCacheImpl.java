package com.orientechnologies.orient.core.storage.cache.local.preload;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OStoragePerformanceStatistic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class OPreloadCacheImpl implements OPreloadCache {
  private final OWriteAheadLog               writeAheadLog;
  private final int                          pageSize;
  private final OStoragePerformanceStatistic storagePerformanceStatistic;
  private final OByteBufferPool              bufferPool;

  private final int batchSize;
  private final int maxCacheSize;

  private final AtomicLong cacheSize = new AtomicLong();

  private final AtomicBoolean purgeInProgress = new AtomicBoolean();

  private final ConcurrentHashMap<PageKey, CacheEntry> preloadedEntries = new ConcurrentHashMap<PageKey, CacheEntry>();
  private final ConcurrentLinkedQueue<PageKey>         entriesQueue     = new ConcurrentLinkedQueue<PageKey>();

  public OPreloadCacheImpl(final OWriteAheadLog writeAheadLog, final int pageSize,
      final OStoragePerformanceStatistic storagePerformanceStatistic, final OByteBufferPool bufferPool, final int batchSize,
      int maxCacheSize) {
    this.writeAheadLog = writeAheadLog;
    this.pageSize = pageSize;
    this.storagePerformanceStatistic = storagePerformanceStatistic;
    this.bufferPool = bufferPool;
    this.batchSize = batchSize;
    this.maxCacheSize = maxCacheSize;
  }

  @Override
  public OCachePointer load(final long fileId, final OFileClassic fileClassic, final long startPageIndex, final int pageCount,
      final boolean addNewPages, final OModifiableBoolean cacheHit) throws IOException {

    final PageKey pageKey = new PageKey(fileId, startPageIndex);
    final CacheEntry cacheEntry = preloadedEntries.get(pageKey);

    if (cacheEntry != null)
      return cacheEntry.cachePointer;

    final OLogSequenceNumber lastLsn;
    if (writeAheadLog != null)
      lastLsn = writeAheadLog.getFlushedLsn();
    else
      lastLsn = new OLogSequenceNumber(-1, -1);

    final long firstPageStartPosition = startPageIndex * pageSize;
    final long firstPageEndPosition = firstPageStartPosition + pageSize;

    if (fileClassic.getFileSize() >= firstPageEndPosition) {
      final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = OSessionStoragePerformanceStatistic
          .getStatisticInstance();
      if (sessionStoragePerformanceStatistic != null) {
        sessionStoragePerformanceStatistic.startPageReadFromFileTimer();
      }
      storagePerformanceStatistic.startPageReadFromFileTimer();

      int pagesRead = 0;

      try {
        if (pageCount == 1) {
          final ByteBuffer buffer = bufferPool.acquireDirect(false);

          fileClassic.read(firstPageStartPosition, buffer);
          buffer.position(0);

          final OCachePointer dataPointer = new OCachePointer(buffer, bufferPool, lastLsn, fileId, startPageIndex);
          pagesRead = 1;

          return dataPointer;
        }

        final long maxPageCount = (fileClassic.getFileSize() - firstPageStartPosition) / pageSize;
        final int realPageCount = Math.min((int) maxPageCount, pageCount);

        final ByteBuffer[] buffers = new ByteBuffer[realPageCount];
        for (int i = 0; i < buffers.length; i++) {
          buffers[i] = bufferPool.acquireDirect(false);
          assert buffers[i].position() == 0;
        }

        final long bytesRead = fileClassic.read(firstPageStartPosition, buffers);
        assert bytesRead % pageSize == 0;

        final int buffersRead = (int) (bytesRead / pageSize);
        OCachePointer pointer = null;

        for (int n = 0; n < buffersRead; n++) {
          final OCachePointer cp = new OCachePointer(buffers[n], bufferPool, lastLsn, fileId, startPageIndex + n);
          final CacheEntry oe = preloadedEntries
              .putIfAbsent(new PageKey(fileId, startPageIndex + n), new CacheEntry(cp, System.currentTimeMillis()));

          if (oe != null) {
            bufferPool.release(buffers[n]);

            if (n == 0) {
              pointer = oe.cachePointer;
            }
          } else {
            if (n == 0) {
              pointer = cp;
            }

            cp.incrementReferrer();

            entriesQueue.offer(new PageKey(fileId, startPageIndex + n));
            final long cs = cacheSize.incrementAndGet();

            if (cs > maxCacheSize) {
              while (!purgeInProgress.compareAndSet(false, true)) {
                if (cacheSize.get() > maxCacheSize) {
                  for (int i = 0; i < batchSize; i++) {
                    final PageKey pk = entriesQueue.poll();
                    if (pk != null) {
                      final CacheEntry re = preloadedEntries.remove(pk);

                      cacheSize.decrementAndGet();
                      re.cachePointer.decrementReferrer();
                    } else
                      break;
                  }

                  purgeInProgress.set(true);
                }
              }
            }
          }

          buffers[n] = null;
        }

        for (ByteBuffer buffer : buffers) {
          if (buffer != null) {
            bufferPool.release(buffer);
          }
        }

        pagesRead += buffersRead;

        return pointer;
      } finally {
        if (sessionStoragePerformanceStatistic != null) {
          sessionStoragePerformanceStatistic.stopPageReadFromFileTimer(pagesRead);
        }

        storagePerformanceStatistic.stopPageReadFromFileTimer(pagesRead);
      }
    } else if (addNewPages) {
      final int space = (int) (firstPageEndPosition - fileClassic.getFileSize());

      if (space > 0)
        fileClassic.allocateSpace(space);

      final ByteBuffer buffer = bufferPool.acquireDirect(true);
      final OCachePointer dataPointer = new OCachePointer(buffer, bufferPool, lastLsn, fileId, startPageIndex);

      cacheHit.setValue(true);
      return dataPointer;
    } else
      return null;
  }

  Queue<PageKey> getQueue() {
    return entriesQueue;
  }

  boolean contains(final long fileId, final long pageIndex) {
    return preloadedEntries.containsKey(new PageKey(fileId, pageIndex));
  }

  @Override
  public void close() {
    for (CacheEntry entry : preloadedEntries.values()) {
      entry.cachePointer.decrementReferrer();
    }

    preloadedEntries.clear();
    entriesQueue.clear();
  }

  private static final class PageKey implements Comparable<PageKey> {
    private final long fileId;
    private final long pageIndex;

    public PageKey(long fileId, long pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public int compareTo(PageKey o) {
      if (fileId > o.fileId)
        return 1;
      if (fileId < o.fileId)
        return -1;

      if (pageIndex > o.pageIndex)
        return 1;
      if (pageIndex < o.pageIndex)
        return -1;

      return 0;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      PageKey pageKey = (PageKey) o;

      if (fileId != pageKey.fileId)
        return false;
      return pageIndex == pageKey.pageIndex;

    }

    @Override
    public int hashCode() {
      int result = (int) (fileId ^ (fileId >>> 32));
      result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
      return result;
    }
  }

  private static final class CacheEntry {
    private final OCachePointer cachePointer;
    private final long          ts;

    public CacheEntry(OCachePointer cachePointer, long ts) {
      this.cachePointer = cachePointer;
      this.ts = ts;
    }
  }
}
