package com.orientechnologies.orient.core.storage.cache.local.preload;

import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;

import java.io.IOException;

/**
 * Preload cache which is used underneath of {@link com.orientechnologies.orient.core.storage.cache.local.OWOWCache}
 * to prefetch several pages into the database in single batch and cache prefetched pages for future requests.
 * <p>
 * This cache usually has small size not more than 1000 pages and is used for optimization of full scan queries.
 */
public interface OPreloadCache {
  OCachePointer load(final long fileId, final OFileClassic fileClassic, final long startPageIndex, final int pageCount,
      final boolean addNewPages, final OModifiableBoolean cacheHit) throws IOException;

  void close();
}
