package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.orient.core.Orient;
import com.sun.corba.se.pept.transport.ByteBufferPool;
import org.junit.Before;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 8/19/2015
 */
@Test
public class OWALPageChangesCollectorTest {

  @BeforeMethod
  public void before() {
    Orient.instance();
  }

  public void testSingleLongValueInStartChunk() {
    byte[] data = new byte[128];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesCollector changesCollector = new OWALPageChangesCollector(128);
    changesCollector.setLongValue(pointer, 42, 64);
    Assert.assertEquals(changesCollector.getLongValue(pointer, 64), 42);

  }

  public void testSingleLongValuesInMiddleOfChunk() {
    byte[] data = new byte[128];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesCollector changesCollector = new OWALPageChangesCollector(128);
    changesCollector.setLongValue(pointer, 42, 60);
    Assert.assertEquals(changesCollector.getLongValue(pointer, 60), 42);

  }

  public void testSingleIntValue() {
    byte[] data = new byte[128];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesCollector changesCollector = new OWALPageChangesCollector(128);
    changesCollector.setIntValue(pointer, 42, 64);
    Assert.assertEquals(changesCollector.getIntValue(pointer, 64), 42);

  }

  public void testSingleShortValue() {
    byte[] data = new byte[128];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesCollector changesCollector = new OWALPageChangesCollector(128);
    changesCollector.setShortValue(pointer,(short) 42 ,64 );
    Assert.assertEquals(changesCollector.getShortValue(pointer, 64), 42);

  }

  public void testSingleByteValue() {
    byte[] data = new byte[128];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesCollector changesCollector = new OWALPageChangesCollector(128);
    changesCollector.setByteValue(pointer, (byte) 42, 64);
    Assert.assertEquals(changesCollector.getByteValue(pointer, 64), 42);

  }

  public void testMoveData() {
    byte[] data = new byte[128];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesCollector changesCollector = new OWALPageChangesCollector(128);
    byte[] values = new byte[] { 1, 2, 3, 4 };

    changesCollector.setBinaryValue(pointer, values, 64);
    changesCollector.moveData(pointer, 64, 74, 4);
    Assert.assertEquals(changesCollector.getBinaryValue(pointer, 64, 4), values);

  }


  public void testBinaryValueTwoChunksFromStart() {
    byte[] data = new byte[256];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesCollector changesCollector = new OWALPageChangesCollector(256);
    byte[] changes = new byte[128];

    Random random = new Random();
    random.nextBytes(changes);

    changesCollector.setBinaryValue(pointer, changes, 64);

    Assert.assertEquals(changesCollector.getBinaryValue(pointer, 64, 128), changes);

  }

  public void testBinaryValueTwoChunksInMiddle() {
    byte[] data = new byte[256];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesCollector changesCollector = new OWALPageChangesCollector(256);
    byte[] changes = new byte[128];

    Random random = new Random();
    random.nextBytes(changes);

    changesCollector.setBinaryValue(pointer, changes, 32);

    Assert.assertEquals(changesCollector.getBinaryValue(pointer, 32, 128), changes);

  }
}
