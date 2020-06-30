package org.apache.pinot.controller.api.resources;

import java.io.File;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.spi.crypt.NoOpPinotCrypter;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PinotSegmentUploadDownloadRestletResourceTest {

  private static final String TABLE_NAME = "table_abc";
  private static final String SEGMENT_NAME = "segment_xyz";

  private PinotSegmentUploadDownloadRestletResource _resource = new PinotSegmentUploadDownloadRestletResource();
  private File _encryptedFile;
  private File _decryptedFile;

  @BeforeClass
  public void setup()
      throws Exception {

    // create temp files
    _encryptedFile = File.createTempFile("segment", ".enc");
    _decryptedFile = File.createTempFile("segment", ".dec");
    _encryptedFile.deleteOnExit();
    _decryptedFile.deleteOnExit();

    // configure pinot crypter
    Configuration conf = new PropertiesConfiguration();
    conf.addProperty("class.nooppinotcrypter", NoOpPinotCrypter.class.getName());
    PinotCrypterFactory.init(conf);
  }

  @Test
  public void testEncryptSegmentIfNeeded_crypterInTableConfig() {

    // arrange
    boolean uploadedSegmentIsEncrypted = false;
    String crypterClassNameInTableConfig = "NoOpPinotCrypter";
    String crypterClassNameUsedInUploadedSegment = null;

    // act
    Pair<String, File> encryptionInfo = _resource
        .encryptSegmentIfNeeded(_decryptedFile, _encryptedFile, uploadedSegmentIsEncrypted,
            crypterClassNameUsedInUploadedSegment, crypterClassNameInTableConfig, SEGMENT_NAME, TABLE_NAME);

    // assert
    assertEquals("NoOpPinotCrypter", encryptionInfo.getLeft());
    assertEquals(_encryptedFile, encryptionInfo.getRight());
  }

  @Test
  public void testEncryptSegmentIfNeeded_uploadedSegmentIsEncrypted() {

    // arrange
    boolean uploadedSegmentIsEncrypted = true;
    String crypterClassNameInTableConfig = "NoOpPinotCrypter";
    String crypterClassNameUsedInUploadedSegment = "NoOpPinotCrypter";

    // act
    Pair<String, File> encryptionInfo = _resource
        .encryptSegmentIfNeeded(_decryptedFile, _encryptedFile, uploadedSegmentIsEncrypted,
            crypterClassNameUsedInUploadedSegment, crypterClassNameInTableConfig, SEGMENT_NAME, TABLE_NAME);

    // assert
    assertEquals("NoOpPinotCrypter", encryptionInfo.getLeft());
    assertEquals(_encryptedFile, encryptionInfo.getRight());
  }

  @Test(expectedExceptions = ControllerApplicationException.class, expectedExceptionsMessageRegExp = "Uploaded segment"
      + " is encrypted with 'FancyCrypter' while table config requires 'NoOpPinotCrypter' as crypter .*")
  public void testEncryptSegmentIfNeeded_differentCrypters() {

    // arrange
    boolean uploadedSegmentIsEncrypted = true;
    String crypterClassNameInTableConfig = "NoOpPinotCrypter";
    String crypterClassNameUsedInUploadedSegment = "FancyCrypter";

    // act
    _resource.encryptSegmentIfNeeded(_decryptedFile, _encryptedFile, uploadedSegmentIsEncrypted,
        crypterClassNameUsedInUploadedSegment, crypterClassNameInTableConfig, SEGMENT_NAME, TABLE_NAME);
  }

  @Test
  public void testEncryptSegmentIfNeeded_noEncryption() {

    // arrange
    boolean uploadedSegmentIsEncrypted = false;
    String crypterClassNameInTableConfig = null;
    String crypterClassNameUsedInUploadedSegment = null;

    // act
    Pair<String, File> encryptionInfo = _resource
        .encryptSegmentIfNeeded(_decryptedFile, _encryptedFile, uploadedSegmentIsEncrypted,
            crypterClassNameUsedInUploadedSegment, crypterClassNameInTableConfig, SEGMENT_NAME, TABLE_NAME);

    // assert
    assertNull(encryptionInfo.getLeft());
    assertEquals(_decryptedFile, encryptionInfo.getRight());
  }
}