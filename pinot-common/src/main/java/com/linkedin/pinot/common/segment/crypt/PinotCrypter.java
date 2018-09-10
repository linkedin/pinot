/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.segment.crypt;

import java.io.File;
import org.apache.commons.configuration.Configuration;


/**
 * The PinotCrypter will encrypt and decrypt segments when they are downloaded. This class is especially useful in cases
 * where segments cannot be stored unencrypted in storage.
 */
public interface PinotCrypter {

  /**
   * Initializes a crypter with any configurations it might need.
   * @param config
   */
  void init(Configuration config);

  /**
   * Encrypts the object into the file location provided. The implementation should clean up file after any failures.
   * @param decryptedObject
   * @param encryptedFile
   * @throws Exception
   */
  void encrypt(Object decryptedObject, File encryptedFile);

  /**
   * Decrypts object into file location provided. The implementation should clean up file after any failures.
   * @param encryptedObject
   * @param decryptedFile
   * @throws Exception
   */
  void decrypt(Object encryptedObject, File decryptedFile);
}
