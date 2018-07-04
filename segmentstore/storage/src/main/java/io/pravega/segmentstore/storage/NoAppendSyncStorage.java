/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

import java.util.List;

/**
 * Defines an abstraction for Permanent Storage.
 * Note: not all operations defined here are needed in the (async) Storage interface.
 */
@SuppressWarnings("checkstyle:JavadocMethod")
public interface NoAppendSyncStorage extends SyncStorage {
    //Lists all the files for segmentname.
    List<String> list(String segmentName);
}
