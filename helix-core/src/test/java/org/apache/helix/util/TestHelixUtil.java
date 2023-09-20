package org.apache.helix.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHelixUtil {
  @Test
  public void testFillStringTemplateFromMap() {
    Map<String, String> keyValuePairs =
        ImmutableMap.of("CAGE", "H", "CABINET", "30", "INSTANCE_NAME", "foo.bar.com_1234");

    Assert.assertEquals(
        HelixUtil.fillStringTemplateFromMap("rack=${CAGE}:${CABINET},host=${INSTANCE_NAME}",
            keyValuePairs), "rack=H:30,host=foo.bar.com_1234");
  }
}
