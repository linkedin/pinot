/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.geospatial.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.locationtech.jts.geom.Geometry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class GeometrySerializer {
    private static GeometrySerde serde = new GeometrySerde();

    private GeometrySerializer() {
    }

    public static byte[] serialize(Geometry geometry) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);
        serde.write(new Kryo(), output, geometry);
        output.close();
        return out.toByteArray();
    }

    public static Geometry deserialize(byte[] bytes) {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        Input input = new Input(in);
        Object deserialized = serde.read(new Kryo(), input, Geometry.class);
        input.close();
        return (Geometry) deserialized;
    }
}