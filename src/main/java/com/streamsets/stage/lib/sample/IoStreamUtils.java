package com.streamsets.stage.lib.sample;

import java.io.ByteArrayOutputStream;

/**
 * Subclass of ByteArrayOutputStream which exposed the internal buffer to help avoid making a copy of the buffer.
 *
 * Note that the buffer size may be greater than the actual data. Therefore use {@link #size()} method to determine
 * the actual size of data.
 */

public class IoStreamUtils {

    public static class ByRefByteArrayOutputStream extends ByteArrayOutputStream {
        public byte[] getInternalBuffer() {
            return buf;
        }
    }
}
