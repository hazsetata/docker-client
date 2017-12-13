/*-
 * -\-\-
 * docker-client
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.docker.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpHost;
import org.apache.http.annotation.Contract;
import org.apache.http.annotation.ThreadingBehavior;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.protocol.HttpContext;

/**
 * Provides a ConnectionSocketFactory for connecting Apache HTTP clients to Windows named pipes.
 */
@Contract(threading = ThreadingBehavior.IMMUTABLE_CONDITIONAL)
public class WindowsNamedPipeSocketFactory implements ConnectionSocketFactory {
  private static final Pattern PIPE_URI_PATTERN =
      Pattern.compile("^npipe:/+(?<host>.+)/pipe/(?<name>.+)$");
  private static final String PIPE_NAME_FORMAT = "\\\\%s\\pipe\\%s";

  private final String pipeName;

  public WindowsNamedPipeSocketFactory(final URI socketUri) {
    Matcher uriMatcher = PIPE_URI_PATTERN.matcher(socketUri.toString());
    if (uriMatcher.matches()) {
      pipeName = String.format(
          PIPE_NAME_FORMAT,
          uriMatcher.group("host").equalsIgnoreCase("localhost") ? "." : uriMatcher.group("host"),
          uriMatcher.group("name")
      );
    } else {
      throw new IllegalArgumentException("Invalid named pipe URI.");
    }
  }

  public static URI sanitizeUri(final URI uri) {
    if (uri.getScheme().equals("npipe")) {
      return URI.create("npipe://localhost:80");
    } else {
      return uri;
    }
  }

  @Override
  public Socket createSocket(HttpContext httpContext) throws IOException {
    return new WindowsNamedPipeSocket(this.pipeName);
  }

  @Override
  public Socket connectSocket(final int connectTimeout,
                              final Socket socket,
                              final HttpHost host,
                              final InetSocketAddress remoteAddress,
                              final InetSocketAddress localAddress,
                              final HttpContext context) throws IOException {
    if (!(socket instanceof WindowsNamedPipeSocket)) {
      throw new AssertionError("Unexpected socket: " + socket);
    }

    socket.connect(new InetSocketAddress(80)); // The SocketAddress implementation is ignored

    return socket;
  }

  public class WindowsNamedPipeSocket extends Socket {
    private String namedPipePath;

    private RandomAccessFile namedPipeFile;
    private InputStream inputStream;
    private OutputStream outputStream;

    public WindowsNamedPipeSocket(String namedPipePath) {
      this.namedPipePath = namedPipePath;
    }

    @Override
    public void connect(SocketAddress endpoint, int timeout) throws IOException {
      namedPipeFile = new RandomAccessFile(namedPipePath, "rw");
      inputStream = new InputStream() {
        @Override
        public int read() throws IOException {
          return namedPipeFile.read();
        }

        @Override
        public int read(byte[] bytes) throws IOException {
          return namedPipeFile.read(bytes);
        }

        @Override
        public int read(byte[] bytes, int off, int len) throws IOException {
          return namedPipeFile.read(bytes, off, len);
        }
      };

      outputStream = new OutputStream() {
        @Override
        public void write(int abyte) throws IOException {
          namedPipeFile.write(abyte);
        }

        @Override
        public void write(byte[] bytes) throws IOException {
          namedPipeFile.write(bytes);
        }

        @Override
        public void write(byte[] bytes, int off, int len) throws IOException {
          namedPipeFile.write(bytes, off, len);
        }
      };
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return inputStream;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
      return outputStream;
    }

    @Override
    public void setTcpNoDelay(boolean on) throws SocketException {
      // Do nothing
    }

    @Override
    public boolean getTcpNoDelay() throws SocketException {
      return true;
    }

    @Override
    public void setSoLinger(boolean on, int linger) throws SocketException {
      // Do nothing
    }

    @Override
    public int getSoLinger() throws SocketException {
      return -1; // Disabled
    }

    @Override
    public void setOOBInline(boolean on) throws SocketException {
      // Do nothing
    }

    @Override
    public boolean getOOBInline() throws SocketException {
      return false;
    }

    @Override
    public synchronized void setSoTimeout(int timeout) throws SocketException {
      // Do nothing
    }

    @Override
    public synchronized int getSoTimeout() throws SocketException {
      return 0;
    }

    @Override
    public synchronized void setSendBufferSize(int size) throws SocketException {
      // Do nothing
    }

    @Override
    public synchronized int getSendBufferSize() throws SocketException {
      return 0;
    }

    @Override
    public synchronized void setReceiveBufferSize(int size) throws SocketException {
      // Do nothing
    }

    @Override
    public synchronized int getReceiveBufferSize() throws SocketException {
      return 0;
    }

    @Override
    public void setKeepAlive(boolean on) throws SocketException {
      // Do nothing
    }

    @Override
    public boolean getKeepAlive() throws SocketException {
      return true;
    }

    @Override
    public void setTrafficClass(int tc) throws SocketException {
      // Do nothing
    }

    @Override
    public int getTrafficClass() throws SocketException {
      return 0;
    }

    @Override
    public void setReuseAddress(boolean on) throws SocketException {
      // Do nothing
    }

    @Override
    public boolean getReuseAddress() throws SocketException {
      return false;
    }

    @Override
    public void shutdownInput() throws IOException {
      // Do nothing
    }

    @Override
    public void shutdownOutput() throws IOException {
      // Do nothing
    }

    @Override
    public synchronized void close() throws IOException {
      if (namedPipeFile != null) {
        namedPipeFile.close();
        namedPipeFile = null;
        inputStream = null;
        outputStream = null;
      }
    }

    @Override
    public boolean isClosed() {
      return (namedPipeFile == null);
    }
  }
}
