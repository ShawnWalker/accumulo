/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.file;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public abstract class FileOperations {

  private static final HashSet<String> validExtensions = new HashSet<String>(Arrays.asList(Constants.MAPFILE_EXTENSION, RFile.EXTENSION));

  public static Set<String> getValidExtensions() {
    return validExtensions;
  }

  public static String getNewFileExtension(AccumuloConfiguration acuconf) {
    return acuconf.get(Property.TABLE_FILE_TYPE);
  }

  public static FileOperations getInstance() {
    return new DispatchingFileFactory();
  }

  //
  // Abstract methods (to be implemented by subclasses)
  //

  protected abstract long getFileSize(GetFileSizeOperation options) throws IOException;

  protected abstract FileSKVWriter openWriter(OpenWriterOperation options) throws IOException;

  protected abstract FileSKVIterator openIndex(OpenIndexOperation options) throws IOException;

  protected abstract FileSKVIterator openScanReader(OpenScanReaderOperation options) throws IOException;

  protected abstract FileSKVIterator openReader(OpenReaderOperation options) throws IOException;

  //
  // File operations
  //

  /**
   * Construct an operation object allowing one to query the size of a file. <br>
   * Syntax:
   *
   * <pre>
   * long size = fileOperations.getFileSize().ofFile(filename, fileSystem, fsConfiguration).withTableConfiguration(tableConf).execute();
   * </pre>
   */
  @SuppressWarnings("unchecked")
  public NeedsFile<NeedsTableConfiguration<GetFileSizeOperationBuilder>> getFileSize() {
    return (NeedsFile) new GetFileSizeOperation();
  }

  /**
   * Construct an operation object allowing one to create a writer for a file. <br>
   * Syntax:
   *
   * <pre>
   * FileSKVWriter writer = fileOperations.openWriter()
   *     .ofFile(...)
   *     .withTableConfiguration(...)
   *     .withRateLimiter(...) // optional
   *     .withCompression(...) // optional
   *     .execute();
   * </pre>
   */
  @SuppressWarnings("unchecked")
  public NeedsFile<NeedsTableConfiguration<OpenWriterOperationBuilder>> openWriter() {
    return (NeedsFile) new OpenWriterOperation();
  }

  /**
   * Construct an operation object allowing one to create an index iterator for a file. <br>
   * Syntax:
   *
   * <pre>
   * FileSKVIterator iterator = fileOperations.openIndex()
   *     .ofFile(...)
   *     .withTableConfiguration(...)
   *     .withRateLimiter(...) // optional
   *     .withBlockCache(...) // optional
   *     .execute();
   * </pre>
   */
  @SuppressWarnings("unchecked")
  public NeedsFile<NeedsTableConfiguration<OpenIndexOperationBuilder>> openIndex() {
    return (NeedsFile) new OpenIndexOperation();
  }

  /**
   * Construct an operation object allowing one to create a "scan" reader for a file. Scan readers do not have any optimizations for seeking beyond their
   * initial position. This is useful for file operations that only need to scan data within a range and do not need to seek. Therefore file metadata such as
   * indexes does not need to be kept in memory while the file is scanned. Also seek optimizations like bloom filters do not need to be loaded. <br>
   * Syntax:
   *
   * <pre>
   * FileSKVIterator scanner = fileOperations.openScanReader()
   *     .ofFile(...)
   *     .overRange(...)
   *     .withTableConfiguration(...)
   *     .withRateLimiter(...) // optional
   *     .withBlockCache(...) // optional
   *     .execute();
   * </pre>
   */
  @SuppressWarnings("unchecked")
  public NeedsFile<NeedsRange<NeedsTableConfiguration<OpenScanReaderOperationBuilder>>> openScanReader() {
    return (NeedsFile) new OpenScanReaderOperation();
  }

  /**
   * Construct an operation object allowing one to create a reader for a file. A reader constructed in this manner fully supports seeking, and also enables any
   * optimizations related to seeking (e.g. Bloom filters). <br>
   * Syntax:
   *
   * <pre>
   * FileSKVIterator scanner = fileOperations.openReader()
   *     .ofFile(...)
   *     .withTableConfiguration(...)
   *     .withRateLimiter(...) // optional
   *     .withBlockCache(...) // optional
   *     .seekToBeginning(...) // optional
   *     .execute();
   * </pre>
   */
  @SuppressWarnings("unchecked")
  public NeedsFile<NeedsTableConfiguration<OpenReaderOperationBuilder>> openReader() {
    return (NeedsFile) new OpenReaderOperation();
  }

  //
  // Domain specific embedded language for execution of operations.
  //
  // Here, for each ...Operation class which is a POJO holding a group of parameters,
  // we have a parallel ...OperationBuilder interface which only exposes the setters / execute methods.
  // This allows us to expose only the setter/execute methods to upper layers, while
  // allowing lower layers the freedom to both get and set.
  //

  /**
   * Options common to all FileOperations.
   */
  protected static class FileAccessOperation<SubclassType extends FileAccessOperation<SubclassType>> {
    private AccumuloConfiguration tableConfiguration;

    private String filename;
    private FileSystem fs;
    private Configuration fsConf;

    /** Specify the table configuration defining access to this file. */
    @SuppressWarnings("unchecked")
    public SubclassType withTableConfiguration(AccumuloConfiguration tableConfiguration) {
      this.tableConfiguration = tableConfiguration;
      return (SubclassType) this;
    }

    /** Specify the file this operation should apply to. */
    @SuppressWarnings("unchecked")
    public SubclassType ofFile(String filename, FileSystem fs, Configuration fsConf) {
      this.filename = filename;
      this.fs = fs;
      this.fsConf = fsConf;
      return (SubclassType) this;
    }

    /** Specify the file this operation should apply to. */
    @SuppressWarnings("unchecked")
    public SubclassType ofFile(String filename) {
      this.filename = filename;
      return (SubclassType) this;
    }

    /** Specify the filesystem which this operation should apply to, along with its configuration. */
    @SuppressWarnings("unchecked")
    public SubclassType inFileSystem(FileSystem fs, Configuration fsConf) {
      this.fs = fs;
      this.fsConf = fsConf;
      return (SubclassType) this;
    }

    public String getFilename() {
      return filename;
    }

    public FileSystem getFileSystem() {
      return fs;
    }

    public Configuration getConfiguration() {
      return fsConf;
    }

    public AccumuloConfiguration getTableConfiguration() {
      return tableConfiguration;
    }

    /** Check for null parameters. */
    protected void validate() {
      Objects.requireNonNull(getFilename());
      Objects.requireNonNull(getFileSystem());
      Objects.requireNonNull(getConfiguration());
      Objects.requireNonNull(getTableConfiguration());
    }
  }

  /** Builder interface parallel to {@link FileAccessOperation}. */
  protected static interface FileAccessOperationBuilder<SubbuilderType> extends NeedsFile<SubbuilderType>, NeedsFileSystem<SubbuilderType>,
      NeedsTableConfiguration<SubbuilderType> {
    // no optional/generic methods.
  }

  /**
   * Operation object for performing {@code getFileSize()} operations.
   */
  protected class GetFileSizeOperation extends FileAccessOperation<GetFileSizeOperation> implements GetFileSizeOperationBuilder {
    /** Return the size of the file. */
    public long execute() throws IOException {
      validate();
      return getFileSize(this);
    }
  }

  /** Builder interface for {@link GetFileSizeOperation}, allowing execution of {@code getFileSize()} operations. */
  public static interface GetFileSizeOperationBuilder extends FileAccessOperationBuilder<GetFileSizeOperationBuilder> {
    /** Return the size of the file. */
    public long execute() throws IOException;
  }

  /**
   * Options common to all {@code FileOperation}s which perform reading or writing.
   */
  protected static class FileIOOperation<SubclassType extends FileIOOperation<SubclassType>> extends FileAccessOperation<SubclassType> {
    private RateLimiter rateLimiter;

    /** Specify a rate limiter for this operation. */
    @SuppressWarnings("unchecked")
    public SubclassType withRateLimiter(RateLimiter rateLimiter) {
      this.rateLimiter = rateLimiter;
      return (SubclassType) this;
    }

    public RateLimiter getRateLimiter() {
      return rateLimiter;
    }
  }

  /** Builder interface parallel to {@link FileIOOperation}. */
  protected static interface FileIOOperationBuilder<SubbuilderType> extends FileAccessOperationBuilder<SubbuilderType> {
    /** Specify a rate limiter for this operation. */
    public SubbuilderType withRateLimiter(RateLimiter rateLimiter);
  }

  /**
   * Operation object for constructing a writer.
   */
  protected class OpenWriterOperation extends FileIOOperation<OpenWriterOperation> implements OpenWriterOperationBuilder {
    private String compression;

    public OpenWriterOperation withCompression(String compression) {
      this.compression = compression;
      return this;
    }

    public String getCompression() {
      return compression;
    }

    public FileSKVWriter execute() throws IOException {
      validate();
      return openWriter(this);
    }
  }

  /** Builder interface parallel to {@link OpenWriterOperation}. */
  public static interface OpenWriterOperationBuilder extends FileIOOperationBuilder<OpenWriterOperationBuilder> {
    /** Set the compression type. */
    public OpenWriterOperationBuilder withCompression(String compression);

    /** Construct the writer. */
    public FileSKVWriter execute() throws IOException;
  }

  /**
   * Options common to all {@code FileOperations} which perform reads.
   */
  protected static class FileReaderOperation<SubclassType extends FileReaderOperation<SubclassType>> extends FileIOOperation<SubclassType> {
    private BlockCache dataCache;
    private BlockCache indexCache;

    /** (Optional) Set the block cache pair to be used to optimize reads within the constructed reader. */
    @SuppressWarnings("unchecked")
    public SubclassType withBlockCache(BlockCache dataCache, BlockCache indexCache) {
      this.dataCache = dataCache;
      this.indexCache = indexCache;
      return (SubclassType) this;
    }

    /** (Optional) set the data cache to be used to optimize reads within the constructed reader. */
    @SuppressWarnings("unchecked")
    public SubclassType withDataCache(BlockCache dataCache) {
      this.dataCache = dataCache;
      return (SubclassType) this;
    }

    /** (Optional) set the index cache to be used to optimize reads within the constructed reader. */
    @SuppressWarnings("unchecked")
    public SubclassType withIndexCache(BlockCache indexCache) {
      this.indexCache = indexCache;
      return (SubclassType) this;
    }

    public BlockCache getDataCache() {
      return dataCache;
    }

    public BlockCache getIndexCache() {
      return indexCache;
    }
  }

  /** Builder interface parallel to {@link FileReaderOperation}. */
  protected static interface FileReaderOperationBuilder<SubbuilderType> extends FileIOOperationBuilder<SubbuilderType> {
    /** (Optional) Set the block cache pair to be used to optimize reads within the constructed reader. */
    public SubbuilderType withBlockCache(BlockCache dataCache, BlockCache indexCache);

    /** (Optional) set the data cache to be used to optimize reads within the constructed reader. */
    public SubbuilderType withDataCache(BlockCache dataCache);

    /** (Optional) set the index cache to be used to optimize reads within the constructed reader. */
    public SubbuilderType withIndexCache(BlockCache indexCache);
  }

  /**
   * Operation object for opening an index.
   */
  protected class OpenIndexOperation extends FileReaderOperation<OpenIndexOperation> implements OpenIndexOperationBuilder {
    public FileSKVIterator execute() throws IOException {
      validate();
      return openIndex(this);
    }
  }

  /** Builder interface parallel to {@link OpenIndexOperation}. */
  public static interface OpenIndexOperationBuilder extends FileReaderOperationBuilder<OpenIndexOperationBuilder> {
    /** Construct the reader. */
    public FileSKVIterator execute() throws IOException;
  }

  /** Operation object for opening a scan reader. */
  protected class OpenScanReaderOperation extends FileReaderOperation<OpenScanReaderOperation> implements OpenScanReaderOperationBuilder {
    private Range range;
    private Set<ByteSequence> columnFamilies;
    private boolean inclusive;

    /** Set the range over which the constructed iterator will search. */
    public OpenScanReaderOperation overRange(Range range, Set<ByteSequence> columnFamilies, boolean inclusive) {
      this.range = range;
      this.columnFamilies = columnFamilies;
      this.inclusive = inclusive;
      return this;
    }

    /** The range over which this reader should scan. */
    public Range getRange() {
      return range;
    }

    /** The column families which this reader should scan. */
    public Set<ByteSequence> getColumnFamilies() {
      return columnFamilies;
    }

    public boolean isRangeInclusive() {
      return inclusive;
    }

    @Override
    protected void validate() {
      super.validate();
      Objects.requireNonNull(range);
      Objects.requireNonNull(columnFamilies);
    }

    /** Execute the operation, constructing a scan iterator. */
    public FileSKVIterator execute() throws IOException {
      validate();
      return openScanReader(this);
    }
  }

  /** Builder interface parallel to {@link OpenScanReaderOperation}. */
  public static interface OpenScanReaderOperationBuilder extends FileReaderOperationBuilder<OpenScanReaderOperationBuilder>,
      NeedsRange<OpenScanReaderOperationBuilder> {
    /** Execute the operation, constructing a scan iterator. */
    public FileSKVIterator execute() throws IOException;
  }

  /** Operation object for opening a full reader. */
  protected class OpenReaderOperation extends FileReaderOperation<OpenReaderOperation> implements OpenReaderOperationBuilder {
    private boolean seekToBeginning = false;

    /**
     * Seek the constructed iterator to the beginning of its domain before returning. Equivalent to {@code seekToBeginning(true)}.
     */
    public OpenReaderOperation seekToBeginning() {
      return seekToBeginning(true);
    }

    /** If true, seek the constructed iterator to the beginning of its domain before returning. */
    public OpenReaderOperation seekToBeginning(boolean seekToBeginning) {
      this.seekToBeginning = seekToBeginning;
      return this;
    }

    public boolean isSeekToBeginning() {
      return seekToBeginning;
    }

    /** Execute the operation, constructing the specified file reader. */
    public FileSKVIterator execute() throws IOException {
      validate();
      return openReader(this);
    }
  }

  /** Builder parallel to {@link OpenReaderOperation}. */
  public static interface OpenReaderOperationBuilder extends FileReaderOperationBuilder<OpenReaderOperationBuilder> {
    /**
     * Seek the constructed iterator to the beginning of its domain before returning. Equivalent to {@code seekToBeginning(true)}.
     */
    public OpenReaderOperationBuilder seekToBeginning();

    /** If true, seek the constructed iterator to the beginning of its domain before returning. */
    public OpenReaderOperationBuilder seekToBeginning(boolean seekToBeginning);

    /** Execute the operation, constructing the specified file reader. */
    public FileSKVIterator execute() throws IOException;
  }

  /**
   * Type wrapper to ensure that {@code ofFile(...)} is called before other methods.
   */
  public static interface NeedsFile<ReturnType> {
    /** Specify the file this operation should apply to. */
    public ReturnType ofFile(String filename, FileSystem fs, Configuration fsConf);

    /** Specify the file this operation should apply to. */
    public NeedsFileSystem<ReturnType> ofFile(String filename);
  }

  /**
   * Type wrapper to ensure that {@code inFileSystem(...)} is called before other methods.
   */
  public static interface NeedsFileSystem<ReturnType> {
    /** Specify the {@link FileSystem} that this operation operates on, along with an alternate configuration. */
    public ReturnType inFileSystem(FileSystem fs, Configuration fsConf);
  }

  /**
   * Type wrapper to ensure that {@code withTableConfiguration(...)} is called before other methods.
   */
  public static interface NeedsTableConfiguration<ReturnType> {
    /** Specify the table configuration defining access to this file. */
    public ReturnType withTableConfiguration(AccumuloConfiguration tableConfiguration);
  }

  /**
   * Type wrapper to ensure that {@code overRange(...)} is called before other methods.
   */
  public static interface NeedsRange<ReturnType> {
    /** Set the range over which the constructed iterator will search. */
    public ReturnType overRange(Range range, Set<ByteSequence> columnFamilies, boolean inclusive);
  }

}
