package org.collaboratory.ga4gh.loader.model.metadata;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.val;
import org.collaboratory.ga4gh.core.ObjectPersistance;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;

import static java.util.stream.Collectors.groupingBy;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

@Builder
@Value
public class FileMetaDataContext implements Serializable, Iterable<FileMetaData> {

  private static final long serialVersionUID = 1486673032L;
  private static final FileMetaData.FilenameComparator FILENAME_COMPARATOR = new FileMetaData.FilenameComparator();
  private static final FileMetaData.FileSizeComparator FILE_SIZE_COMPARATOR = new FileMetaData.FileSizeComparator();

  @Singular
  private final List<FileMetaData> fileMetaDatas;

  public void store(String filename) throws IOException {
    ObjectPersistance.store(this, filename);
  }

  public static FileMetaDataContext restore(String filename) throws IOException, ClassNotFoundException {
    return (FileMetaDataContext) ObjectPersistance.restore(filename);
  }

  public FileMetaDataContext filter(@NonNull final Predicate<? super FileMetaData> predicate) {
    val builder = FileMetaDataContext.builder();
    fileMetaDatas.stream().filter(predicate).forEach(builder::fileMetaData);
    return builder.build();
  }

  public static FileMetaDataContext buildFileMetaDataContext(@NonNull final Iterable<ObjectNode> objectNodes) {
    val builder = FileMetaDataContext.builder();
    stream(objectNodes)
        .map(FileMetaData::buildFileMetaData)
        .forEach(builder::fileMetaData);
    return builder.build();
  }

  public Map<String, FileMetaDataContext> groupFileMetaDataBySample() {
    return groupFileMetaDataContext(FileMetaData::getSampleId);
  }

  public Map<String, FileMetaDataContext> groupFileMetaDataByCaller() {
    return groupFileMetaDataContext(x -> x.getVcfFilenameParser().getCallerId());
  }

  public Map<String, FileMetaDataContext> groupFileMetaDatasByDonor() {
    return groupFileMetaDataContext(FileMetaData::getDonorId);
  }

  public Map<String, FileMetaDataContext> groupFileMetaDatasByDataType() {
    return groupFileMetaDataContext(FileMetaData::getDataType);
  }

  public Map<String, FileMetaDataContext> groupFileMetaDatasByMutationType() {
    return groupFileMetaDataContext(x -> x.getVcfFilenameParser().getMutationType());
  }

  public Map<String, FileMetaDataContext> groupFileMetaDatasBySubMutationType() {
    return groupFileMetaDataContext(x -> x.getVcfFilenameParser().getSubMutationType());
  }

  public FileMetaDataContext sortByFileSize(final boolean ascending) {
    val list = Lists.newArrayList(fileMetaDatas);
    Collections.sort(list, FILE_SIZE_COMPARATOR);

    if (! ascending){
      Collections.reverse(list);
    }
    return new FileMetaDataContext(ImmutableList.copyOf(list));
  }

  public FileMetaDataContext sortByFilename(final boolean ascending) {
    val list = Lists.newArrayList(fileMetaDatas);
    Collections.sort(list, FILENAME_COMPARATOR);
    val fileMetaDataList = ImmutableList.copyOf(list);
    return FileMetaDataContext.builder()
        .fileMetaDatas(fileMetaDataList)
        .build();
  }

  public Map<String, FileMetaDataContext> groupFileMetaDataContext(
      final Function<? super FileMetaData, ? extends String> functor) {
    return ImmutableMap.copyOf(fileMetaDatas.stream().collect(groupingBy(functor, toFileMetaDataContext())));
  }

  public static Collector<FileMetaData, ImmutableList.Builder<FileMetaData>, FileMetaDataContext> toFileMetaDataContext() {
    return Collector.of(
        ImmutableList.Builder::new,
        (builder, e) -> builder.add(e),
        (b1, b2) -> b1.addAll(b2.build()),
        (builder) -> new FileMetaDataContext(builder.build()));
  }

  @Override
  public Iterator<FileMetaData> iterator() {
    return fileMetaDatas.iterator();
  }

  public int size() {
    return fileMetaDatas.size();
  }

  public FileMetaDataContext shuffle(final long seed){
    val rand = new Random();
    rand.setSeed(seed);
    val list = Lists.<FileMetaData> newArrayList(getFileMetaDatas());
    Collections.shuffle(list, rand);
    return FileMetaDataContext.builder().fileMetaDatas(ImmutableList.copyOf(list)).build();
  }

}
