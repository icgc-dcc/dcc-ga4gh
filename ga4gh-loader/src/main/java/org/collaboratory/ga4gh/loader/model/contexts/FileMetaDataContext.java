package org.collaboratory.ga4gh.loader.model.contexts;

import static java.util.stream.Collectors.groupingBy;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;

import org.collaboratory.ga4gh.loader.model.metadata.FileMetaData;
import org.collaboratory.ga4gh.loader.model.metadata.FileMetaData.FileSizeComparator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.val;

@Builder
@Value
public class FileMetaDataContext implements Iterable<FileMetaData> {

  @Singular
  private final List<FileMetaData> fileMetaDatas;

  public FileMetaDataContext filter(@NonNull final Predicate<? super FileMetaData> predicate) {
    val builder = FileMetaDataContext.builder();
    fileMetaDatas.stream().filter(predicate).forEach(x -> builder.fileMetaData(x));
    return builder.build();
  }

  public static FileMetaDataContext buildFileMetaDataList(@NonNull final Iterable<ObjectNode> objectNodes) {
    val builder = FileMetaDataContext.builder();
    stream(objectNodes).map(FileMetaData::buildFileMetaData).forEach(x -> builder.fileMetaData(x));
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
    Collections.sort(list, new FileSizeComparator(ascending));
    return new FileMetaDataContext(ImmutableList.copyOf(list));
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

}
