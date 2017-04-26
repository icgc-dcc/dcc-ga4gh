package org.icgc.dcc.ga4gh.loader2;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.loader2.persistance.FileObjectRestorerFactory;
import org.icgc.dcc.ga4gh.loader2.storage.StorageFactory;
import org.junit.Test;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Set;

import static org.icgc.dcc.ga4gh.loader.Config.STORAGE_OUTPUT_VCF_STORAGE_DIR;
import static org.icgc.dcc.ga4gh.loader.Config.TOKEN;
import static org.icgc.dcc.ga4gh.loader2.dao.portal.PortalMetadataDaoFactory.newDefaultPortalMetadataDaoFactory;
import static org.icgc.dcc.ga4gh.loader2.portal.PortalCollabVcfFileQueryCreator.newPortalCollabVcfFileQueryCreator;

@Slf4j
public class DaoTest {

  @Test
  public void testDao(){
    val storage = StorageFactory.builder()
        .bypassMD5Check(false)
        .outputVcfDir(Paths.get(STORAGE_OUTPUT_VCF_STORAGE_DIR))
        .persistVcfDownloads(true)
        .token(TOKEN)
        .build()
        .getStorage();

    val localFileRestorerFactory = FileObjectRestorerFactory.newFileObjectRestorerFactory("test.persisted");
    val query = newPortalCollabVcfFileQueryCreator();
    val portalMetadataDaoFactory = newDefaultPortalMetadataDaoFactory(localFileRestorerFactory, query);
    val portalMetadataDao = portalMetadataDaoFactory.getPortalMetadataDao();
    for (val portalMetadata : portalMetadataDao.findAll()){
      val file = storage.getFile(portalMetadata);
      log.info("Downloaded: {}", portalMetadata.getPortalFilename().getFilename());


    }

  }

  @Test
  public void testTT(){


    val pp = new MyProcessor();


  }

  @PersistState
  private ArrayList<String> getList(){
    return Lists.newArrayList("hello", "there", "rob");
  }

  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  public static @interface PersistState{ }

  public static class MyProcessor extends AbstractProcessor{

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
      for (val element : roundEnv.getElementsAnnotatedWith(PersistState.class)){
        if (element instanceof TypeElement){
          val typeElement = (TypeElement)element;
          for (val enclosedElement : typeElement.getEnclosedElements()){
            if (enclosedElement instanceof VariableElement){
              val variableElement = (VariableElement)enclosedElement;
              log.info("Varname: {}",variableElement.getSimpleName());
            }
          }
        }
      }
      return false;
    }
  }
}
