package org.icgc.dcc.ga4gh.loader.vcf.callprocessors;

import com.google.common.collect.ImmutableList;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import lombok.val;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.joining;

/**
 * Contains methods that are needed by all CallProcessor implementations
 */
public abstract class AbstractCallProcessor implements CallProcessor {

  public static final int DEFAULT_REFERENCE_ALLELE_POSITION = 0;
  public static final int ALTERNATIVE_ALLELE_INDEX_OFFSET = 1;
  public static final int UNKNOWN_ALLELE_INDEX = -1;
  public static final String UNKNOWN_ALLELE_STRING = ".";

  /*
   * Returns a list contain Allele IDs associated with the maternal and paternal alleles of this genotype. Allele ID (or
   * index) 0 represents the reference allele. If there are N alternative alleles, then the Alternative Allele ID range
   * is 1 to 0+N. Example, if the a variant has reference allele "A" and the alternative alleles are "[T,GG,ATG]", and
   * the genotype is 2/1, this means the first allele of this genotype has ID 2, which maps to "ATG" and the second
   * allele of this genotype has ID 1, which maps to "GG". TODO: test and verify empty or null alleles
   * 
   * @throws IllegalStateException when an allele from the genotype does not exist in the alternative alleles, or when
   * an allele was not parsed correctly
   * 
   */
  private static boolean isAlleleUnknown(final Allele allele) {
    return allele.getBaseString().equals(UNKNOWN_ALLELE_STRING);
  }

  protected List<Integer> convertGenotypeAlleles(final List<Allele> alternativeAlleles, final Genotype genotype) {
    val allelesBuilder = ImmutableList.<Integer> builder();
    for (val allele : genotype.getAlleles()) {
      if (allele.isNonReference()) {
        val indexAltAllele = alternativeAlleles.indexOf(allele);
        val foundIndex = indexAltAllele > -1;
        if (foundIndex) {
          allelesBuilder.add(ALTERNATIVE_ALLELE_INDEX_OFFSET + indexAltAllele);
        } else if (isAlleleUnknown(allele)) {
          allelesBuilder.add(UNKNOWN_ALLELE_INDEX);
        } else {
          checkState(foundIndex, "Could not find the allele [%s] in the alternative alleles list [%s] ",
              allele.getBaseString(),
              alternativeAlleles.stream()
                  .map(x -> x.getBaseString())
                  .collect(joining(",")));
        }
      } else {
        allelesBuilder.add(DEFAULT_REFERENCE_ALLELE_POSITION);
      }
    }

    val alleles = allelesBuilder.build();
    val hasCorrectNumberOfAlleles = alleles.size() == genotype.getAlleles().size();

    checkState(hasCorrectNumberOfAlleles,
        "There was an error with creating the allele index list. AlternateAlleles: [%s], GenotypesAlleles: [%s]",
        alternativeAlleles.stream().map(x -> x.getBaseString()).collect(joining(",")),
        genotype.getAlleles().stream().map(x -> x.getBaseString()).collect(joining(",")));
    return alleles;
  }

}
