/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.collaboratory.ga4gh.loader.model.metadata;

import com.google.common.collect.Iterables;
import lombok.Getter;
import lombok.NonNull;
import org.collaboratory.ga4gh.loader.vcf.enums.CallerTypes;
import org.icgc.dcc.common.core.util.Splitters;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.Joiners.DOT;

/**
 * Takes a filename, and extracts particular fields characteristic of ICGC VCF files
 */
public class PortalVCFFilenameParser
    implements Serializable, Comparable<PortalVCFFilenameParser> {

  private static final long serialVersionUID = 1484172857L;

  private static final int MIN_NUM_FIELDS = 6;
  private static final int OBJECT_ID_POS = 0;
  private static final int CALLER_ID_POS = 1;
  private static final int DATE_POS = 2;
  private static final int MUTATION_TYPE_POS = 3;
  private static final int SUB_MUTATION_TYPE_POS = 4;
  private static final int FILE_TYPE_POS = 5;

  @Getter
  private final String[] elements;
  private CallerTypes callerType = null;

  public PortalVCFFilenameParser(@NonNull final String filename) {
    checkArgument(!filename.isEmpty(), "The filename [%s] is empty", filename);
    elements = Iterables.toArray(Splitters.DOT
        .trimResults()
        .split(filename), String.class);
    checkArgument(elements.length >= MIN_NUM_FIELDS,
        "The filename [%s] has %d fields, but a minimum of %d is expected", filename, elements.length, MIN_NUM_FIELDS);
  }

  public String getObjectId() {
    return elements[OBJECT_ID_POS];
  }

  public String getCallerId() {
    return elements[CALLER_ID_POS];
  }

  public String getDate() {
    return elements[DATE_POS];
  }

  public String getMutationType() {
    return elements[MUTATION_TYPE_POS];
  }

  public String getSubMutationType() {
    return elements[SUB_MUTATION_TYPE_POS];
  }

  public String getFileType() {
    return elements[FILE_TYPE_POS];
  }

  public String getFilename() {
    return DOT.join(elements);
  }

  @Override
  public String toString() {
    return getFilename();
  }

  private static CallerTypes parseCallerType(final String callerId) {
    boolean found = false;
    CallerTypes foundCallerType = null;
    for (CallerTypes callerType : CallerTypes.values()) {
      if (callerType.isIn(callerId)) {
        foundCallerType = callerType;
        found = true;
        break;
      }
    }
    checkState(found, "The callerId [%s] does not contain any of the available caller types: [%s]",
        callerId, CallerTypes.values());
    return foundCallerType;
  }

  /*
   * Lazy initialization. Needed for when regenerating Enums
   */
  public CallerTypes getCallerType() {
    if (callerType == null) {
      callerType = parseCallerType(this.getCallerId());
    }
    return callerType;
  }

  @Override
  public int compareTo(PortalVCFFilenameParser o) {
    return this.getFilename().compareTo(o.getFilename());
  }
}
