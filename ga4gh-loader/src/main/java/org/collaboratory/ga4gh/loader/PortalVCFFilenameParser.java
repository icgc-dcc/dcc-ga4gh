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
package org.collaboratory.ga4gh.loader;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Joiner;

import lombok.Getter;
import lombok.NonNull;

/**
 * Takes a filename, and extracts particular fields characteristic of ICGC VCF files
 */
public class PortalVCFFilenameParser {

  private static final int MIN_NUM_FIELDS = 6;
  private static final int OBJECT_ID_POS = 0;
  private static final int CALLER_ID_POS = 1;
  private static final int DATE_POS = 2;
  private static final int MUTATION_TYPE_POS = 3;
  private static final int MUTATION_SUB_TYPE_POS = 4;
  private static final int FILE_TYPE_POS = 5;

  @Getter
  private final String[] array;

  public PortalVCFFilenameParser(@NonNull final String filename) {
    array = filename.split("\\.");
    checkArgument(array.length >= MIN_NUM_FIELDS, String.format(
        "The filename [%s] has %d fields, but a minimum of %d is expected", filename, array.length, MIN_NUM_FIELDS));
  }

  public String getObjectId() {
    return array[OBJECT_ID_POS];
  }

  public String getCallerId() {
    return array[CALLER_ID_POS];
  }

  public String getDate() {
    return array[DATE_POS];
  }

  public String getMutationType() {
    return array[MUTATION_TYPE_POS];
  }

  public String getMutationSubType() {
    return array[MUTATION_SUB_TYPE_POS];
  }

  public String getFileType() {
    return array[FILE_TYPE_POS];
  }

  public String getFilename() {
    return Joiner.on(".").join(array);
  }

  @Override
  public String toString() {
    return getFilename();
  }

}
