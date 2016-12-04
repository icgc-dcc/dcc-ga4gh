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

import static lombok.AccessLevel.PRIVATE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor(access = PRIVATE)
public final class PortalFiles {

  private static final String OBJECT_ID = "objectId";
  private static final String PROJECT_CODE = "projectCode";
  private static final String DONOR_ID = "donorId";
  private static final String DONORS = "donors";
  private static final String SAMPLE_ID = "sampleId";
  private static final String ID = "id";
  private static final String DATA_TYPE = "dataType";
  private static final String DATA_CATEGORIZATION = "dataCategorization";
  private static final String FILE_NAME = "fileName";
  private static final String FILE_COPIES = "fileCopies";

  public static String getObjectId(@NonNull ObjectNode file) {
    return file.path(OBJECT_ID).textValue();
  }

  // TODO: [rtisma] - need to also get the file name for consensus discrimination

  public static String getDataType(@NonNull ObjectNode file) {
    return getDataCategorization(file).path(DATA_TYPE).textValue();
  }

  public static String getFileId(@NonNull ObjectNode file) {
    return file.path(ID).textValue();
  }

  public static String getFileName(@NonNull ObjectNode file) {
    return getFirstFileCopy(file).path(FILE_NAME).textValue();
  }

  public static String getProjectCode(@NonNull ObjectNode file) {
    return getFirstDonor(file).path(PROJECT_CODE).get(0).textValue();
  }

  public static String getDonorId(@NonNull ObjectNode file) {
    return getFirstDonor(file).path(DONOR_ID).textValue();
  }

  public static String getSampleId(@NonNull ObjectNode file) {
    return getFirstDonor(file).path(SAMPLE_ID).get(0).textValue();
  }

  private static JsonNode getFirstDonor(ObjectNode file) {
    return getDonors(file).path(0);
  }

  private static JsonNode getFirstFileCopy(ObjectNode file) {
    return getFileCopies(file).path(0);
  }

  private static JsonNode getDataCategorization(ObjectNode file) {
    return file.path(DATA_CATEGORIZATION);
  }

  private static JsonNode getFileCopies(ObjectNode file) {
    return file.path(FILE_COPIES);
  }

  private static JsonNode getDonors(ObjectNode file) {
    return file.path(DONORS);
  }

  public static PortalVCFFilenameParser getParser(ObjectNode file) {
    return new PortalVCFFilenameParser(getFileName(file));
  }

}
