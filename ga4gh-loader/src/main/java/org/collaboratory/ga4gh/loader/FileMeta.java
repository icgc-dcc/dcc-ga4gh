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
import lombok.val;

@NoArgsConstructor(access = PRIVATE)
public final class FileMeta {

  public static final String FILECOPIES = "fileCopies";
  public static final String OBJECTID = "objectId";
  public static final String PROJECTCODE = "projectCode";
  public static final String DONORID = "donorId";
  public static final String DONORS = "donors";
  public static final String SAMPLEID = "sampleId";
  public static final String REPONAME = "repoName";
  public static final String INDEXFILE = "indexFile";
  public static final String ID = "id";

  public static String getObjectId(@NonNull ObjectNode file) {
    return file.path(OBJECTID).textValue();
  }

  public static String getFileId(@NonNull ObjectNode file, String repoName) {
    JsonNode indexFileNode = getIndexFile(file, repoName);
    return indexFileNode.path(ID).textValue();
  }

  public static String getProjectCode(@NonNull ObjectNode file) {
    return getFirstDonor(file).path(PROJECTCODE).get(0).textValue();
  }

  public static String getDonorId(@NonNull ObjectNode file) {
    return getFirstDonor(file).path(DONORID).textValue();
  }

  public static String getSampleId(@NonNull ObjectNode file) {
    return getFirstDonor(file).path(SAMPLEID).get(0).textValue();
  }

  private static JsonNode getFirstDonor(ObjectNode file) {
    return getDonors(file).path(0);
  }

  private static JsonNode getFileCopies(ObjectNode file) {
    return file.path(FILECOPIES);
  }

  private static JsonNode getIndexFile(ObjectNode file, String repoName) {
    val fileCopiesNode = getFileCopies(file);
    val iterator = fileCopiesNode.elements();
    while (iterator.hasNext()) {
      JsonNode fileCopiesElement = iterator.next();
      JsonNode repoNameNode = fileCopiesElement.path(REPONAME);
      String actualRepoName = repoNameNode.textValue();
      if (actualRepoName.equals(repoName)) {
        return fileCopiesElement.path(INDEXFILE);
      }
    }
    throw new IllegalArgumentException(
        "The RepoName \"" + repoName + "\" DNE in the currect ObjectNode: " + file.toString());
  }

  private static JsonNode getDonors(ObjectNode file) {
    return file.path(DONORS);
  }

}
