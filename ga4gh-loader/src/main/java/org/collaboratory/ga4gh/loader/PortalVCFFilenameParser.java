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

import java.util.Arrays;
import java.util.stream.Collectors;

import lombok.NonNull;
import lombok.val;

/**
 * Takes a fileName from the fileMeta ObjectNode from Portal, and parses certain attributes needed for loading
 */
public class PortalVCFFilenameParser {

  private static final int OBJECT_ID_POS = 0;
  private static final int CALLER_ID_POS = 1;
  private static final int DATE_POS = 2;
  private static final int MUTATION_TYPE_POS = 3;
  private static final int MUTATION_SUB_TYPE_POS = 4;
  private static final int FILE_TYPE_POS = 5;
  private static final int EXPECTED_NUM_POSITIONS = 6;

  private final String[] filenameArray;

  public PortalVCFFilenameParser(@NonNull String filename) {
    filenameArray = filename.split("\\.");
  }

  // Since stored as array, just reconstruct it
  public String getFilename() {
    return Arrays.stream(filenameArray).collect(Collectors.joining("."));
  }

  public boolean isMinimumLength() {
    return filenameArray.length >= EXPECTED_NUM_POSITIONS;
  }

  private String getPos(int position) {
    if (position >= filenameArray.length) {
      return "";
    } else {
      return filenameArray[position];
    }
  }

  public String getId() {
    return getPos(OBJECT_ID_POS);
  }

  public String getCallerId() {
    return getPos(CALLER_ID_POS);
  }

  public String getDate() {
    return getPos(DATE_POS);
  }

  public String getMutationType() {
    return getPos(MUTATION_TYPE_POS);
  }

  public String getMutationSubType() {
    return getPos(MUTATION_SUB_TYPE_POS);
  }

  public String getFileType() {
    val sb = new StringBuilder();
    for (int i = FILE_TYPE_POS; i < filenameArray.length; i++) {
      sb.append(getPos(i));
    }
    return sb.toString();
  }

}