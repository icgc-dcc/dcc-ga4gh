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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.icgc.dcc.common.core.util.stream.Streams;

import lombok.Getter;
import lombok.NonNull;
import lombok.val;

/**
 * Takes a fileName from the fileMeta ObjectNode from Portal, and parses certain attributes needed for loading
 */
public class PortalVCFFilenameParser {

  private static final String OBJECT_ID = "objectId";
  private static final String CALLER_ID = "callerId";
  private static final String DATE = "data";
  private static final String MUTATION_TYPE = "mutationType";
  private static final String MUTATION_SUB_TYPE = "subMutationType";
  private static final String FILE_TYPE = "fileType";
  private static final String REGEX_PATTERN =
      createFileNameRegex(OBJECT_ID, CALLER_ID, DATE, MUTATION_TYPE, MUTATION_SUB_TYPE, FILE_TYPE);

  private static final Pattern PATTERN = Pattern.compile(REGEX_PATTERN);

  private static String createFileNameRegex(String... strings) {
    val prefix = "(?<";
    val suffix = ">[^\\.]+)\\.";
    val end = ">.*)";
    return prefix
        + Streams.stream(strings).collect(Collectors.joining(suffix + prefix))
        + end;
  }

  @Getter
  private final String filename;
  private final Matcher matcher;

  public PortalVCFFilenameParser(@NonNull final String filename) {
    this.filename = filename;
    this.matcher = PATTERN.matcher(filename);
    if (matcher.find() == false) {
      throw new IllegalStateException(
          String.format("The input filename \"%s\" does not match the regex pattern: \n%s", filename, REGEX_PATTERN));
    }
  }

  public String getId() {
    return matcher.group(OBJECT_ID);
  }

  public String getCallerId() {
    return matcher.group(CALLER_ID);
  }

  public String getDate() {
    return matcher.group(DATE);
  }

  public String getMutationType() {
    return matcher.group(MUTATION_TYPE);
  }

  public String getMutationSubType() {
    return matcher.group(MUTATION_SUB_TYPE);
  }

  public String getFileType() {
    return matcher.group(FILE_TYPE);
  }

}
