/*
 * Copyright (c) 2017 The Ontario Institute for Cancer Research. All rights reserved.
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
package org.icgc.dcc.ga4gh.common.types;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.CompareState;

import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

@RequiredArgsConstructor
@Getter
public enum WorkflowTypes {

  CONSENSUS                        ("consensus"),
  BROAD_SNOWMAN                    ("broad-snowman"),
  BROAD_SNOWMAN_10                 ("broad-snowman-10"),
  BROAD_SNOWMAN_11                 ("broad-snowman-11"),
  BROAD_SNOWMAN_13                 ("broad-snowman-13"),
  BROAD_SNOWMAN_14                 ("broad-snowman-14"),
  BROAD_MUTECT_V3                  ("broad-mutect-v3"),
  SVCP_1_0_2                       ("svcp_1-0-2"),
  SVCP_1_0_3                       ("svcp_1-0-3"),
  SVCP_1_0_4                       ("svcp_1-0-4"),
  SVCP_1_0_5                       ("svcp_1-0-5"),
  SVCP_1_0_6                       ("svcp_1-0-6"),
  SVCP_1_0_7                       ("svcp_1-0-7"),
  SVCP_1_0_8                       ("svcp_1-0-8"),
  MUSE_1_0RC_B391201_VCF           ("MUSE_1-0rc-b391201-vcf"),
  MUSE_1_0RC_VCF                   ("MUSE_1-0rc-vcf"),
  DKFZ_SNVCALLING_1_0_132_1        ("dkfz-snvCalling_1-0-132-1"),
  DKFZ_SNVCALLING_1_0_132_1_HPC    ("dkfz-snvCalling_1-0-132-1-hpc"),
  DKFZ_INDELCALLING_1_0_132_1      ("dkfz-indelCalling_1-0-132-1"),
  DKFZ_INDELCALLING_1_0_132_1_HPC  ("dkfz-indelCalling_1-0-132-1-hpc"),
  UNKNOWN("unknown");


  private static final boolean DEFAULT_NO_ERRORS_FLAG = false;
  private static final WorkflowTypes[] WORKFLOW_TYPES = values();

  @NonNull
  private final String name;

  public boolean equals(@NonNull final String name) {
    return this.getName().equals(name);
  }

  public boolean isAtBeginningOf(@NonNull final String name) {
    return name.matches("^" + this.getName() + ".*");
  }

  public boolean isIn(@NonNull final String name) {
    return name.contains(this.getName());
  }

  @Override
  public String toString() {
    return this.getName();
  }

  public CompareState compareState(WorkflowTypes other){
    return CompareState.getState(this, other);
  }

  private static Set<String> set(String ... strings){
    return stream(strings).collect(toImmutableSet());
  }

  private static WorkflowTypes parse(String name, Predicate<WorkflowTypes> predicate){
    for (val v : WORKFLOW_TYPES){
      if (predicate.test(v)){
        return v;
      }
    }
    return WorkflowTypes.UNKNOWN;
  }

  public static WorkflowTypes parseStartsWith(String name, boolean check){
    val workflowType = parse(name, w -> w.isAtBeginningOf(name) );
    if (check){
      checkState(workflowType != WorkflowTypes.UNKNOWN, "%s does not contain a workflowType that starts with [%s]", WorkflowTypes.class.getName(), name);
    }
    return workflowType;
  }

  public static WorkflowTypes parseMatch(String name, boolean check){
    val workflowType = parse(name, w -> w.equals(name) );
    if (check) {
      checkState(workflowType != WorkflowTypes.UNKNOWN, "The name [%s] does not match any workflowType name in %s", name,
          WorkflowTypes.class.getName());
    }
    return workflowType;
  }

  public static WorkflowTypes parseContains(String name, boolean check){
    val workflowType = parse(name, w -> w.isIn(name) );
    if (check) {
      checkState(workflowType != WorkflowTypes.UNKNOWN, "The name [%s] does exist in any of the workflowTypes in %s", name,
          WorkflowTypes.class.getName());
    }
    return workflowType;
  }

}
