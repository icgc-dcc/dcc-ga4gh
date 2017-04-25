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
package org.icgc.dcc.ga4gh.loader.vcf.enums;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum CallerTypes {

  MUSE_1_0rc_b391201_vcf("MUSE_1-0rc-b391201-vcf"), // 2 columns, objectIds. One of the sampleNames matches the objectId of the file. That could mean that sample is the normal
  MUSE_1_0rc_vcf("MUSE_1-0rc-vcf"), // objectIds, 2 columns, same as MUSE_1-0rc-b391201-vcf"
  broad_dRanger("broad-dRanger"), // 2 columns, format is <objectid>:N <objectid>:T
  broad_dRanger_10("broad-dRanger-10"), // 2 columns, format is <objectid>:N <objectid>:T
  broad_dRanger_11("broad-dRanger-11"), //
  broad_dRanger_13("broad-dRanger-13"), //
  broad_dRanger_14("broad-dRanger-14"), //
  broad_dRanger_snowman("broad-dRanger_snowman"), //2 columns, format is <objectid>:N <objectid>:T , with colon
  broad_dRanger_snowman_10("broad-dRanger_snowman-10"), // 2 columns, format is <objectid>:N <objectid>:T
  broad_dRanger_snowman_11("broad-dRanger_snowman-11"), //
  broad_dRanger_snowman_13("broad-dRanger_snowman-13"), //
  broad_dRanger_snowman_14("broad-dRanger_snowman-14"), //
  broad_mutect_v3("broad-mutect-v3"), // only one column, which shold be tumor...verify with junjun
  broad_snowman("broad-snowman"), // 2 columns, format is <objectid>N <objectid>T (notice, there is no COLON)
  broad_snowman_10("broad-snowman-10"), //2 columns, format is <objectid>N <objectid>T (notice, there is no COLON)
  broad_snowman_11("broad-snowman-11"), //
  broad_snowman_13("broad-snowman-13"), //
  broad_snowman_14("broad-snowman-14"), //
  consensus("consensus"), //
  dkfz_copyNumberEstimation_1_0_189("dkfz-copyNumberEstimation_1-0-189"), // Only one columns, which should be tumor. SampleName is actually TUMOR
  dkfz_copyNumberEstimation_1_0_189_1_hpc("dkfz-copyNumberEstimation_1-0-189-1-hpc"), //
  dkfz_copyNumberEstimation_1_0_189_hpc("dkfz-copyNumberEstimation_1-0-189-hpc"), //// Only one columns, which should be tumor. SampleName is actually TUMOR
  dkfz_copyNumberEstimation_1_0_189_hpc_fix("dkfz-copyNumberEstimation_1-0-189-hpc-fix"), // Only one columns, which should be tumor. SampleName is actually TUMOR
  dkfz_indelCalling_1_0_132_1("dkfz-indelCalling_1-0-132-1"), // 2 columns, but tumor is named TUMOR
  dkfz_indelCalling_1_0_132_1_hpc("dkfz-indelCalling_1-0-132-1-hpc"), // 2 columns, but tumor is named TUMOR
  dkfz_snvCalling_1_0_132_1("dkfz-snvCalling_1-0-132-1"), //  2 columns, but tumor is named TUMOR
  dkfz_snvCalling_1_0_132_1_hpc("dkfz-snvCalling_1-0-132-1-hpc"), // 2 columns, but tumor is named TUMOR
  embl_delly_1_0_0_preFilter("embl-delly_1-0-0-preFilter"), // 2 columns, both objectIds but neither match the objectid of the file. Needs more investigationg
  embl_delly_1_0_0_preFilter_hpc("embl-delly_1-0-0-preFilter-hpc"), //
  embl_delly_1_3_0_preFilter("embl-delly_1-3-0-preFilter"), // 2 columns. First is a "0" and the other is an objectId (which does NOT match the object id of the file)
  svcp_1_0_2("svcp_1-0-2"), //2 columns, but tumor is named TUMOR
  svcp_1_0_3("svcp_1-0-3"), //2 columns, but tumor is named TUMOR
  svcp_1_0_4("svcp_1-0-4"), //2 columns, but tumor is named TUMOR
  svcp_1_0_5("svcp_1-0-5"), // 2 columns, but tumor is named TUMOR
  svcp_1_0_6("svcp_1-0-6"), //2 columns, but tumor is named TUMOR
  svcp_1_0_7("svcp_1-0-7"), //2 columns, but tumor is named TUMOR
  svcp_1_0_8("svcp_1-0-8"), //2 columns, but tumor is named TUMOR
  svfix2_4_0_12("svfix2_4-0-12"); //2 columns, but tumor is named TUMOR

  // MUSE_1-0rc-b391201-vcf
  // MUSE_1-0rc-vcf
  // broad-dRanger
  // broad-dRanger-10
  // broad-dRanger-11
  // broad-dRanger-13
  // broad-dRanger-14
  // broad-dRanger_snowman
  // broad-dRanger_snowman-10
  // broad-dRanger_snowman-11
  // broad-dRanger_snowman-13
  // broad-dRanger_snowman-14
  // broad-mutect-v3
  // broad-snowman
  // broad-snowman-10
  // broad-snowman-11
  // broad-snowman-13
  // broad-snowman-14
  // consensus
  // dkfz-copyNumberEstimation_1-0-189
  // dkfz-copyNumberEstimation_1-0-189-1-hpc
  // dkfz-copyNumberEstimation_1-0-189-hpc
  // dkfz-copyNumberEstimation_1-0-189-hpc-fix
  // dkfz-indelCalling_1-0-132-1
  // dkfz-indelCalling_1-0-132-1-hpc
  // dkfz-snvCalling_1-0-132-1
  // dkfz-snvCalling_1-0-132-1-hpc
  // embl-delly_1-0-0-preFilter
  // embl-delly_1-0-0-preFilter-hpc
  // embl-delly_1-3-0-preFilter
  // svcp_1-0-2
  // svcp_1-0-3
  // svcp_1-0-4
  // svcp_1-0-5
  // svcp_1-0-6
  // svcp_1-0-7
  // svcp_1-0-8
  // svfix2_4-0-12

  @NonNull
  private String realName;

  public boolean equals(@NonNull final String name) {
    return getRealName().equals(name);
  }

  public boolean isIn(@NonNull final String name) {
    return name.matches("^" + getRealName() + ".*");
  }

  @Override
  public String toString() {
    return getRealName();
  }
}
