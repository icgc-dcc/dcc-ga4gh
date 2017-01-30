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
package org.collaboratory.ga4gh.loader.enums;

import lombok.NonNull;

public enum CallerTypes {
  dkfz_indelCalling_1_0_132_1, dkfz_indelCalling_1_0_132_1_hpc, dkfz_snvCalling_1_0_132_1, dkfz_snvCalling_1_0_132_1_hpc, dkfz_copyNumberEstimation_1_0_189_hpc, dkfz_copyNumberEstimation_1_0_189_hpc_fix, dkfz_copyNumberEstimation_1_0_189, dkfz_copyNumberEstimation_1_0_189_1_hpc, broad_dRanger, broad_dRanger_10, broad_dRanger_11, broad_dRanger_13, broad_dRanger_14, broad_snowman, broad_snowman_10, broad_snowman_11, broad_snowman_13, broad_snowman_14, broad_dRanger_snowman, broad_dRanger_snowman_10, broad_dRanger_snowman_11, broad_dRanger_snowman_13, broad_dRanger_snowman_14, svcp_1_0_2, svcp_1_0_3, svcp_1_0_4, svcp_1_0_5, svcp_1_0_6, svcp_1_0_7, svcp_1_0_8, embl_delly_1_0_0_preFilter_hpc, embl_delly_1_0_0_preFilter, embl_delly_1_3_0_preFilter, MUSE_1_0rc_b391201_vcf, MUSE_1_0rc_vcf, svfix2_4_0_12, broad_mutect_v3, consensus;

  public boolean equals(@NonNull final String name) {
    return name().equals(name);
  }

  public boolean isIn(@NonNull final String name) {
    return name.matches("^" + name() + ".*");
  }

  @Override
  public String toString() {
    return name();
  }
}
