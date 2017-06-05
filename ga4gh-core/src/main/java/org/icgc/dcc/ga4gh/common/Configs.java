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

package org.icgc.dcc.ga4gh.common;

import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import java.lang.reflect.Field;
import java.util.Comparator;

import static java.lang.String.format;
import static java.lang.reflect.Modifier.isFinal;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

@NoArgsConstructor(access = PRIVATE)
public class Configs {

  @SneakyThrows
  public static String createPublicFinalStaticFieldsDescription(Class<?> clazz) {
    val publicFinalStaticFields = stream(clazz.getFields())
        .filter(x -> isPublic(x.getModifiers()))
        .filter(x -> isFinal(x.getModifiers()))
        .filter(x -> isStatic(x.getModifiers()))
        .sorted(new FieldComparator())
        .collect(toImmutableList());
    val sb = new StringBuilder();
    for (val field : publicFinalStaticFields){
      val fieldName = field.getName();
      val fieldValue = field.get(null);
      sb.append(format("%s : %s\n", fieldName, fieldValue));
    }
    return sb.toString();
  }

  public static class FieldComparator implements Comparator<Field> {

    @Override public int compare(Field o1, Field o2) {
      return o1.getName().compareTo(o2.getName());
    }

  }


}
