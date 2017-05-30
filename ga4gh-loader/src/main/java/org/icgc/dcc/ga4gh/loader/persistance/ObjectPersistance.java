
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

package org.icgc.dcc.ga4gh.loader.persistance;

import lombok.Cleanup;
import lombok.NoArgsConstructor;
import lombok.val;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkState;
import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public class ObjectPersistance {

  public static <T extends Serializable> void store(final T t, final String filename) throws IOException {
    store(t, Paths.get(filename));
  }

  public static <T extends Serializable> void store(final T t, final Path pathname) throws IOException {
    @Cleanup
    val fout = new FileOutputStream(pathname.toFile());
    val oos = new ObjectOutputStream(fout);
    oos.writeObject(t);
  }

  public static Object restore(final String filename) throws ClassNotFoundException, IOException {
    return restore(Paths.get(filename));
  }

  public static Object restore(final Path pathname) throws ClassNotFoundException, IOException {
    val file = pathname.toFile();
    checkState(file.exists(), "The File[{}] DNE", pathname.toString());
    checkState(file.isFile(), "The Path[{}] is not a file", pathname.toString());
    @Cleanup
    val fin = new FileInputStream(file);
    val ois = new ObjectInputStream(fin);
    return ois.readObject();
  }

}
