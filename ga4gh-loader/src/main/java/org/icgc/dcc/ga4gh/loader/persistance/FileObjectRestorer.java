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

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;

import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor(access =  PRIVATE)
public class FileObjectRestorer<T extends Serializable> implements ObjectRestorer<Path, T> {

  @NonNull
  @Getter
  private final Path persistedPath;

  @Override @SuppressWarnings("unchecked")
  public T restore() throws IOException, ClassNotFoundException {
    if (isPersisted()){
      return (T) ObjectPersistance.restore(getPersistedPath());
    } else {
      throw new IllegalStateException(String.format("Cannot restore if persistedFilename [%s] DNE", getPersistedPath().toString()));
    }
  }

  @Override public void store(T t) throws IOException {
    ObjectPersistance.<T>store(t,getPersistedPath());
  }

  @Override public void clean() throws IOException {
    Files.deleteIfExists(getPersistedPath());
  }

  @Override public boolean isPersisted(){
    return Files.exists(getPersistedPath());
  }

  public static <T extends Serializable> FileObjectRestorer<T> newFileObjectRestorer(Path persistedPath){
    return new FileObjectRestorer<T>(persistedPath);
  }

}
