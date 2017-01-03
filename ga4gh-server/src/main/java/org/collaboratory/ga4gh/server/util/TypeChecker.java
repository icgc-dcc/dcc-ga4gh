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
package org.collaboratory.ga4gh.server.util;

import static lombok.AccessLevel.PRIVATE;

import java.util.Collection;
import java.util.Map;

import lombok.NoArgsConstructor;

@NoArgsConstructor(access = PRIVATE)
public final class TypeChecker {

  public static boolean isStringInteger(String input) {
    try {
      Integer.parseInt(input);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public static boolean isStringDouble(String input) {
    try {
      Double.parseDouble(input);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public static boolean isStringFloat(String input) {
    try {
      Float.parseFloat(input);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public static boolean isStringBoolean(String input) {
    try {
      Boolean.parseBoolean(input);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public static boolean isObjectBoolean(Object obj) {
    return obj instanceof Boolean;
  }

  public static boolean isObjectInteger(Object obj) {
    return obj instanceof Integer;
  }

  public static boolean isObjectDouble(Object obj) {
    return obj instanceof Double;
  }

  public static boolean isObjectFloat(Object obj) {
    return obj instanceof Float;
  }

  public static boolean isObjectMap(Object obj) {
    return obj instanceof Map<?, ?>;
  }

  public static boolean isObjectCollection(Object obj) {
    return obj instanceof Collection<?>;
  }

}
