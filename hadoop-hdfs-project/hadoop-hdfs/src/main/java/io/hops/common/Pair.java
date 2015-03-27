/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.common;

public class Pair<L, R> {

  private final L l;
  private final R r;

  public Pair(L l, R r) {
    this.l = l;
    this.r = r;
  }

  public L getL() {
    return l;
  }

  public R getR() {
    return r;
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 43 * hash + (this.l != null ? this.l.hashCode() : 0);
    hash = 43 * hash + (this.r != null ? this.r.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Pair<L, R> other = (Pair<L, R>) obj;
    if (this.l != other.l && (this.l == null || !this.l.equals(other.l))) {
      return false;
    }
    if (this.r != other.r && (this.r == null || !this.r.equals(other.r))) {
      return false;
    }
    return true;
  }
}
