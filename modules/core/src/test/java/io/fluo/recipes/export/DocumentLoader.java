/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.recipes.export;

import org.apache.commons.lang.StringUtils;

import io.fluo.api.types.TypedLoader;
import io.fluo.api.types.TypedTransactionBase;

public class DocumentLoader extends TypedLoader {

  String docid;
  String refs[];

  DocumentLoader(String docid, String... refs) {
    this.docid = docid;
    this.refs = refs;
  }

  @Override
  public void load(TypedTransactionBase tx, Context context) throws Exception {
    tx.mutate().row("d:" + docid).fam("content").qual("new").set(StringUtils.join(refs, " "));
  }
}
