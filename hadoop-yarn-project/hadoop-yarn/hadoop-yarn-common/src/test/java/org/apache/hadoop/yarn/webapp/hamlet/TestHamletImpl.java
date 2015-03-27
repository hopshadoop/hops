/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.webapp.hamlet;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.CoreAttrs;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.H1;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.LINK;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.SCRIPT;
import org.junit.Test;

import java.io.PrintWriter;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestHamletImpl {
  /**
   * Test the generic implementation methods
   *
   * @see TestHamlet for Hamlet syntax
   */
  @Test
  public void testGeneric() {
    PrintWriter out = spy(new PrintWriter(System.out));
    HamletImpl hi = new HamletImpl(out, 0, false);
    hi.
        root("start")._attr("name", "value").
        _("start text").
        elem("sub")._attr("name", "value").
        _("sub text")._().
        elem("sub1")._noEndTag()._attr("boolean", null).
        _("sub1text")._().
        _("start text2").
        elem("pre")._pre().
        _("pre text").
        elem("i")._inline()._("inline")._()._().
        elem("i")._inline()._("inline after pre")._().
        _("start text3").
        elem("sub2").
        _("sub2text")._().
        elem("sub3")._noEndTag().
        _("sub3text")._().
        elem("sub4")._noEndTag().
        elem("i")._inline()._("inline")._().
        _("sub4text")._()._();

    out.flush();
    assertEquals(0, hi.nestLevel);
    assertEquals(20, hi.indents);
    verify(out).print("<start");
    verify(out, times(2)).print(" name=\"value\"");
    verify(out).print(" boolean");
    verify(out).print("</start>");
    verify(out, never()).print("</sub1>");
    verify(out, never()).print("</sub3>");
    verify(out, never()).print("</sub4>");
  }

  @Test
  public void testSetSelector() {
    CoreAttrs e = mock(CoreAttrs.class);
    HamletImpl.setSelector(e, "#id.class");

    verify(e).$id("id");
    verify(e).$class("class");

    H1 t = mock(H1.class);
    HamletImpl.setSelector(t, "#id.class")._("heading");

    verify(t).$id("id");
    verify(t).$class("class");
    verify(t)._("heading");
  }

  @Test
  public void testSetLinkHref() {
    LINK link = mock(LINK.class);
    HamletImpl.setLinkHref(link, "uri");
    HamletImpl.setLinkHref(link, "style.css");

    verify(link).$href("uri");
    verify(link).$rel("stylesheet");
    verify(link).$href("style.css");

    verifyNoMoreInteractions(link);
  }

  @Test
  public void testSetScriptSrc() {
    SCRIPT script = mock(SCRIPT.class);
    HamletImpl.setScriptSrc(script, "uri");
    HamletImpl.setScriptSrc(script, "script.js");

    verify(script).$src("uri");
    verify(script).$type("text/javascript");
    verify(script).$src("script.js");

    verifyNoMoreInteractions(script);
  }
}
