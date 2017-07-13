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

package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * General xml utilities.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class XMLUtils {
  /**
   * Exception that reflects an invalid XML document.
   */
  static public class InvalidXmlException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public InvalidXmlException(String s) {
      super(s);
    }
  }
  
  /**
   * Exception that reflects a string that cannot be unmangled.
   */
  public static class UnmanglingError extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    public UnmanglingError(String str, Exception e) {
      super(str, e);
    }
    
    public UnmanglingError(String str) {
      super(str);
    }
  }
  

  /**
   * Given a code point, determine if it should be mangled before being
   * represented in an XML document.
   * <p/>
   * Any code point that isn't valid in XML must be mangled.
   * See http://en.wikipedia.org/wiki/Valid_characters_in_XML for a
   * quick reference, or the w3 standard for the authoritative reference.
   *
   * @param cp
   *     The code point
   * @return True if the code point should be mangled
   */
  private static boolean codePointMustBeMangled(int cp) {
    if (cp < 0x20) {
      return ((cp != 0x9) && (cp != 0xa) && (cp != 0xd));
    } else if ((0xd7ff < cp) && (cp < 0xe000)) {
      return true;
    } else if ((cp == 0xfffe) || (cp == 0xffff)) {
      return true;
    } else if (cp == 0x5c) {
      // we mangle backslash to simplify decoding... it's
      // easier if backslashes always begin mangled sequences. 
      return true;
    }
    return false;
  }

  private static int NUM_SLASH_POSITIONS = 4;

  private static String mangleCodePoint(int cp) {
    return String.format("\\%0" + NUM_SLASH_POSITIONS + "x;", cp);
  }

  /**
   * Mangle a string so that it can be represented in an XML document.
   * <p/>
   * There are three kinds of code points in XML:
   * - Those that can be represented normally,
   * - Those that have to be escaped (for example, & must be represented
   * as &amp;)
   * - Those that cannot be represented at all in XML.
   * <p/>
   * The built-in SAX functions will handle the first two types for us just
   * fine.  However, sometimes we come across a code point of the third type.
   * In this case, we have to mangle the string in order to represent it at
   * all.  We also mangle backslash to avoid confusing a backslash in the
   * string with part our escape sequence.
   * <p/>
   * The encoding used here is as follows: an illegal code point is
   * represented as '\ABCD;', where ABCD is the hexadecimal value of
   * the code point.
   *
   * @param str
   *     The input string.
   * @return The mangled string.
   */
  public static String mangleXmlString(String str) {
    final StringBuilder bld = new StringBuilder();
    final int length = str.length();
    for (int offset = 0; offset < length; ) {
      final int cp = str.codePointAt(offset);
      final int len = Character.charCount(cp);
      if (codePointMustBeMangled(cp)) {
        bld.append(mangleCodePoint(cp));
      } else {
        for (int i = 0; i < len; i++) {
          bld.append(str.charAt(offset + i));
        }
      }
      offset += len;
    }
    return bld.toString();
  }

  /**
   * Demangle a string from an XML document.
   * See {@link #mangleXmlString(String)} for a description of the mangling
   * format.
   *
   * @param str
   *     The string to be demangled.
   * @return The unmangled string
   * @throws UnmanglingError
   *     if the input is malformed.
   */
  public static String unmangleXmlString(String str) throws UnmanglingError {
    int slashPosition = -1;
    String escapedCp = "";
    StringBuilder bld = new StringBuilder();
    for (int i = 0; i < str.length(); i++) {
      char ch = str.charAt(i);
      if ((slashPosition >= 0) && (slashPosition < NUM_SLASH_POSITIONS)) {
        escapedCp += ch;
        ++slashPosition;
      } else if (slashPosition == NUM_SLASH_POSITIONS) {
        if (ch != ';') {
          throw new UnmanglingError("unterminated code point escape: " +
              "expected semicolon at end.");
        }
        try {
          bld.appendCodePoint(Integer.parseInt(escapedCp, 16));
        } catch (NumberFormatException e) {
          throw new UnmanglingError("error parsing unmangling escape code", e);
        }
        escapedCp = "";
        slashPosition = -1;
      } else if (ch == '\\') {
        slashPosition = 0;
      } else {
        bld.append(ch);
      }
    }
    if (slashPosition != -1) {
      throw new UnmanglingError("unterminated code point escape: string " +
          "broke off in the middle");
    }
    return bld.toString();
  }
  
  /**
   * Add a SAX tag with a string inside.
   *
   * @param contentHandler
   *     the SAX content handler
   * @param tag
   *     the element tag to use
   * @param value
   *     the string to put inside the tag
   */
  public static void addSaxString(ContentHandler contentHandler, String tag,
      String val) throws SAXException {
    contentHandler.startElement("", "", tag, new AttributesImpl());
    char c[] = mangleXmlString(val).toCharArray();
    contentHandler.characters(c, 0, c.length);
    contentHandler.endElement("", "", tag);
  }

  /**
   * Represents a bag of key-value pairs encountered during parsing an XML
   * file.
   */
  static public class Stanza {
    private TreeMap<String, LinkedList<Stanza>> subtrees;

    /**
     * The unmangled value of this stanza.
     */
    private String value;
    
    public Stanza() {
      subtrees = new TreeMap<>();
      value = "";
    }
    
    public void setValue(String value) {
      this.value = value;
    }
    
    public String getValue() {
      return this.value;
    }
    
    /**
     * Discover if a stanza has a given entry.
     *
     * @param name
     *     entry to look for
     * @return true if the entry was found
     */
    public boolean hasChildren(String name) {
      return subtrees.containsKey(name);
    }
    
    /**
     * Pull an entry from a stanza.
     *
     * @param name
     *     entry to look for
     * @return the entry
     */
    public List<Stanza> getChildren(String name) throws InvalidXmlException {
      LinkedList<Stanza> children = subtrees.get(name);
      if (children == null) {
        throw new InvalidXmlException("no entry found for " + name);
      }
      return children;
    }
    
    /**
     * Pull a string entry from a stanza.
     *
     * @param name
     *     entry to look for
     * @return the entry
     */
    public String getValue(String name) throws InvalidXmlException {
      if (!subtrees.containsKey(name)) {
        throw new InvalidXmlException("no entry found for " + name);
      }
      LinkedList<Stanza> l = subtrees.get(name);
      if (l.size() != 1) {
        throw new InvalidXmlException("More than one value found for " + name);
      }
      return l.get(0).getValue();
    }
    
    /**
     * Add an entry to a stanza.
     *
     * @param name
     *     name of the entry to add
     * @param child
     *     the entry to add
     */
    public void addChild(String name, Stanza child) {
      LinkedList<Stanza> l;
      if (subtrees.containsKey(name)) {
        l = subtrees.get(name);
      } else {
        l = new LinkedList<>();
        subtrees.put(name, l);
      }
      l.add(child);
    }
    
    /**
     * Convert a stanza to a human-readable string.
     */
    @Override
    public String toString() {
      StringBuilder bld = new StringBuilder();
      bld.append("{");
      if (!value.equals("")) {
        bld.append("\"").append(value).append("\"");
      }
      String prefix = "";
      for (Map.Entry<String, LinkedList<Stanza>> entry : subtrees.entrySet()) {
        String key = entry.getKey();
        LinkedList<Stanza> ll = entry.getValue();
        for (Stanza child : ll) {
          bld.append(prefix);
          bld.append("<").append(key).append(">");
          bld.append(child.toString());
          prefix = ", ";
        }
      }
      bld.append("}");
      return bld.toString();
    }
  }
}