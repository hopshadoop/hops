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
package org.apache.hadoop.yarn.api.resource;

import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.NodeAttributeOpCode;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.AbstractConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.And;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.Or;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.SingleConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParseException;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.SourceTags;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.TargetConstraintParser;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.ConstraintParser;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.CardinalityConstraintParser;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.ConjunctionConstraintParser;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.MultipleConstraintsTokenizer;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.SourceTagsTokenizer;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.ConstraintTokenizer;

import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.allocationTag;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.and;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.cardinality;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.or;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetIn;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetNodeAttribute;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetNotIn;

import org.junit.Assert;
import org.junit.Test;

/**
 * Class to test placement constraint parser.
 */
public class TestPlacementConstraintParser {

  @Test
  public void testTargetExpressionParser()
      throws PlacementConstraintParseException {
    String expressionStr;
    ConstraintParser parser;
    AbstractConstraint constraint;
    SingleConstraint single;

    // Anti-affinity with single target tag
    // NOTIN,NODE,foo
    expressionStr = "NOTIN, NODE, foo";
    parser = new TargetConstraintParser(expressionStr);
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof SingleConstraint);
    single = (SingleConstraint) constraint;
    Assert.assertEquals("node", single.getScope());
    Assert.assertEquals(0, single.getMinCardinality());
    Assert.assertEquals(0, single.getMaxCardinality());
    verifyConstraintToString(expressionStr, constraint);

    // lower cases is also valid
    expressionStr = "notin, node, foo";
    parser = new TargetConstraintParser(expressionStr);
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof SingleConstraint);
    single = (SingleConstraint) constraint;
    Assert.assertEquals("node", single.getScope());
    Assert.assertEquals(0, single.getMinCardinality());
    Assert.assertEquals(0, single.getMaxCardinality());
    verifyConstraintToString(expressionStr, constraint);

    // Affinity with single target tag
    // IN,NODE,foo
    expressionStr = "IN, NODE, foo";
    parser = new TargetConstraintParser(expressionStr);
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof SingleConstraint);
    single = (SingleConstraint) constraint;
    Assert.assertEquals("node", single.getScope());
    Assert.assertEquals(1, single.getMinCardinality());
    Assert.assertEquals(Integer.MAX_VALUE, single.getMaxCardinality());
    verifyConstraintToString(expressionStr, constraint);

    // Anti-affinity with multiple target tags
    // NOTIN,NDOE,foo,bar,exp
    expressionStr = "NOTIN, NODE, foo, bar, exp";
    parser = new TargetConstraintParser(expressionStr);
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof SingleConstraint);
    single = (SingleConstraint) constraint;
    Assert.assertEquals("node", single.getScope());
    Assert.assertEquals(0, single.getMinCardinality());
    Assert.assertEquals(0, single.getMaxCardinality());
    Assert.assertEquals(1, single.getTargetExpressions().size());
    TargetExpression exp =
        single.getTargetExpressions().iterator().next();
    Assert.assertEquals("ALLOCATION_TAG", exp.getTargetType().toString());
    Assert.assertEquals(3, exp.getTargetValues().size());
    verifyConstraintToString(expressionStr, constraint);

    // Invalid OP
    parser = new TargetConstraintParser("XYZ, NODE, foo");
    try {
      parser.parse();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof PlacementConstraintParseException);
      Assert.assertTrue(e.getMessage().contains("expecting in or notin"));
    }
  }

  @Test
  public void testCardinalityConstraintParser()
      throws PlacementConstraintParseException {
    String expressionExpr;
    ConstraintParser parser;
    AbstractConstraint constraint;
    SingleConstraint single;

    // cardinality,NODE,foo,0,1
    expressionExpr = "cardinality, NODE, foo, 0, 1";
    parser = new CardinalityConstraintParser(expressionExpr);
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof SingleConstraint);
    single = (SingleConstraint) constraint;
    Assert.assertEquals("node", single.getScope());
    Assert.assertEquals(0, single.getMinCardinality());
    Assert.assertEquals(1, single.getMaxCardinality());
    Assert.assertEquals(1, single.getTargetExpressions().size());
    TargetExpression exp =
        single.getTargetExpressions().iterator().next();
    Assert.assertEquals("ALLOCATION_TAG", exp.getTargetType().toString());
    Assert.assertEquals(1, exp.getTargetValues().size());
    Assert.assertEquals("foo", exp.getTargetValues().iterator().next());
    verifyConstraintToString(expressionExpr, constraint);

    // cardinality,NODE,foo,bar,moo,0,1
    expressionExpr = "cardinality,RACK,foo,bar,moo,0,1";
    parser = new CardinalityConstraintParser(expressionExpr);
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof SingleConstraint);
    single = (SingleConstraint) constraint;
    Assert.assertEquals("rack", single.getScope());
    Assert.assertEquals(0, single.getMinCardinality());
    Assert.assertEquals(1, single.getMaxCardinality());
    Assert.assertEquals(1, single.getTargetExpressions().size());
    exp = single.getTargetExpressions().iterator().next();
    Assert.assertEquals("ALLOCATION_TAG", exp.getTargetType().toString());
    Assert.assertEquals(3, exp.getTargetValues().size());
    Set<String> expectedTags = Sets.newHashSet("foo", "bar", "moo");
    Assert.assertTrue(Sets.difference(expectedTags, exp.getTargetValues())
        .isEmpty());
    verifyConstraintToString(expressionExpr, constraint);

    // Invalid scope string
    try {
      parser = new CardinalityConstraintParser(
          "cardinality,NOWHERE,foo,bar,moo,0,1");
      parser.parse();
      Assert.fail("Expecting a parsing failure!");
    } catch (PlacementConstraintParseException e) {
      Assert.assertTrue(e.getMessage()
          .contains("expecting scope to node or rack, but met NOWHERE"));
    }

    // Invalid number of expression elements
    try {
      parser = new CardinalityConstraintParser(
          "cardinality,NODE,0,1");
      parser.parse();
      Assert.fail("Expecting a parsing failure!");
    } catch (PlacementConstraintParseException e) {
      Assert.assertTrue(e.getMessage()
          .contains("at least 5 elements, but only 4 is given"));
    }
  }

  @Test
  public void testAndConstraintParser()
      throws PlacementConstraintParseException {
    String expressionExpr;
    ConstraintParser parser;
    AbstractConstraint constraint;
    And and;

    expressionExpr = "AND(NOTIN,NODE,foo:NOTIN,NODE,bar)";
    parser = new ConjunctionConstraintParser(expressionExpr);
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof And);
    and = (And) constraint;
    Assert.assertEquals(2, and.getChildren().size());
    verifyConstraintToString(expressionExpr, constraint);

    expressionExpr = "AND(NOTIN,NODE,foo:cardinality,NODE,foo,0,1)";
    parser = new ConjunctionConstraintParser(expressionExpr);
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof And);
    Assert.assertEquals(2, and.getChildren().size());
    verifyConstraintToString(expressionExpr, constraint);

    expressionExpr =
        "AND(NOTIN,NODE,foo:AND(NOTIN,NODE,foo:cardinality,NODE,foo,0,1))";
    parser = new ConjunctionConstraintParser(expressionExpr);
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof And);
    and = (And) constraint;
    Assert.assertTrue(and.getChildren().get(0) instanceof SingleConstraint);
    Assert.assertTrue(and.getChildren().get(1) instanceof And);
    and = (And) and.getChildren().get(1);
    Assert.assertEquals(2, and.getChildren().size());
    verifyConstraintToString(expressionExpr, constraint);
  }

  @Test
  public void testOrConstraintParser()
      throws PlacementConstraintParseException {
    String expressionExpr;
    ConstraintParser parser;
    AbstractConstraint constraint;
    Or or;

    expressionExpr = "OR(NOTIN,NODE,foo:NOTIN,NODE,bar)";
    parser = new ConjunctionConstraintParser(expressionExpr);
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof Or);
    or = (Or) constraint;
    Assert.assertEquals(2, or.getChildren().size());
    verifyConstraintToString(expressionExpr, constraint);

    expressionExpr = "OR(NOTIN,NODE,foo:cardinality,NODE,foo,0,1)";
    parser = new ConjunctionConstraintParser(expressionExpr);
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof Or);
    Assert.assertEquals(2, or.getChildren().size());
    verifyConstraintToString(expressionExpr, constraint);

    expressionExpr =
        "OR(NOTIN,NODE,foo:OR(NOTIN,NODE,foo:cardinality,NODE,foo,0,1))";
    parser = new ConjunctionConstraintParser(expressionExpr);
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof Or);
    or = (Or) constraint;
    Assert.assertTrue(or.getChildren().get(0) instanceof SingleConstraint);
    Assert.assertTrue(or.getChildren().get(1) instanceof Or);
    or = (Or) or.getChildren().get(1);
    Assert.assertEquals(2, or.getChildren().size());
    verifyConstraintToString(expressionExpr, constraint);
  }

  @Test
  public void testMultipleConstraintsTokenizer()
      throws PlacementConstraintParseException {
    MultipleConstraintsTokenizer ct;
    SourceTagsTokenizer st;
    TokenizerTester mp;

    ct = new MultipleConstraintsTokenizer(
        "foo=1,A1,A2,A3:bar=2,B1,B2:moo=3,C1,C2");
    mp = new TokenizerTester(ct,
        "foo=1,A1,A2,A3", "bar=2,B1,B2", "moo=3,C1,C2");
    mp.verify();

    ct = new MultipleConstraintsTokenizer(
        "foo=1,AND(A2:A3):bar=2,OR(B1:AND(B2:B3)):moo=3,C1,C2");
    mp = new TokenizerTester(ct,
        "foo=1,AND(A2:A3)", "bar=2,OR(B1:AND(B2:B3))", "moo=3,C1,C2");
    mp.verify();

    ct = new MultipleConstraintsTokenizer("A:B:C");
    mp = new TokenizerTester(ct, "A", "B", "C");
    mp.verify();

    ct = new MultipleConstraintsTokenizer("A:AND(B:C):D");
    mp = new TokenizerTester(ct, "A", "AND(B:C)", "D");
    mp.verify();

    ct = new MultipleConstraintsTokenizer("A:AND(B:OR(C:D)):E");
    mp = new TokenizerTester(ct, "A", "AND(B:OR(C:D))", "E");
    mp.verify();

    ct = new MultipleConstraintsTokenizer("A:AND(B:OR(C:D)):E");
    mp = new TokenizerTester(ct, "A", "AND(B:OR(C:D))", "E");
    mp.verify();

    st = new SourceTagsTokenizer("A=4");
    mp = new TokenizerTester(st, "A", "4");
    mp.verify();

    try {
      st = new SourceTagsTokenizer("A=B");
      mp = new TokenizerTester(st, "A", "B");
      mp.verify();
      Assert.fail("Expecting a parsing failure");
    } catch (PlacementConstraintParseException e) {
      Assert.assertTrue(e.getMessage()
          .contains("Value of the expression must be an integer"));
    }
  }

  private static class TokenizerTester {

    private ConstraintTokenizer tokenizer;
    private String[] expectedExtractions;

    protected TokenizerTester(ConstraintTokenizer tk,
        String... expctedStrings) {
      this.tokenizer = tk;
      this.expectedExtractions = expctedStrings;
    }

    void verify()
        throws PlacementConstraintParseException {
      tokenizer.validate();
      int i = 0;
      while (tokenizer.hasMoreElements()) {
        String current = tokenizer.nextElement();
        Assert.assertTrue(i < expectedExtractions.length);
        Assert.assertEquals(expectedExtractions[i], current);
        i++;
      }
    }
  }

  @Test
  public void testParsePlacementSpec()
      throws PlacementConstraintParseException {
    Map<SourceTags, PlacementConstraint> result;
    PlacementConstraint expectedPc1, expectedPc2;
    PlacementConstraint actualPc1, actualPc2;
    SourceTags tag1, tag2;

    // A single anti-affinity constraint
    result = PlacementConstraintParser
        .parsePlacementSpec("foo=3,notin,node,foo");
    Assert.assertEquals(1, result.size());
    tag1 = result.keySet().iterator().next();
    Assert.assertEquals("foo", tag1.getTag());
    Assert.assertEquals(3, tag1.getNumOfAllocations());
    expectedPc1 = targetNotIn("node", allocationTag("foo")).build();
    actualPc1 = result.values().iterator().next();
    Assert.assertEquals(expectedPc1, actualPc1);

    // Upper case
    result = PlacementConstraintParser
        .parsePlacementSpec("foo=3,NOTIN,NODE,foo");
    Assert.assertEquals(1, result.size());
    tag1 = result.keySet().iterator().next();
    Assert.assertEquals("foo", tag1.getTag());
    Assert.assertEquals(3, tag1.getNumOfAllocations());
    expectedPc1 = targetNotIn("node", allocationTag("foo")).build();
    actualPc1 = result.values().iterator().next();
    Assert.assertEquals(expectedPc1, actualPc1);

    // A single cardinality constraint
    result = PlacementConstraintParser
        .parsePlacementSpec("foo=10,cardinality,node,foo,bar,0,100");
    Assert.assertEquals(1, result.size());
    tag1 = result.keySet().iterator().next();
    Assert.assertEquals("foo", tag1.getTag());
    Assert.assertEquals(10, tag1.getNumOfAllocations());
    expectedPc1 = cardinality("node", 0, 100, "foo", "bar").build();
    Assert.assertEquals(expectedPc1, result.values().iterator().next());

    // Two constraint expressions
    result = PlacementConstraintParser
        .parsePlacementSpec("foo=3,notin,node,foo:bar=2,in,node,foo");
    Assert.assertEquals(2, result.size());
    Iterator<SourceTags> keyIt = result.keySet().iterator();
    tag1 = keyIt.next();
    Assert.assertEquals("foo", tag1.getTag());
    Assert.assertEquals(3, tag1.getNumOfAllocations());
    tag2 = keyIt.next();
    Assert.assertEquals("bar", tag2.getTag());
    Assert.assertEquals(2, tag2.getNumOfAllocations());
    Iterator<PlacementConstraint> valueIt = result.values().iterator();
    expectedPc1 = targetNotIn("node", allocationTag("foo")).build();
    expectedPc2 = targetIn("node", allocationTag("foo")).build();
    Assert.assertEquals(expectedPc1, valueIt.next());
    Assert.assertEquals(expectedPc2, valueIt.next());

    // And constraint
    result = PlacementConstraintParser
        .parsePlacementSpec("foo=1000,and(notin,node,bar:in,node,foo)");
    Assert.assertEquals(1, result.size());
    keyIt = result.keySet().iterator();
    tag1 = keyIt.next();
    Assert.assertEquals("foo", tag1.getTag());
    Assert.assertEquals(1000, tag1.getNumOfAllocations());
    actualPc1 = result.values().iterator().next();
    expectedPc1 = and(targetNotIn("node", allocationTag("bar")),
        targetIn("node", allocationTag("foo"))).build();
    Assert.assertEquals(expectedPc1, actualPc1);

    // Multiple constraints with nested forms.
    result = PlacementConstraintParser.parsePlacementSpec(
            "foo=1000,and(notin,node,bar:or(in,node,foo:in,node,moo))"
                + ":bar=200,notin,node,foo");
    Assert.assertEquals(2, result.size());
    keyIt = result.keySet().iterator();
    tag1 = keyIt.next();
    tag2 = keyIt.next();
    Assert.assertEquals("foo", tag1.getTag());
    Assert.assertEquals(1000, tag1.getNumOfAllocations());
    Assert.assertEquals("bar", tag2.getTag());
    Assert.assertEquals(200, tag2.getNumOfAllocations());
    valueIt = result.values().iterator();
    actualPc1 = valueIt.next();
    actualPc2 = valueIt.next();

    expectedPc1 = and(targetNotIn("node", allocationTag("bar")),
        or(targetIn("node", allocationTag("foo")),
            targetIn("node", allocationTag("moo")))).build();
    Assert.assertEquals(actualPc1, expectedPc1);
    expectedPc2 = targetNotIn("node", allocationTag("foo")).build();
    Assert.assertEquals(expectedPc2, actualPc2);
  }

  // We verify the toString result by parsing it again
  // instead of raw string comparing. This is because internally
  // we are not storing tags strictly to its original order, so
  // the toString result might have different ordering with the
  // input expression.
  private void verifyConstraintToString(String inputExpr,
      AbstractConstraint constraint) {
    String constrainExpr = constraint.toString();
    System.out.println("Input:    " + inputExpr
        .toLowerCase().replaceAll(" ", ""));
    System.out.println("ToString: " + constrainExpr);
    try {
      PlacementConstraintParser.parseExpression(constrainExpr);
    } catch (PlacementConstraintParseException e) {
      Assert.fail("The parser is unable to parse the expression: "
          + constrainExpr + ", caused by: " + e.getMessage());
    }
  }

  @Test
  public void testParseNodeAttributeSpec()
      throws PlacementConstraintParseException {
    Map<SourceTags, PlacementConstraint> result;
    PlacementConstraint.AbstractConstraint expectedPc1, expectedPc2;
    PlacementConstraint actualPc1, actualPc2;

    // A single node attribute constraint
    result = PlacementConstraintParser
        .parsePlacementSpec("xyz=4,rm.yarn.io/foo=true");
    Assert.assertEquals(1, result.size());
    TargetExpression target = PlacementTargets
        .nodeAttribute("rm.yarn.io/foo", "true");
    expectedPc1 = targetNodeAttribute("node", NodeAttributeOpCode.EQ, target);

    actualPc1 = result.values().iterator().next();
    Assert.assertEquals(expectedPc1, actualPc1.getConstraintExpr());

    // A single node attribute constraint
    result = PlacementConstraintParser
        .parsePlacementSpec("xyz=3,rm.yarn.io/foo!=abc");
    Assert.assertEquals(1, result.size());
    target = PlacementTargets
        .nodeAttribute("rm.yarn.io/foo", "abc");
    expectedPc1 = targetNodeAttribute("node", NodeAttributeOpCode.NE, target);

    actualPc1 = result.values().iterator().next();
    Assert.assertEquals(expectedPc1, actualPc1.getConstraintExpr());

    actualPc1 = result.values().iterator().next();
    Assert.assertEquals(expectedPc1, actualPc1.getConstraintExpr());

    // A single node attribute constraint
    result = PlacementConstraintParser
        .parsePlacementSpec(
            "xyz=1,rm.yarn.io/foo!=abc:zxy=1,rm.yarn.io/bar=true");
    Assert.assertEquals(2, result.size());
    target = PlacementTargets
        .nodeAttribute("rm.yarn.io/foo", "abc");
    expectedPc1 = targetNodeAttribute("node", NodeAttributeOpCode.NE, target);
    target = PlacementTargets
        .nodeAttribute("rm.yarn.io/bar", "true");
    expectedPc2 = targetNodeAttribute("node", NodeAttributeOpCode.EQ, target);

    Iterator<PlacementConstraint> valueIt = result.values().iterator();
    actualPc1 = valueIt.next();
    actualPc2 = valueIt.next();
    Assert.assertEquals(expectedPc1, actualPc1.getConstraintExpr());
    Assert.assertEquals(expectedPc2, actualPc2.getConstraintExpr());

    // A single node attribute constraint w/o source tags
    result = PlacementConstraintParser
        .parsePlacementSpec("rm.yarn.io/foo=true");
    Assert.assertEquals(1, result.size());
    target = PlacementTargets.nodeAttribute("rm.yarn.io/foo", "true");
    expectedPc1 = targetNodeAttribute("node", NodeAttributeOpCode.EQ, target);

    SourceTags actualSourceTags = result.keySet().iterator().next();
    Assert.assertTrue(actualSourceTags.isEmpty());
    actualPc1 = result.values().iterator().next();
    Assert.assertEquals(expectedPc1, actualPc1.getConstraintExpr());

    // If source tags is not specified for a node-attribute constraint,
    // then this expression must be single constraint expression.
    try {
      PlacementConstraintParser
          .parsePlacementSpec("rm.yarn.io/foo=true:xyz=1,notin,node,xyz");
      Assert.fail("Expected a failure!");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof PlacementConstraintParseException);
    }
  }
}
