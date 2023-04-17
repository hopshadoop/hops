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

package io.hops.erasure_coding;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

public class SimpleRegeneratingCode extends ErasureCode {
  public static final Log LOG = LogFactory.getLog(SimpleRegeneratingCode.class);
  private int stripeSize;
  private int paritySize;
  private int paritySizeSRC;
  private int paritySizeRS;
  private int simpleParityDegree;
  private int[] generatingPolynomial;
  private int PRIMITIVE_ROOT = 2;
  private int[] primitivePower;
  private GaloisField GF = GaloisField.getInstance();
  private int[] errSignature;
  private int[] dataBuff;
  private int[][] groupsTable;

  @Deprecated
  public SimpleRegeneratingCode(int stripeSize, int paritySize) {
    init(stripeSize, paritySize);
  }

  public SimpleRegeneratingCode() {

  }

  @Override
  public void init(Codec codec) {
      this.paritySizeSRC = ((Long) codec.json.get("parity_length_src")).intValue();
    init(codec.stripeLength, codec.parityLength);
    LOG.info(" Initialized " + SimpleRegeneratingCode.class +
        " stripeLength:" + codec.stripeLength +
        " parityLength:" + codec.parityLength +
        " SRC parities:" + paritySizeSRC);
  }

  private void init(int stripeSize, int paritySize) {
    this.stripeSize = stripeSize;
    this.paritySize = paritySize;
    this.paritySizeRS = paritySize - paritySizeSRC;

    assert (stripeSize + paritySizeRS < GF.getFieldSize());
    assert (paritySize >= paritySizeSRC);

    // The degree of a simple parity is the number of locations
    // combined into the single parity. The degree is a function
    // of the RS-stripe (stripe + RS parity) length.
    // (The number of SRC groups is paritySizeSRC + 1, because
    // one SRC parity is implied -- not stored).
    simpleParityDegree = (int) Math.ceil(
        (double) (stripeSize + paritySizeRS) / (double) (paritySizeSRC + 1));
    
    while (simpleParityDegree * paritySizeSRC >= stripeSize + paritySizeRS) {
      LOG.info("\nInvalid code parameters." +
          " Reducing SRC parities to " + (paritySizeSRC - 1) +
          " Increasing RS parities to " + (paritySizeRS + 1));
      this.paritySizeSRC--;
      this.paritySizeRS++;
      simpleParityDegree = (int) Math.ceil(
          (double) (stripeSize + paritySizeRS) / (double) (paritySizeSRC + 1));
    }
    
    this.errSignature = new int[paritySizeRS];
    this.dataBuff = new int[paritySizeRS + stripeSize];

    this.primitivePower = new int[stripeSize + paritySizeRS];
    // compute powers of the primitive root
    for (int i = 0; i < stripeSize + paritySizeRS; i++) {
      primitivePower[i] = GF.power(PRIMITIVE_ROOT, i);
    }

    // compute generating polynomial
    int[] gen = {1};
    int[] poly = new int[2];
    for (int i = 0; i < paritySizeRS; i++) {
      poly[0] = primitivePower[i];
      poly[1] = 1;
      gen = GF.multiply(gen, poly);
    }

    // generating polynomial has all generating roots
    generatingPolynomial = gen;

    // groupsTable[][]
    // groupsTable[loc]: the SRC group neighbors of location loc.
    groupsTable = new int[paritySize + stripeSize][];
    for (int i = 0; i < groupsTable.length; i++) {
      List<Integer> locationsInGroup = getSRCGroupNeighbors(i);
      groupsTable[i] = new int[locationsInGroup.size()];
      int k = 0;
      for (int loc : locationsInGroup) {
        groupsTable[i][k++] = loc;
      }
    }
  }

  @Override
  public void encode(int[] message, int[] parity) {
    assert (message.length == stripeSize && parity.length == paritySize);
    // initialize data buffer
    for (int i = 0; i < paritySizeRS; i++) {
      dataBuff[i] = 0;
    }

    // put message in the data buffer
    for (int i = 0; i < stripeSize; i++) {
      dataBuff[i + paritySizeRS] = message[i];
    }

    // calculate RS parities and copy into parity[]
    GF.remainder(dataBuff, generatingPolynomial);
    for (int i = 0; i < paritySizeRS; i++) {
      parity[i + paritySizeSRC] = dataBuff[i];
    }

    // restore message in dataBuff
    for (int i = 0; i < stripeSize; i++) {
      dataBuff[i + paritySizeRS] = message[i];
    }

    // compute the SRC parities and store into parity[]
    for (int i = 0; i < paritySizeSRC; i++) {
      parity[i] = 0;
      for (int j = simpleParityDegree * i; j < simpleParityDegree * (i + 1);
           j++) {
        parity[i] = GF.add(dataBuff[j], parity[i]);
      }
    }
  }

  /*
   * Perform Reed Solomon decoding.
   */
  private void decodeReedSolomon(int[] data, int[] erasedLocations,
      int[] erasedValues) {

    if (erasedLocations.length == 0) {
      return;
    }

    assert (erasedLocations.length == erasedValues.length);
    assert (erasedLocations.length <= paritySizeRS);

    for (int i = 0; i < erasedLocations.length; i++) {
      data[erasedLocations[i]] = 0;
    }

    for (int i = 0; i < erasedLocations.length; i++) {
      errSignature[i] = primitivePower[erasedLocations[i]];
      erasedValues[i] = GF.substitute(data, primitivePower[i]);
    }

    GF.solveVandermondeSystem(errSignature, erasedValues,
        erasedLocations.length);
  }

  /*
   * Performs Reed Solomon decoding, assuming that all positions not included in
   * erasedLocations are available.
   */
  @Override
  public void decode(int[] data, int[] erasedLocations, int[] erasedValues) {
    decodeReedSolomon(data, erasedLocations, erasedValues);
  }

  @Override
  public void decode(int[] data, int[] erasedLocations, int[] erasedValues,
      int[] locationsToRead, int[] locationsNotToRead) {

    assert (erasedLocations.length == erasedValues.length);

    // CASE 1 : SINGLE ERASURE
    // If only one erasure is passed, perform a quick repair using the
    // local group (locationsToRead).

    if (erasedLocations.length == 1) {
      erasedValues[0] = 0;
      for (int i = 0; i < locationsToRead.length; i++) {
        erasedValues[0] = GF.add(data[locationsToRead[i]], erasedValues[0]);
      }
      return;
    }

    // CASE 2 : MULTIPLE ERASURES - NO CONFLICT

    if (!groupConflict(erasedLocations)) {
      for (int i = 0; i < erasedLocations.length; i++) {
        int[] singleErasedLocation = new int[1];
        singleErasedLocation[0] = erasedLocations[i];
        int[] singleErasedValue = new int[1];
        singleErasedValue[0] = 0;
        
        decode(data, singleErasedLocation, singleErasedValue,
            groupsTable[erasedLocations[i]], null);
        
        erasedValues[i] = singleErasedValue[0];
      }
      return;
    }

    // CASE 3 : MULTIPLE ERASURES - CONFLICT
    // According to locationsToReadForDecode(), locationsToRead should
    // be of length equal to stripeSize for RS decoding.

    assert (locationsToRead.length == stripeSize);
    assert (locationsNotToRead.length ==
        stripeSize + paritySize - locationsToRead.length);

    // count the number of src parities that are erased
    int numOferasedSRCparities = 0;
    for (int i = 0; i < erasedLocations.length; i++) {
      if (erasedLocations[i] < paritySizeSRC) {
        numOferasedSRCparities++;
      }
    }

    int[] dataRS = new int[paritySizeRS + stripeSize];
    for (int i = 0; i < paritySizeRS + stripeSize; i++) {
      dataRS[i] = data[i + paritySizeSRC];
    }

    /*
     * erasedLocationsRS contains actual erased locations of the RS stripe
     * plus some locations that are not supposed to be read.
     */
    int[] erasedLocationsRS =
        new int[locationsNotToRead.length - this.paritySizeSRC];
    int k = 0;
    for (int i = 0; i < locationsNotToRead.length; i++) {
      if (locationsNotToRead[i] >= paritySizeSRC) {
        erasedLocationsRS[k++] = locationsNotToRead[i] - paritySizeSRC;
      }
    }

    int[] erasedValuesRS = new int[erasedLocationsRS.length];

    decodeReedSolomon(dataRS, erasedLocationsRS, erasedValuesRS);

    for (int i = 0; i < erasedLocationsRS.length; i++) {
      data[paritySizeSRC + erasedLocationsRS[i]] = erasedValuesRS[i];
    }

    // now that the RS part is all fixed, fix the simple parities
    for (int i = 0; i < erasedLocations.length; i++) {
      if (erasedLocations[i] < paritySizeSRC) {
        int par = erasedLocations[i];
        data[par] = 0;
        for (int j = 0; j < groupsTable[erasedLocations[i]].length; j++) {
          data[par] =
              GF.add(data[groupsTable[erasedLocations[i]][j]], data[par]);
        }
      }
    }

    for (int i = 0; i < erasedLocations.length; i++) {
      erasedValues[i] = data[erasedLocations[i]];
    }

    return;

  }


  @Override
  public int stripeSize() {
    return this.stripeSize;
  }

  @Override
  public int paritySize() {
    return this.paritySize;
  }

  @Override
  public int symbolSize() {
    return (int) Math.round(Math.log(GF.getFieldSize()) / Math.log(2));
  }

  /**
   * Figure out which locations need to be read to decode erased locations. The
   * locations are specified as integers in the range [ 0, stripeSize() +
   * paritySize() ). Values in the range [ 0, paritySize() ) represent parity
   * data. Values in the range [ paritySize(), paritySize() + stripeSize() )
   * represent message data.
   *
   * @param erasedLocations
   *     The erased locations.
   * @return The locations to read.
   */
  @Override
  public List<Integer> locationsToReadForDecode(List<Integer> erasedLocations)
      throws TooManyErasedLocations {

    //LOG.info("Erased locations: "+erasedLocations.toString());

    List<Integer> locationsToRead;
    
    // If only one location is erased, return its local (src) group
    if (erasedLocations.size() == 1) {
      //locationsToRead = computeSRCGroupLocations(erasedLocations.get(0));
      int loc = erasedLocations.get(0);
      locationsToRead = new ArrayList<Integer>(groupsTable[loc].length);
      for (int i = 0; i < groupsTable[loc].length; i++) {
        locationsToRead.add(groupsTable[loc][i]);
      }
      //LOG.info("locations to read: "+locationsToRead.toString());
      return locationsToRead;
    }

    // If more than one location are erased, check if they belong to the same group.
    // If they do not belong to same group (- no conflict), add locations of each
    // separate group.
    int[] erasedLocationsArray = new int[erasedLocations.size()];
    for (int i = 0; i < erasedLocations.size(); i++) {
      erasedLocationsArray[i] = erasedLocations.get(i);
    }

    if (!groupConflict(erasedLocationsArray)) {
      // we expect approximately simpleParityDegree locations to be read for each
      // erased location.
      locationsToRead =
          new ArrayList<Integer>(erasedLocations.size() * simpleParityDegree);
      // add unique locations
      for (int loc : erasedLocations) {
        for (int i = 0; i < groupsTable[loc].length; i++) {
          if (!locationsToRead.contains(groupsTable[loc][i])) {
            locationsToRead.add(groupsTable[loc][i]);
          }
        }
      }
      return locationsToRead;
    }

    // If more than one location is erased and there is at least a pair belonging to the
    // same group, we will have to perform Reed Solomon (RS) decoding. Hence, read
    // locations that are necessary for RS decoding.
    locationsToRead = new ArrayList<Integer>(stripeSize());
    int limit = stripeSize() + paritySize();

    //Loop through all possible locations in the stripe, omitting the SRC parities.
    for (int loc = paritySizeSRC; loc < limit; loc++) {
      //Is the location good.
      if (erasedLocations.indexOf(loc) == -1) {
        locationsToRead.add(loc);
        if (stripeSize() == locationsToRead.size()) {
          break;
        }
      }
    }
    // If we are are not able to fill up the locationsToRead list,
    // we did not find enough good locations. Throw TooManyErasedLocations.
    if (locationsToRead.size() != stripeSize()) {
      String locationsStr = "";
      for (Integer erasedLocation : erasedLocations) {
        locationsStr += " " + erasedLocation;
      }
      throw new TooManyErasedLocations("Locations " + locationsStr);
    }
    return locationsToRead;
  }

  /*
   * Given a location loc, return a list with the other locations
   * belonging to the same SRC group as loc.
   */
  private List<Integer> getSRCGroupNeighbors(int loc) {
    int limit = stripeSize() + paritySize();

    /* A group is expected to have at most simpleParityDegree + 1
     * locations. groupLocations will contain the simpleParityDegree
     * neighbors of loc.
     */

    List<Integer> neighbors = new ArrayList<Integer>(simpleParityDegree);
    int group = getSRCGroup(loc);

    // CASE 1: Group with "stored" SRC parity.
    if (group < paritySizeSRC) {
      if (group != loc)
      // group equals the location of the SRC parity
      // Hence, add the location to the neighbors.
      {
        neighbors.add(group);
      }
      // add the rest neighbors (loc is excluded)
      for (int i = paritySizeSRC + group * simpleParityDegree;
           i < paritySizeSRC + (group + 1) * simpleParityDegree; i++) {
        if (i != loc) {
          neighbors.add(i);
        }
      }
    }// CASE 2: Group is the one with the "inferred" SRC parity.
    else {
      assert (loc >= paritySizeSRC);
      // All SRC parities are neighbors.
      for (int i = 0; i < paritySizeSRC; i++) {
        neighbors.add(i);
      }
      // Add the remaining (non SRC-parity) neighbors.
      for (int i = paritySizeSRC + group * simpleParityDegree; i < limit; i++) {
        if (i != loc) {
          neighbors.add(i);
        }
      }
    }
    return neighbors;
  }

  /*
   * Return the id of the SRC group location loc belongs to.
   * Return -1 for invalid loc.
   */
  private int getSRCGroup(int loc) {
    int group = -1;
    if (0 <= loc && loc < paritySizeSRC) {
      group = loc;
    } else if (paritySizeSRC <= loc && loc < stripeSize + paritySize) {
      group = (int) (loc - paritySizeSRC) / simpleParityDegree;
    } else {
      group = -1;
    }

    return group;
  }

  /*
   * Check for conflict (- whether any two locations in locs
   * belong to the same SRC group.
   */
  private boolean groupConflict(int[] locs) {
    int[] groups = new int[paritySizeSRC + 1];
    for (int i = 0; i < groups.length; i++) {
      groups[i] = 0;
    }
    /*
     * if at least one position in locs is SRC parity,
     * mark the last group.
     */
    for (int i = 0; i < locs.length; i++) {
      if (locs[i] < paritySizeSRC) {
        groups[paritySizeSRC] = 1;
        break;
      }
    }
    
    for (int i = 0; i < locs.length; i++) {
      if (groups[getSRCGroup(locs[i])]++ > 0) {
        return true;
      }
    }
    return false;
  }
}
