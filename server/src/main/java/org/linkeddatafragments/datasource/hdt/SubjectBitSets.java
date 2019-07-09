package org.linkeddatafragments.datasource.hdt;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashSet;

public class SubjectBitSets implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = 7820793642825869446L;
  /**
   * 
   */
  private BitSet greaterThanBitset;
  private BitSet equalBitset;

  public SubjectBitSets(HashSet<Integer> subjectIdsWithGreaterValues,
      HashSet<Integer> subjectIdsWithEqualValues) {
    this.greaterThanBitset = new BitSet();
    this.equalBitset = new BitSet();

    for (Integer subjectId : subjectIdsWithEqualValues) {
      this.equalBitset.set(subjectId);
    }

    for (Integer subjectId : subjectIdsWithGreaterValues) {
      this.greaterThanBitset.set(subjectId);
    }
  }

  public SubjectBitSets() {}

  public BitSet getGreaterThanBitset() {
    return greaterThanBitset;
  }

  public void setGreaterThanBitset(BitSet greaterThanBitset) {
    this.greaterThanBitset = greaterThanBitset;
  }

  public BitSet getEqualBitset() {
    return equalBitset;
  }

  public void setEqualBitset(BitSet equalBitset) {
    this.equalBitset = equalBitset;
  }

}
