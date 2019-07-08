/**
 * 
 */
package org.linkeddatafragments.datasource.hdt;

/**
 * @author Ilkcan Keles
 *
 */
public class SubjectIdWithValue {
  private int subjectId;
  private float value;

  /**
   * @param statement
   * @param tripleId
   */
  public SubjectIdWithValue(int subjectId, float value) {
    this.subjectId = subjectId;
    this.value = value;
  }

  public int getSubjectId() {
    return subjectId;
  }

  /**
   * @return the value
   */
  public float getValue() {
    return value;
  }
}
