/**
 * 
 */
package org.linkeddatafragments.datasource.hdt;

/**
 * @author Ilkcan Keles
 *
 */
public class SubjectIdWithValue {
  private long subjectId;
  private float value;

  /**
   * @param statement
   * @param tripleId
   */
  public SubjectIdWithValue(long subjectId, float value) {
    this.subjectId = subjectId;
    this.value = value;
  }

  public long getSubjectId() {
    return subjectId;
  }

  /**
   * @return the value
   */
  public float getValue() {
    return value;
  }
}
