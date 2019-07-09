package org.linkeddatafragments.datasource.hdt;

import java.util.Comparator;
import org.linkeddatafragments.skytpf.ISkyTPFRequest.SkylinePrefFunc;

public class SubjectIdWithValueComparator implements Comparator<SubjectIdWithValue> {

  private boolean maxPreferred;

  public SubjectIdWithValueComparator(SkylinePrefFunc skylinePreferenceFunction) {
    this.maxPreferred = skylinePreferenceFunction.equals(SkylinePrefFunc.MAX);
  }

  @Override
  public int compare(SubjectIdWithValue t1, SubjectIdWithValue t2) {
    if (this.maxPreferred) {
      return Float.compare(t2.getValue(), t1.getValue());
    } else {
      return Float.compare(t1.getValue(), t2.getValue());
    }
  }

}
