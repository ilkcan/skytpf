package org.linkeddatafragments.skytpf;

import java.util.Comparator;
import org.apache.jena.datatypes.BaseDatatype.TypedValue;
import org.apache.jena.rdf.model.Statement;
import org.linkeddatafragments.skytpf.ISkyTPFRequest.SkylinePrefFunc;

public class StatementComparator implements Comparator<Statement> {

  private boolean maxPreferred;

  public StatementComparator(SkylinePrefFunc skylinePreferenceFunction) {
    this.maxPreferred = skylinePreferenceFunction.equals(SkylinePrefFunc.MAX);
  }

  @Override
  public int compare(Statement s1, Statement s2) {
    try {
      float s1Val = s1.getObject().asLiteral().getFloat();
      float s2Val = s2.getObject().asLiteral().getFloat();
      if (this.maxPreferred) {
        return Float.compare(s2Val, s1Val);
      } else {
        return Float.compare(s1Val, s2Val);
      }
    } catch (Exception e) {
      Object firstVal = s1.getObject().asLiteral();
      Object secondVal = s2.getObject().asLiteral();
      String s1LexicalValue, s2LexicalValue;
      if (firstVal instanceof TypedValue && secondVal instanceof TypedValue) {
        s1LexicalValue = ((TypedValue) firstVal).lexicalValue;
        s2LexicalValue = ((TypedValue) secondVal).lexicalValue;
      } else {
        s1LexicalValue = (String) firstVal;
        s2LexicalValue = (String) secondVal;
      }

      try {
        float s1Val = Float.parseFloat(s1LexicalValue);
        float s2Val = Float.parseFloat(s2LexicalValue);
        if (this.maxPreferred) {
          return Float.compare(s2Val, s1Val);
        } else {
          return Float.compare(s1Val, s2Val);
        }
      } catch (Exception e2) {
        if (this.maxPreferred) {
          return s2LexicalValue.compareTo(s1LexicalValue);
        } else {
          return s1LexicalValue.compareTo(s2LexicalValue);
        }
      }
    }
  }

}
