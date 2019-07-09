package org.linkeddatafragments.skytpf;

import java.util.Comparator;
import org.apache.jena.datatypes.BaseDatatype.TypedValue;
import org.apache.jena.rdf.model.Literal;

public class LiteralComparator implements Comparator<Literal> {

  public LiteralComparator() {}

  @Override
  public int compare(Literal l1, Literal l2) {
    try {
      float s1Val = l1.getFloat();
      float s2Val = l2.getFloat();
      return Float.compare(s1Val, s2Val);
    } catch (Exception e) {
      String s1LexicalValue, s2LexicalValue;
      if (l1 instanceof TypedValue && l2 instanceof TypedValue) {
        s1LexicalValue = ((TypedValue) l1).lexicalValue;
        s2LexicalValue = ((TypedValue) l2).lexicalValue;
      } else {
        s1LexicalValue = (String) l1.getString();
        s2LexicalValue = (String) l2.getString();
      }

      try {
        float s1Val = Float.parseFloat(s1LexicalValue);
        float s2Val = Float.parseFloat(s2LexicalValue);
        return Float.compare(s1Val, s2Val);
      } catch (Exception e2) {
        return s1LexicalValue.compareTo(s2LexicalValue);
      }
    }
  }

}
