package dk.aau.cs.skytpf.model;

import java.util.Comparator;

public class TriplePatternComparator implements Comparator<TriplePattern> {

  @Override
  public int compare(TriplePattern o1, TriplePattern o2) {
    return Integer.compare(o1.getTriplesCount(), o2.getTriplesCount());
  }

}
