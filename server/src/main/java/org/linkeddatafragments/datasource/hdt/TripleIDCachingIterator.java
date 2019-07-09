package org.linkeddatafragments.datasource.hdt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.fragments.tpf.ITriplePatternElement;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdtjena.NodeDictionary;

public class TripleIDCachingIterator implements Iterator<TripleID> {
  private final TripleIDProducingIterator it;
  private final List<TripleID> cache = new ArrayList<TripleID>();

  private boolean replayMode = false;
  private int replayIdx = 0;

  public TripleIDCachingIterator(NodeDictionary dictionary,
      final List<Binding> jenaSolutionMappings, final List<Var> foundVariables,
      final ITriplePatternElement<RDFNode, String, String> subjectOfTP,
      final ITriplePatternElement<RDFNode, String, String> predicateOfTP,
      final ITriplePatternElement<RDFNode, String, String> objectOfTP) {
    it = new TripleIDProducingIterator(dictionary, jenaSolutionMappings, foundVariables,
        subjectOfTP, predicateOfTP, objectOfTP);
  }

  @Override
  public boolean hasNext() {
    if (replayMode && replayIdx < cache.size())
      return true;

    replayMode = false;
    return it.hasNext();
  }

  @Override
  public TripleID next() {
    if (!hasNext())
      throw new NoSuchElementException();

    if (replayMode) {
      return cache.get(replayIdx++);
    }

    final TripleID t = it.next();
    cache.add(t);
    return t;
  }

  public void reset() {
    replayMode = true;
    replayIdx = 0;
  }

} // end of TripleIDCachingIterator
