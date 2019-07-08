package org.linkeddatafragments.datasource.hdt;

import java.util.ArrayList;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.brtpf.IBrTPFRequest;
import org.linkeddatafragments.datasource.AbstractBrTPFWorker;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.tpf.ITriplePatternElement;
import org.linkeddatafragments.util.Utils;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdtjena.NodeDictionary;

/**
 * Worker class for HDT
 * 
 * @author Ilkcan Keles
 */
public class HdtBrTPFWorker extends AbstractBrTPFWorker<RDFNode, String, String> {


  /**
   * HDT Datasource
   */
  private HDT datasource;

  /**
   * The dictionary
   */
  private NodeDictionary dictionary;

  /**
   * Create Hdt BrTPF Worker
   * 
   * @param req
   */
  public HdtBrTPFWorker(final IBrTPFRequest<RDFNode, String, String> req) {
    super(req);
  }

  public void setDatasourceAndDictionary(HDT datasource, NodeDictionary dictionary) {
    this.datasource = datasource;
    this.dictionary = dictionary;
  }

  /**
   * Creates an {@link ILinkedDataFragment} for brTPF request from the HDT
   * 
   * @param subject
   * @param predicate
   * @param object
   * @param bindings
   * @param boundVariables
   * @param offset
   * @param limit
   * @return
   */

  @Override
  protected ILinkedDataFragment createBrTPFFragment(
      ITriplePatternElement<RDFNode, String, String> subject,
      ITriplePatternElement<RDFNode, String, String> predicate,
      ITriplePatternElement<RDFNode, String, String> object, ArrayList<Binding> bindings,
      ArrayList<Var> boundVariables, long offset, long limit) throws IllegalArgumentException {
    final TripleIDCachingIterator it = new TripleIDCachingIterator(dictionary, bindings,
        boundVariables, subject, predicate, object);

    final Model triples = ModelFactory.createDefaultModel();
    int triplesCheckedSoFar = 0;
    int triplesAddedInCurrentPage = 0;
    boolean atOffset;
    while (it.hasNext()) {
      final TripleID t = it.next();
      final IteratorTripleID matches = datasource.getTriples().search(t);
      final boolean hasMatches = matches.hasNext();
      if (hasMatches) {
        matches.goToStart();
        while (!(atOffset = (triplesCheckedSoFar == offset)) && matches.hasNext()) {
          matches.next();
          triplesCheckedSoFar++;
        }
        // try to add `limit` triples to the result model
        if (atOffset) {
          while (triplesAddedInCurrentPage < limit && matches.hasNext()) {
            triples.add(triples.asStatement(Utils.toTriple(dictionary, matches.next())));
            triplesAddedInCurrentPage++;
          }
        }
      }
    }

    final int bindingsSize = bindings.size();
    final long minimumTotal = offset + triplesAddedInCurrentPage + 1;
    final long estimatedTotal;
    if (triplesAddedInCurrentPage < limit) {
      estimatedTotal = offset + triplesAddedInCurrentPage;
    }
    // else // This else block is for testing purposes only. The next else block is the correct
    // one.
    // {
    // estimatedTotal = minimumTotal;
    // }
    else {
      final int THRESHOLD = 10;
      final int maxBindingsToUseInEstimation;
      if (bindingsSize <= THRESHOLD) {
        maxBindingsToUseInEstimation = bindingsSize;
      } else {
        maxBindingsToUseInEstimation = THRESHOLD;
      }

      long estimationSum = 0L;
      it.reset();
      int i = 0;
      while (it.hasNext() && i < maxBindingsToUseInEstimation) {
        i++;
        estimationSum += estimateResultSetSize(it.next());
      }

      if (bindingsSize <= THRESHOLD) {
        if (estimationSum <= minimumTotal)
          estimatedTotal = minimumTotal;
        else
          estimatedTotal = estimationSum;
      } else // bindingsSize > THRESHOLD
      {
        final double fraction = bindingsSize / maxBindingsToUseInEstimation;
        final double estimationAsDouble = fraction * estimationSum;
        final long estimation = Math.round(estimationAsDouble);
        if (estimation <= minimumTotal)
          estimatedTotal = minimumTotal;
        else
          estimatedTotal = estimation;
      }
    }
    // create the fragment
    final boolean isLastPage = (estimatedTotal < offset + limit);
    return createTriplePatternFragment(triples, estimatedTotal, isLastPage);
  }

  protected long estimateResultSetSize(final TripleID t) {
    final IteratorTripleID matches = datasource.getTriples().search(t);

    if (matches.hasNext())
      return Math.max(matches.estimatedNumResults(), 1L);
    else
      return 0L;
  }

}
