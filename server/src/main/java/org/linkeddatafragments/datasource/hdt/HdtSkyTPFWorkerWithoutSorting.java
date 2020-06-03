package org.linkeddatafragments.datasource.hdt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.brtpf.IBrTPFRequest;
import org.linkeddatafragments.datasource.AbstractSkyTPFWorker;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.tpf.ITriplePatternElement;
import org.linkeddatafragments.fragments.tpf.TriplePatternElementFactory.ConstantRDFTerm;
import org.linkeddatafragments.skytpf.ISkyTPFRequest.SkylinePrefFunc;
import org.linkeddatafragments.util.Utils;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdtjena.NodeDictionary;

/**
 * Worker class for HDT
 * 
 * @author Ilkcan Keles
 */
public class HdtSkyTPFWorkerWithoutSorting extends AbstractSkyTPFWorker<RDFNode, String, String> {


  /**
   * HDT Datasource
   */
  private HDT datasource;

  /**
   * The dictionary
   */
  private NodeDictionary dictionary;

  private HashMap<Long, HashMap<Long, Integer>> predicateSubjectRanks;
  private HashMap<Long, Long[]> predicateSubjectsSorted;
  private HashMap<Long, HashSet<Long>> predicateSubjects;

  /**
   * Create Hdt BrTPF Worker
   * 
   * @param req
   */
  public HdtSkyTPFWorkerWithoutSorting(final IBrTPFRequest<RDFNode, String, String> req) {
    super(req);
  }

  public void setDatasourceAndDictionary(HDT datasource, NodeDictionary dictionary,
      HashMap<Long, HashMap<Long, Integer>> predicateSubjectRanks,
      HashMap<Long, Long[]> predicateSubjectsSorted,
      HashMap<Long, HashSet<Long>> predicateSubjects) {
    this.datasource = datasource;
    this.dictionary = dictionary;
    this.predicateSubjectRanks = predicateSubjectRanks;
    this.predicateSubjectsSorted = predicateSubjectsSorted;
    this.predicateSubjects = predicateSubjects;
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
  protected ILinkedDataFragment createSkyTpfFragment(
      ITriplePatternElement<RDFNode, String, String> subject,
      ITriplePatternElement<RDFNode, String, String> predicate,
      ITriplePatternElement<RDFNode, String, String> object, ArrayList<Binding> bindings,
      ArrayList<Var> boundVariables, Binding pivotBinding, ArrayList<Var> pivotBoundedVariables,
      SkylinePrefFunc skylinePreferenceFunction, long offset, long limit)
      throws IllegalArgumentException {

    ArrayList<Binding> pivotBindings = new ArrayList<Binding>();
    pivotBindings.add(pivotBinding);
    final TripleIDCachingIterator pivotIterator = new TripleIDCachingIterator(dictionary,
        pivotBindings, pivotBoundedVariables, subject, predicate, object);

    if (!(predicate instanceof ConstantRDFTerm) || !pivotIterator.hasNext()) {
      return createEmptyTriplePatternFragment();
    }

    final TripleID pivotTID = pivotIterator.next();
    long predicateId = predicate.isVariable() ? 0
        : dictionary.getIntID(predicate.asConstantTerm().asNode(), TripleComponentRole.PREDICATE);

    if (predicateId < 0) {
      return createEmptyTriplePatternFragment();
    }
    HashSet<Long> currPredicateSubjects = predicateSubjects.get(predicateId);
    ArrayList<Long> sBindings = new ArrayList<Long>();
    for (Binding solutionMap : bindings) {
      final Node s = solutionMap.get(Var.alloc(subject.asNamedVariable()));
      long currSubjID = dictionary.getIntID(s, TripleComponentRole.SUBJECT);
      if (currPredicateSubjects.contains(currSubjID)) {
        sBindings.add(currSubjID);
      }
    }
    final Model triples = ModelFactory.createDefaultModel();
    int triplesCheckedSoFar = 0;
    int triplesAddedInCurrentPage = 0;
    HashMap<Long, Integer> sortRanks = predicateSubjectRanks.get(predicateId);
    int pivotRank = sortRanks.get(pivotTID.getSubject());
    boolean atOffset;
    for (long currSubjID : sBindings) {
      final TripleID t = new TripleID(currSubjID, predicateId, 0);
      final IteratorTripleID matches = datasource.getTriples().search(t);
      if (matches.hasNext()) {
        matches.goToStart();
        while (!(atOffset = (triplesCheckedSoFar == offset)) && matches.hasNext()) {
          TripleID currTripleId = matches.next();
          long currSubjectId = currTripleId.getSubject();
          int currSubjectRank = sortRanks.get(currSubjectId);
          if (currSubjectRank == pivotRank) {
            triplesCheckedSoFar++;
          } else {
            if (currSubjectRank < pivotRank && skylinePreferenceFunction == SkylinePrefFunc.MAX) {
              triplesCheckedSoFar++;
            } else if (currSubjectRank > pivotRank
                && skylinePreferenceFunction == SkylinePrefFunc.MIN) {
              triplesCheckedSoFar++;
            }
          }
        }
        // try to add `limit` triples to the result model
        if (atOffset) {
          while (triplesAddedInCurrentPage < limit && matches.hasNext()) {
            TripleID currTripleId = matches.next();
            long currSubjectId = currTripleId.getSubject();
            int currSubjectRank = sortRanks.get(currSubjectId);
            if (currSubjectRank == pivotRank) {
              triples.add(triples.asStatement(Utils.toTriple(dictionary, currTripleId)));
              triplesAddedInCurrentPage++;
            } else {
              if ((currSubjectRank < pivotRank && skylinePreferenceFunction == SkylinePrefFunc.MAX)
                  || (currSubjectRank > pivotRank
                      && skylinePreferenceFunction == SkylinePrefFunc.MIN)) {
                triples.add(triples.asStatement(Utils.toTriple(dictionary, currTripleId)));
                triplesAddedInCurrentPage++;
              }
            }
          }
        }
      }
    }

    // estimates can be wrong; ensure 0 is returned if there are no results,
    // and always more than actual results
    final long estimatedTotal = sortRanks.size();
    // create the fragment
    // create the fragment
    final boolean isLastPage = (triplesAddedInCurrentPage < limit);
    return

    createTriplePatternFragment(triples, estimatedTotal, isLastPage);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.linkeddatafragments.datasource.AbstractSkyTPFWorker#createSkyTpfFragment(org.
   * linkeddatafragments.fragments.tpf.ITriplePatternElement,
   * org.linkeddatafragments.fragments.tpf.ITriplePatternElement,
   * org.linkeddatafragments.fragments.tpf.ITriplePatternElement,
   * org.linkeddatafragments.skytpf.ISkyTPFRequest.SkylinePrefFunc, long, long)
   */
  @Override
  protected ILinkedDataFragment createSkyTpfFragment(
      ITriplePatternElement<RDFNode, String, String> subject,
      ITriplePatternElement<RDFNode, String, String> predicate,
      ITriplePatternElement<RDFNode, String, String> object,
      SkylinePrefFunc skylinePreferenceFunction, long offset, long limit) {
    long predicateId = predicate.isVariable() ? 0
        : dictionary.getIntID(predicate.asConstantTerm().asNode(), TripleComponentRole.PREDICATE);
    int offsetIdx = (int) offset;
    int limitInt = (int) limit;
    if (predicateId < 0) {
      return createEmptyTriplePatternFragment();
    }
    Long[] allSubjectIdsSorted = predicateSubjectsSorted.get(predicateId);
    Long[] subjectIds;

    if (skylinePreferenceFunction == SkylinePrefFunc.MAX) {
      subjectIds = Arrays.copyOfRange(allSubjectIdsSorted, offsetIdx, offsetIdx + limitInt);
    } else {
      subjectIds = new Long[limitInt];
      int endIdx = allSubjectIdsSorted.length - offsetIdx - 1;
      int startIdx = endIdx - limitInt;
      if (startIdx < 0) {
        startIdx = 0;
      }
      if (endIdx >= 0) {
        subjectIds = Arrays.copyOfRange(allSubjectIdsSorted, startIdx, endIdx);
      }
    }

    final Model triples = ModelFactory.createDefaultModel();
    if (subjectIds != null) {
      for (long subjectId : subjectIds) {
        IteratorTripleID matches =
            datasource.getTriples().search(new TripleID(subjectId, predicateId, 0));
        if (matches.hasNext()) {
          triples.add(triples.asStatement(Utils.toTriple(dictionary, matches.next())));
        }
      }
    }
    // estimates can be wrong; ensure 0 is returned if there are no results,
    // and always more than actual results
    final long estimatedTotal = allSubjectIdsSorted.length;

    // create the fragment
    final boolean isLastPage = (estimatedTotal < offset + limit);
    return createTriplePatternFragment(triples, estimatedTotal, isLastPage);
  }

}
