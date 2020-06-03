package org.linkeddatafragments.datasource.hdt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.fragments.tpf.ITriplePatternElement;
import org.linkeddatafragments.fragments.tpf.TriplePatternElementFactory.ConstantRDFTerm;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdtjena.NodeDictionary;

public class TripleIDProducingIterator implements Iterator<TripleID> {
  /**
   * The dictionary
   */
  private NodeDictionary dictionary;
  private final boolean sIsVar, pIsVar, oIsVar;
  private final boolean canHaveMatches;

  private final List<Node> sBindings = new ArrayList<Node>();
  private final List<List<Node>> pBindings = new ArrayList<List<Node>>();
  private final List<List<List<Node>>> oBindings = new ArrayList<List<List<Node>>>();

  private long curSubjID, curPredID, curObjID;
  private int curSubjIdx, curPredIdx, curObjIdx;
  private List<Node> curPredBindings;
  private List<Node> curObjBindings;
  private boolean ready;

  public TripleIDProducingIterator(NodeDictionary dictionary,
      final List<Binding> jenaSolutionMappings, final List<Var> boundVariables,
      final ITriplePatternElement<RDFNode, String, String> subjectOfTP,
      final ITriplePatternElement<RDFNode, String, String> predicateOfTP,
      final ITriplePatternElement<RDFNode, String, String> objectOfTP) {
    this.dictionary = dictionary;
    int numOfVarsCoveredBySolMaps = 0;

    if (subjectOfTP.isNamedVariable()) {
      this.sIsVar = true;
      if (boundVariables == null) {
        System.out.println(subjectOfTP.asNamedVariable());
      }
      if (boundVariables.contains(Var.alloc(subjectOfTP.asNamedVariable())))
        numOfVarsCoveredBySolMaps++;
    } else
      this.sIsVar = false;

    if (predicateOfTP.isNamedVariable()) {
      this.pIsVar = true;
      if (boundVariables.contains(Var.alloc(predicateOfTP.asNamedVariable())))
        numOfVarsCoveredBySolMaps++;
    } else
      this.pIsVar = false;

    if (objectOfTP.isNamedVariable()) {
      this.oIsVar = true;
      if (boundVariables.contains(Var.alloc(objectOfTP.asNamedVariable())))
        numOfVarsCoveredBySolMaps++;
    } else
      this.oIsVar = false;

    final boolean needToCheckForDuplicates = (numOfVarsCoveredBySolMaps < boundVariables.size());

    curSubjID = (subjectOfTP instanceof ConstantRDFTerm)
        ? dictionary.getIntID(subjectOfTP.asConstantTerm().asNode(), TripleComponentRole.SUBJECT)
        : 0;

    curPredID =
        (predicateOfTP instanceof ConstantRDFTerm)
            ? dictionary.getIntID(predicateOfTP.asConstantTerm().asNode(),
                TripleComponentRole.PREDICATE)
            : 0;

    curObjID = (objectOfTP instanceof ConstantRDFTerm)
        ? dictionary.getIntID(objectOfTP.asConstantTerm().asNode(), TripleComponentRole.OBJECT)
        : 0;

    canHaveMatches = (curSubjID >= 0) && (curPredID >= 0) && (curObjID >= 0);

    if (canHaveMatches) {
      for (Binding solutionMap : jenaSolutionMappings) {

        final Node s = sIsVar ? solutionMap.get(Var.alloc(subjectOfTP.asNamedVariable())) : null;

        final Node p = pIsVar ? solutionMap.get(Var.alloc(predicateOfTP.asNamedVariable())) : null;

        final Node o = oIsVar ? solutionMap.get(Var.alloc(objectOfTP.asNamedVariable())) : null;

        final List<Node> pBindingsForS;
        final List<List<Node>> oBindingsForS;
        int sIdx;
        if (!needToCheckForDuplicates || (sIdx = sBindings.indexOf(s)) == -1) {
          sBindings.add(s);

          pBindingsForS = new ArrayList<Node>();
          pBindings.add(pBindingsForS);

          oBindingsForS = new ArrayList<List<Node>>();
          oBindings.add(oBindingsForS);
        } else {
          pBindingsForS = pBindings.get(sIdx);
          oBindingsForS = oBindings.get(sIdx);
        }

        final List<Node> oBindingsForSP;
        int pIdx;
        if (!needToCheckForDuplicates || (pIdx = pBindingsForS.indexOf(p)) == -1) {
          pBindingsForS.add(p);

          oBindingsForSP = new ArrayList<Node>();
          oBindingsForS.add(oBindingsForSP);
        } else {
          oBindingsForSP = oBindingsForS.get(pIdx);
        }

        if (!needToCheckForDuplicates || oBindingsForSP.indexOf(o) == -1) {
          oBindingsForSP.add(o);
        }
      }
    }

    reset();
  }

  @Override
  public boolean hasNext() {
    if (ready)
      return true;

    if (!canHaveMatches)
      return false;

    do {
      curObjIdx++;

      while (curPredID == -1 || curObjIdx >= curObjBindings.size()) {
        curPredIdx++;
        while (curSubjID == -1 || curPredIdx >= curPredBindings.size()) {
          curSubjIdx++;
          if (curSubjIdx >= sBindings.size()) {
            return false;
          }

          curPredBindings = pBindings.get(curSubjIdx);
          curPredIdx = 0;

          if (sIsVar)
            curSubjID = dictionary.getIntID(sBindings.get(curSubjIdx), TripleComponentRole.SUBJECT);
        }

        curObjBindings = oBindings.get(curSubjIdx).get(curPredIdx);
        curObjIdx = 0;

        if (pIsVar)
          curPredID =
              dictionary.getIntID(curPredBindings.get(curPredIdx), TripleComponentRole.PREDICATE);
      }

      if (oIsVar)
        curObjID = dictionary.getIntID(curObjBindings.get(curObjIdx), TripleComponentRole.OBJECT);
    } while (curSubjID == -1 || curPredID == -1 || curObjID == -1);

    ready = true;
    return true;
  }

  @Override
  public TripleID next() {
    if (!hasNext())
      throw new NoSuchElementException();

    ready = false;
    return new TripleID(curSubjID, curPredID, curObjID);
  }

  public void reset() {
    ready = canHaveMatches;

    if (canHaveMatches) {
      curSubjIdx = curPredIdx = curObjIdx = 0;

      curPredBindings = pBindings.get(curSubjIdx);
      curObjBindings = oBindings.get(curSubjIdx).get(curPredIdx);

      if (sIsVar)
        curSubjID = dictionary.getIntID(sBindings.get(curSubjIdx), TripleComponentRole.SUBJECT);

      if (pIsVar)
        curPredID =
            dictionary.getIntID(curPredBindings.get(curPredIdx), TripleComponentRole.PREDICATE);

      if (oIsVar)
        curObjID = dictionary.getIntID(curObjBindings.get(curObjIdx), TripleComponentRole.OBJECT);
    }
  }

}
