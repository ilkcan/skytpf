package org.linkeddatafragments.datasource.hdt;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.jena.datatypes.BaseDatatype.TypedValue;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.impl.LiteralImpl;
import org.linkeddatafragments.brtpf.IBrTPFRequest;
import org.linkeddatafragments.datasource.AbstractRequestProcessor;
import org.linkeddatafragments.datasource.AbstractSkyTPFWorker;
import org.linkeddatafragments.datasource.IFragmentRequestProcessor;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragmentRequest;
import org.linkeddatafragments.skytpf.ISkyTPFRequest;
import org.linkeddatafragments.skytpf.ISkyTPFRequest.SkylinePrefFunc;
import org.linkeddatafragments.util.Utils;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdtjena.NodeDictionary;



/**
 * Implementation of {@link IFragmentRequestProcessor} that processes
 * {@link ITriplePatternFragmentRequest}s over data stored in HDT.
 *
 * @author Ruben Verborgh
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class HdtRequestProcessor extends AbstractRequestProcessor<RDFNode, String, String> {

  /**
   * HDT Datasource
   */
  private final HDT datasource;
  private HashMap<Long, HashMap<Long, Integer>> predicateSubjectRanks;
  private HashMap<Long, Long[]> predicateSubjectsSorted;
  private HashMap<Long, HashSet<Long>> predicateSubjects;
  /**
   * The dictionary
   */
  private final NodeDictionary dictionary;

  /**
   * Creates the request processor.
   *
   * @param hdtFile the HDT datafile
   * @throws IOException if the file cannot be loaded
   * @throws ClassNotFoundException
   */
  public HdtRequestProcessor(String hdtFile) throws IOException, ClassNotFoundException {
    datasource = HDTManager.mapIndexedHDT(hdtFile, null); // listener=null
    dictionary = new NodeDictionary(datasource.getDictionary());
    predicateSubjectRanks = new HashMap<Long, HashMap<Long, Integer>>();
    predicateSubjectsSorted = new HashMap<Long, Long[]>();
    initializeNumericPredicateIndexes(hdtFile);

    predicateSubjects = new HashMap<Long, HashSet<Long>>();
    for (long predicateId : predicateSubjectRanks.keySet()) {
      predicateSubjects.put(predicateId,
          new HashSet<Long>(predicateSubjectRanks.get(predicateId).keySet()));
    }
  }

  @SuppressWarnings("unchecked")
  private void initializeNumericPredicateIndexes(String hdtFile)
      throws IOException, ClassNotFoundException {
    File psrObjectFile = new File(hdtFile + ".psr");
    File pssObjectFile = new File(hdtFile + ".pss");

    if (psrObjectFile.isFile()) {
      System.out.println("Psr file exists. Trying to initialize...");
      FSTObjectInput in = new FSTObjectInput(new FileInputStream(psrObjectFile));
      predicateSubjectRanks = (HashMap<Long, HashMap<Long, Integer>>) in.readObject();
      in.close(); // required !
      in = new FSTObjectInput(new FileInputStream(pssObjectFile));
      predicateSubjectsSorted = (HashMap<Long, Long[]>) in.readObject();
      in.close(); // required !

      System.out.println(predicateSubjectRanks.size());
      System.out.println("Initialized numeric attribute index.");
    } else {
      HashMap<Long, HashMap<Long, Float>> tripleIDValues =
          new HashMap<Long, HashMap<Long, Float>>();
      Model emptyModel = ModelFactory.createDefaultModel();
      HashSet<Long> predicatesWithFloatValues = new HashSet<Long>();
      HashSet<Long> predicatesWithStringValues = new HashSet<Long>();
      Iterator<TripleID> tripleIterator = datasource.getTriples().searchAll();
      while (tripleIterator.hasNext()) {
        TripleID tripleId = tripleIterator.next();
        long predicateId = tripleId.getPredicate();
        if (predicatesWithStringValues.contains(predicateId)) {
          continue;
        }
        long subjectId = tripleId.getSubject();

        Triple triple = Utils.toTriple(dictionary, tripleId);
        Statement st = emptyModel.asStatement(triple);
        if (st.getObject().isLiteral()) {
          float val = 0;
          try {
            val = st.getObject().asLiteral().getFloat();
          } catch (Exception e) {
            Object firstVal = st.getObject().asLiteral();
            String lexVal;
            if (firstVal instanceof TypedValue) {
              lexVal = ((TypedValue) firstVal).lexicalValue;
            } else {
              lexVal = ((LiteralImpl) firstVal).getString();
            }

            try {
              val = Float.parseFloat(lexVal);
            } catch (Exception e2) {
              predicatesWithStringValues.add(predicateId);
              if (tripleIDValues.containsKey(predicateId)) {
                tripleIDValues.remove(predicateId);
              }
              continue;
            }
          }

          if (tripleIDValues.containsKey(predicateId)) {
            tripleIDValues.get(predicateId).put(subjectId, val);
          } else {
            HashMap<Long, Float> innerHashMap = new HashMap<Long, Float>();
            innerHashMap.put(subjectId, val);
            tripleIDValues.put(tripleId.getPredicate(), innerHashMap);
          }
          predicatesWithFloatValues.add(tripleId.getPredicate());
        }
      }
      predicatesWithFloatValues.removeAll(predicatesWithStringValues);
      System.out
          .println("Number of predicates with literal values: " + predicatesWithFloatValues.size());
      ArrayList<SubjectIdWithValue> tripleValues = new ArrayList<SubjectIdWithValue>();
      for (long predicateId : predicatesWithFloatValues) {
        System.out.println("creating bitmaps for predicate " + predicateId);
        System.out.println(dictionary.getNode(predicateId, TripleComponentRole.PREDICATE).getURI());
        Set<Long> subjectIds = tripleIDValues.get(predicateId).keySet();
        for (long subjectId : subjectIds) {
          tripleValues.add(
              new SubjectIdWithValue(subjectId, tripleIDValues.get(predicateId).get(subjectId)));
        }
        Collections.sort(tripleValues, new SubjectIdWithValueComparator(SkylinePrefFunc.MAX));
        Long[] sortingResult = new Long[tripleValues.size()];
        for (int i = 0; i < sortingResult.length; i++) {
          sortingResult[i] = tripleValues.get(i).getSubjectId();
        }
        predicateSubjectsSorted.put(predicateId, sortingResult);
        HashMap<Long, Integer> subjectRanks = new HashMap<Long, Integer>();
        int numOfTriples = tripleValues.size();
        int lastIdx = 0;
        float lastVal = tripleValues.get(0).getValue();
        for (int i = 0; i < numOfTriples; i++) {
          long subjectId = tripleValues.get(i).getSubjectId();
          float currVal = tripleValues.get(i).getValue();
          if (lastVal == currVal) {
            subjectRanks.put(subjectId, lastIdx);
          } else {
            lastVal = currVal;
            lastIdx++;
            subjectRanks.put(subjectId, lastIdx);
          }
        }
        tripleIDValues.remove(predicateId);
        tripleValues.clear();
        predicateSubjectRanks.put(predicateId, subjectRanks);
      }

      FSTObjectOutput out = new FSTObjectOutput(new FileOutputStream(psrObjectFile));
      out.writeObject(predicateSubjectRanks);
      out.close();

      out = new FSTObjectOutput(new FileOutputStream(pssObjectFile));
      out.writeObject(predicateSubjectsSorted);
      out.close();
      System.out.println("Finished indexing numeric attributes.");
    }
  }

  /**
   *
   * @param request
   * @return
   * @throws IllegalArgumentException
   */
  @Override
  protected HdtBrTPFWorker getBrTPFSpecificWorker(
      final IBrTPFRequest<RDFNode, String, String> request) throws IllegalArgumentException {
    HdtBrTPFWorker worker = new HdtBrTPFWorker(request);
    worker.setDatasourceAndDictionary(datasource, dictionary);
    return worker;
  }

  /**
   *
   * @param request
   * @return
   * @throws IllegalArgumentException
   */
  @Override
  protected HdtTPFWorker getTPFSpecificWorker(
      final ITriplePatternFragmentRequest<RDFNode, String, String> request)
      throws IllegalArgumentException {
    HdtTPFWorker worker = new HdtTPFWorker(request);
    worker.setDatasourceAndDictionary(datasource, dictionary);
    return worker;
  }

  @Override
  protected AbstractSkyTPFWorker<RDFNode, String, String> getSkyTPFSpecificWorker(
      ISkyTPFRequest<RDFNode, String, String> request) throws IllegalArgumentException {
    HdtSkyTPFWorkerWithoutSorting worker = new HdtSkyTPFWorkerWithoutSorting(request);
    worker.setDatasourceAndDictionary(datasource, dictionary, predicateSubjectRanks,
        predicateSubjectsSorted, predicateSubjects);
    return worker;
  }
}
