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
  private HashMap<Integer, HashMap<Integer, Integer>> predicateSubjectRanks;
  private HashMap<Integer, Integer[]> predicateSubjectsSorted;
  private HashMap<Integer, HashSet<Integer>> predicateSubjects;
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
    predicateSubjectRanks = new HashMap<Integer, HashMap<Integer, Integer>>();
    predicateSubjectsSorted = new HashMap<Integer, Integer[]>();
    initializeNumericPredicateIndexes(hdtFile);

    predicateSubjects = new HashMap<Integer, HashSet<Integer>>();
    for (int predicateId : predicateSubjectRanks.keySet()) {
      predicateSubjects.put(predicateId,
          new HashSet<Integer>(predicateSubjectRanks.get(predicateId).keySet()));
    }
  }

  @SuppressWarnings("unchecked")
  private void initializeNumericPredicateIndexes(String hdtFile)
      throws IOException, ClassNotFoundException {
    File psrObjectFile = new File(hdtFile + ".psr");
    File pssObjectFile = new File(hdtFile + ".pss");
    /*
     * Kryo kryo = new Kryo();
     * 
     * MapSerializer serializer = new MapSerializer(); kryo.register(HashMap.class, serializer);
     * serializer.setKeyClass(Integer.class, kryo.getSerializer(Integer.class));
     * serializer.setKeysCanBeNull(false); MapSerializer innerMapSerializer = new MapSerializer();
     * serializer.setValueClass(HashMap.class, innerMapSerializer);
     * innerMapSerializer.setKeyClass(Integer.class, kryo.getSerializer(Integer.class));
     * kryo.register(BitSet.class, kryo.getSerializer(BitSet.class));
     * 
     * innerMapSerializer.setValueClass(SubjectBitSets.class, new Serializer<SubjectBitSets>() {
     * public void write(Kryo kryo, Output output, SubjectBitSets object) { kryo.writeObject(output,
     * object.getGreaterThanBitset()); kryo.writeObject(output, object.getEqualBitset()); }
     * 
     * public SubjectBitSets read(Kryo kryo, Input input, Class<SubjectBitSets> type) {
     * SubjectBitSets subjectBitSets = new SubjectBitSets();
     * subjectBitSets.setGreaterThanBitset(kryo.readObject(input, BitSet.class));
     * subjectBitSets.setEqualBitset(kryo.readObject(input, BitSet.class)); return subjectBitSets; }
     * });
     */

    if (psrObjectFile.isFile()) {
      System.out.println("Psr file exists. Trying to initialize...");
      FSTObjectInput in = new FSTObjectInput(new FileInputStream(psrObjectFile));
      predicateSubjectRanks = (HashMap<Integer, HashMap<Integer, Integer>>) in.readObject();
      in.close(); // required !
      in = new FSTObjectInput(new FileInputStream(pssObjectFile));
      predicateSubjectsSorted = (HashMap<Integer, Integer[]>) in.readObject();
      in.close(); // required !

      System.out.println(predicateSubjectRanks.size());
      System.out.println("Initialized numeric attribute index.");
    } else {
      HashMap<Integer, HashMap<Integer, Float>> tripleIDValues =
          new HashMap<Integer, HashMap<Integer, Float>>();
      Model emptyModel = ModelFactory.createDefaultModel();
      HashSet<Integer> predicatesWithFloatValues = new HashSet<Integer>();
      HashSet<Integer> predicatesWithStringValues = new HashSet<Integer>();
      Iterator<TripleID> tripleIterator = datasource.getTriples().searchAll();
      while (tripleIterator.hasNext()) {
        TripleID tripleId = tripleIterator.next();
        int predicateId = tripleId.getPredicate();
        if (predicatesWithStringValues.contains(predicateId)) {
          continue;
        }
        int subjectId = tripleId.getSubject();

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
            HashMap<Integer, Float> innerHashMap = new HashMap<Integer, Float>();
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
      for (int predicateId : predicatesWithFloatValues) {
        System.out.println("creating bitmaps for predicate " + predicateId);
        System.out.println(dictionary.getNode(predicateId, TripleComponentRole.PREDICATE).getURI());
        Set<Integer> subjectIds = tripleIDValues.get(predicateId).keySet();
        for (int subjectId : subjectIds) {
          tripleValues.add(
              new SubjectIdWithValue(subjectId, tripleIDValues.get(predicateId).get(subjectId)));
        }
        Collections.sort(tripleValues, new SubjectIdWithValueComparator(SkylinePrefFunc.MAX));
        Integer[] sortingResult = new Integer[tripleValues.size()];
        for (int i = 0; i < sortingResult.length; i++) {
          sortingResult[i] = tripleValues.get(i).getSubjectId();
        }
        predicateSubjectsSorted.put(predicateId, sortingResult);
        HashMap<Integer, Integer> subjectRanks = new HashMap<Integer, Integer>();
        int numOfTriples = tripleValues.size();
        int lastIdx = 0;
        float lastVal = tripleValues.get(0).getValue();
        for (int i = 0; i < numOfTriples; i++) {
          int subjectId = tripleValues.get(i).getSubjectId();
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
   * @param statements
   * @return
   * 
   *         private HashMap<Integer, SubjectBitSets> createBitmapsForSubjects(
   *         ArrayList<SubjectIdWithValue> subjectsWithValue) { HashMap<Integer, SubjectBitSets>
   *         currentHashMap = new HashMap<Integer, SubjectBitSets>(); for (int i = 0; i <
   *         subjectsWithValue.size(); i++) { SubjectIdWithValue currSubject =
   *         subjectsWithValue.get(i); int currSubjectId = currSubject.getSubjectId();
   *         HashSet<Integer> subjectIdsWithGreaterValues = new HashSet<Integer>(); HashSet<Integer>
   *         subjectIdsWithEqualValues = new HashSet<Integer>(); boolean isDone = false; for (int j
   *         = 0; j < subjectsWithValue.size() && !isDone; j++) { SubjectIdWithValue secondSubject =
   *         subjectsWithValue.get(j); int secondSubjectId = secondSubject.getSubjectId(); if (i > j
   *         && currSubject.getValue() != secondSubject.getValue()) {
   *         subjectIdsWithGreaterValues.add(secondSubjectId); } else if (i > j) {
   *         subjectIdsWithEqualValues.add(secondSubjectId); } else if (i < j &&
   *         currSubject.getValue() == secondSubject.getValue()) {
   *         subjectIdsWithEqualValues.add(secondSubjectId); } else if (i == j) { continue; } else {
   *         isDone = true; } } currentHashMap.put(currSubjectId, new
   *         SubjectBitSets(subjectIdsWithGreaterValues, subjectIdsWithEqualValues)); } return
   *         currentHashMap; }
   */
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
