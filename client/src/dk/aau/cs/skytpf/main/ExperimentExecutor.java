package dk.aau.cs.skytpf.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.commons.io.FileUtils;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParserFactory;
import dk.aau.cs.skytpf.main.SkylineQueryInput.SkylineMethod;
import dk.aau.cs.skytpf.main.SkylineQueryInput.SkylinePrefFunc;
import dk.aau.cs.skytpf.model.HttpRequestConfig;
import dk.aau.cs.skytpf.model.TriplePattern;

public class ExperimentExecutor {

  private static ArrayList<List<ProjectionElem>> projectionElemList;
  private static ArrayList<ArrayList<TriplePattern>> triplePatterns;
  private static ArrayList<String> queries;
  private static ArrayList<SkylineQueryInput> inputs;

  private static String syntheticDataStartingFragment;// = "http://172.19.2.99:6855/";
  private static final int[] numberOfAttributes = {2, 3, 4, 5, 6};
  private static final String[] numberOfTriples =
      {"5K", "10K", "15K", "20K", "25K", "30K", "35K", "40K", "45K", "50K"};

  public static void main(String[] args)
      throws IllegalArgumentException, IOException, InterruptedException, ExecutionException {
    if (args.length != 3) {
      System.err.println("The following inputs are required: distribution, number of bindings, and starting fragment");
      return;
    }
    String dist = args[0];
    int numberOfBindings = Integer.parseInt(args[1]);
    syntheticDataStartingFragment = args[2];
    HttpRequestConfig.MAX_NUMBER_OF_BINDINGS = numberOfBindings;
    HttpRequestConfig.MAX_NUMBER_OF_SKYLINE_BINDINGS = numberOfBindings;
    executeSyntheticDataExperiments(dist, dist + "_" + numberOfBindings + ".csv");
  }

  private static void executeSyntheticDataExperiments(String dist, String outputFileName)
      throws IllegalArgumentException, IOException, InterruptedException, ExecutionException {
    PrintWriter outputPW = new PrintWriter(new FileWriter(new File(outputFileName)));
    outputPW.println("dist,na,nt,hmt,hmtnc,hmtnb,hmtnr,hst,hstnc,hstnb,hstnr,cmt,cmtnc,"
        + "cmtnb,cmtnr,cst,cstnc,cstnb,cstnr,tcmt,tcmtnc,tcmtnb,tcmtnr,tst,tstnc,tstnb,tstnr");
    triplePatterns = new ArrayList<ArrayList<TriplePattern>>();
    projectionElemList = new ArrayList<List<ProjectionElem>>();
    inputs = new ArrayList<SkylineQueryInput>();
    initializeQueriesForSyntheticData();
    for (int i = 0; i < numberOfAttributes.length; i++) {
      int noOfAttrs = numberOfAttributes[i];
      ArrayList<TriplePattern> triplePatternsOfQuery = triplePatterns.get(i);
      List<ProjectionElem> currProjectionElems = projectionElemList.get(i);
      SkylineQueryInput currInput = inputs.get(i);

      for (String nt : numberOfTriples) {
        System.out.println("Running experiments for " + dist + " distribution with " + noOfAttrs
            + " attributes and " + nt + " triples");
        String currSF = syntheticDataStartingFragment + dist + "_" + noOfAttrs + "D_" + nt;
        inputs.get(i).setStartFragment(currSF);
        SkylineQueryProcessor.NUMBER_OF_HTTP_REQUESTS.set(0);
        SkylineQueryProcessor.NUMBER_OF_BINDINGS_SENT.set(0);
        SkylineQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.set(0);
        SkylineQueryProcessor.NUMBER_OF_SKYLINE_CANDIDATES = 0;
        SkylineQueryProcessor hybridMultiThreadedQP =
            new SkylineQueryProcessor(triplePatternsOfQuery, currProjectionElems, currInput,
                SkylineMethod.SKYTPF, true, false);
        hybridMultiThreadedQP.processQuery();
        int numberOfSkylinesHMTQP = hybridMultiThreadedQP.getSkylineBindings().size();
        long hmtqpt = hybridMultiThreadedQP.getQueryProcessingTime();
        int hmtnc = SkylineQueryProcessor.NUMBER_OF_SKYLINE_CANDIDATES;
        int hmtnb = SkylineQueryProcessor.NUMBER_OF_BINDINGS_SENT.get()
            + SkylineQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.get();
        int hmtnr = SkylineQueryProcessor.NUMBER_OF_HTTP_REQUESTS.get();
        System.out.println("Hybrid-MT returned after " + Duration.ofMillis(hmtqpt) + " with "
            + hmtnc + " candidates and " + numberOfSkylinesHMTQP + " skylines.");

        SkylineQueryProcessor.NUMBER_OF_HTTP_REQUESTS.set(0);
        SkylineQueryProcessor.NUMBER_OF_BINDINGS_SENT.set(0);
        SkylineQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.set(0);
        SkylineQueryProcessor.NUMBER_OF_SKYLINE_CANDIDATES = 0;
        SkylineQueryProcessor hybridSingleThreadedQP =
            new SkylineQueryProcessor(triplePatternsOfQuery, currProjectionElems, currInput,
                SkylineMethod.SKYTPF, false, false);
        hybridSingleThreadedQP.processQuery();
        int numberOfSkylinesHSTQP = hybridSingleThreadedQP.getSkylineBindings().size();
        long hstqpt = hybridSingleThreadedQP.getQueryProcessingTime();
        int hstnc = SkylineQueryProcessor.NUMBER_OF_SKYLINE_CANDIDATES;
        int hstnb = SkylineQueryProcessor.NUMBER_OF_BINDINGS_SENT.get()
            + SkylineQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.get();
        int hstnr = SkylineQueryProcessor.NUMBER_OF_HTTP_REQUESTS.get();
        System.out.println("Hybrid-ST returned after " + Duration.ofMillis(hstqpt) + " with "
            + hstnc + " candidates and " + numberOfSkylinesHSTQP + " skylines.");

        SkylineQueryProcessor.NUMBER_OF_HTTP_REQUESTS.set(0);
        SkylineQueryProcessor.NUMBER_OF_BINDINGS_SENT.set(0);
        SkylineQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.set(0);
        SkylineQueryProcessor.NUMBER_OF_SKYLINE_CANDIDATES = 0;
        SkylineQueryProcessor clientOnlyMultiThreadedQP =
            new SkylineQueryProcessor(triplePatternsOfQuery, currProjectionElems, currInput,
                SkylineMethod.BRTPF_CLIENT_ONLY, true, false);
        clientOnlyMultiThreadedQP.processQuery();
        int numberOfSkylinesCOMTQP = clientOnlyMultiThreadedQP.getSkylineBindings().size();
        long cmtqpt = clientOnlyMultiThreadedQP.getQueryProcessingTime();
        int cmtnc = SkylineQueryProcessor.NUMBER_OF_SKYLINE_CANDIDATES;
        int cmtnb = SkylineQueryProcessor.NUMBER_OF_BINDINGS_SENT.get()
            + SkylineQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.get();
        int cmtnr = SkylineQueryProcessor.NUMBER_OF_HTTP_REQUESTS.get();
        System.out.println("BrTPF-ClientOnly-MT returned after " + Duration.ofMillis(cmtqpt)
            + " with " + cmtnc + " candidates and " + numberOfSkylinesCOMTQP + " skylines.");

        SkylineQueryProcessor.NUMBER_OF_HTTP_REQUESTS.set(0);
        SkylineQueryProcessor.NUMBER_OF_BINDINGS_SENT.set(0);
        SkylineQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.set(0);
        SkylineQueryProcessor.NUMBER_OF_SKYLINE_CANDIDATES = 0;
        SkylineQueryProcessor clientOnlySingleThreadedQP =
            new SkylineQueryProcessor(triplePatternsOfQuery, currProjectionElems, currInput,
                SkylineMethod.BRTPF_CLIENT_ONLY, false, false);
        clientOnlySingleThreadedQP.processQuery();
        int numberOfSkylinesCOSTQP = clientOnlySingleThreadedQP.getSkylineBindings().size();
        long cstqpt = clientOnlySingleThreadedQP.getQueryProcessingTime();
        int cstnc = SkylineQueryProcessor.NUMBER_OF_SKYLINE_CANDIDATES;
        int cstnb = SkylineQueryProcessor.NUMBER_OF_BINDINGS_SENT.get()
            + SkylineQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.get();
        int cstnr = SkylineQueryProcessor.NUMBER_OF_HTTP_REQUESTS.get();
        System.out.println("BrTPF-ClientOnly-ST returned after " + Duration.ofMillis(cstqpt)
            + " with " + cstnc + " candidates and " + numberOfSkylinesCOSTQP + " skylines.");

        long tcmtqpt = -1;
        int tcmtnc = -1;
        int tcmtnb = -1;
        int tcmtnr = -1;
        long tcstqpt = -1;
        int tcstnc = -1;
        int tcstnb = -1;
        int tcstnr = -1;
        if (nt.equals("10K") && dist.equals("Corr")) {
          SkylineQueryProcessor.NUMBER_OF_HTTP_REQUESTS.set(0);
          SkylineQueryProcessor.NUMBER_OF_BINDINGS_SENT.set(0);
          SkylineQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.set(0);
          SkylineQueryProcessor.NUMBER_OF_SKYLINE_CANDIDATES = 0;
          SkylineQueryProcessor tpfClientOnlyMultiThreadedQP =
              new SkylineQueryProcessor(triplePatternsOfQuery, currProjectionElems, currInput,
                  SkylineMethod.TPF_CLIENT_ONLY, true, false);
          tpfClientOnlyMultiThreadedQP.processQuery();
          int numberOfSkylinesTCOMTQP = tpfClientOnlyMultiThreadedQP.getSkylineBindings().size();
          tcmtqpt = tpfClientOnlyMultiThreadedQP.getQueryProcessingTime();
          tcmtnc = SkylineQueryProcessor.NUMBER_OF_SKYLINE_CANDIDATES;
          tcmtnb = SkylineQueryProcessor.NUMBER_OF_BINDINGS_SENT.get()
              + SkylineQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.get();
          tcmtnr = SkylineQueryProcessor.NUMBER_OF_HTTP_REQUESTS.get();
          System.out.println("TPF-ClientOnly-MT returned after " + Duration.ofMillis(tcmtqpt)
              + " with " + tcmtnc + " candidates and " + numberOfSkylinesTCOMTQP + " skylines.");

          SkylineQueryProcessor.NUMBER_OF_HTTP_REQUESTS.set(0);
          SkylineQueryProcessor.NUMBER_OF_BINDINGS_SENT.set(0);
          SkylineQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.set(0);
          SkylineQueryProcessor.NUMBER_OF_SKYLINE_CANDIDATES = 0;
          SkylineQueryProcessor tpfClientOnlySingleThreadedQP =
              new SkylineQueryProcessor(triplePatternsOfQuery, currProjectionElems, currInput,
                  SkylineMethod.TPF_CLIENT_ONLY, false, false);
          tpfClientOnlySingleThreadedQP.processQuery();
          int numberOfSkylinesTCOSTQP = tpfClientOnlySingleThreadedQP.getSkylineBindings().size();
          tcstqpt = tpfClientOnlySingleThreadedQP.getQueryProcessingTime();
          tcstnc = SkylineQueryProcessor.NUMBER_OF_SKYLINE_CANDIDATES;
          tcstnb = SkylineQueryProcessor.NUMBER_OF_BINDINGS_SENT.get()
              + SkylineQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.get();
          tcstnr = SkylineQueryProcessor.NUMBER_OF_HTTP_REQUESTS.get();
          System.out.println("TPF-ClientOnly-ST returned after " + Duration.ofMillis(tcstqpt)
              + " with " + tcstnc + " candidates and " + numberOfSkylinesTCOSTQP + " skylines.");
        }
        System.out.println(dist + "," + noOfAttrs + "," + nt + "," + hmtqpt + "," + hstqpt + ","
            + cmtqpt + "," + cstqpt + "," + tcmtqpt + "," + tcstqpt);
        System.out
            .println(hmtnc + "," + hstnc + "," + cmtnc + "," + cstnc + "," + tcmtnc + "," + tcstnc);
        if (numberOfSkylinesHMTQP != numberOfSkylinesHSTQP
            || numberOfSkylinesHMTQP != numberOfSkylinesCOMTQP
            || numberOfSkylinesHMTQP != numberOfSkylinesCOSTQP) {
          System.err.println(dist + ", " + noOfAttrs + ", " + nt + ", " + numberOfSkylinesHMTQP
              + ", " + numberOfSkylinesHSTQP + ", " + numberOfSkylinesCOMTQP + ", "
              + numberOfSkylinesCOSTQP);
        }
        outputPW.println(dist + "," + noOfAttrs + "," + nt + "," + hmtqpt + "," + hmtnc + ","
            + hmtnb + "," + hmtnr + "," + hstqpt + "," + hstnc + "," + hstnb + "," + hstnr + ","
            + cmtqpt + "," + cmtnc + "," + cmtnb + "," + cmtnr + "," + cstqpt + "," + cstnc + ","
            + cstnb + "," + cstnr + "," + tcmtqpt + "," + tcmtnc + "," + tcmtnb + "," + tcmtnr + ","
            + tcstqpt + "," + tcstnc + "," + tcstnb + "," + tcstnr);
      }

    }
    outputPW.close();
  }

  private static void initializeQueriesForSyntheticData()
      throws IllegalArgumentException, IOException {
    String queryStringBase = "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n" + " SELECT *\n"
        + " WHERE\n" + " { ?x <https://www.w3.org/1999/02/22-rdf-syntax-ns#type> foaf:book .";

    for (int noOfAtrr : numberOfAttributes) {
      ArrayList<SkylinePrefFunc> skylinePrefFuncs = new ArrayList<SkylinePrefFunc>();
      ArrayList<String> skylineAttrs = new ArrayList<String>();
      String queryString = new String(queryStringBase);
      for (int i = 1; i <= noOfAtrr; i++) {
        String attrName = "?a" + i;
        skylineAttrs.add(attrName);
        skylinePrefFuncs.add(SkylinePrefFunc.MIN);
        queryString += "?x foaf:attr_" + i + " " + attrName + " .";
      }
      queryString += "}";
      initializeQuery(queryString, skylineAttrs, skylinePrefFuncs);
      SkylineQueryInput input = new SkylineQueryInput();
      input.setSkylineAttributes(skylineAttrs);
      input.setSkylinePreferenceFunctions(skylinePrefFuncs);
      inputs.add(input);
    }
  }

  private static void initializeQuery(String queryString, ArrayList<String> skylineAttrs,
      ArrayList<SkylinePrefFunc> skylinePrefFuncs) throws IOException, IllegalArgumentException {
    ArrayList<TriplePattern> triplePatternsOfQuery = new ArrayList<TriplePattern>();
    List<ProjectionElem> pelOfQuery;
    for (String skylineAttr : skylineAttrs) {
      if (!queryString.contains(skylineAttr)) {
        throw new IllegalArgumentException(
            "The given query does not contain the skyline attribute " + skylineAttr + ".");
      }
    }
    SPARQLParserFactory factory = new SPARQLParserFactory();
    QueryParser parser = factory.getParser();
    ParsedQuery parsedQuery = parser.parseQuery(queryString, null);
    TupleExpr query = parsedQuery.getTupleExpr();
    if (query instanceof Projection) {
      Projection proj = (Projection) query;
      pelOfQuery = proj.getProjectionElemList().getElements();
    } else {
      throw new IllegalArgumentException("The given query should be a select query.");
    }
    List<StatementPattern> statementPatterns = StatementPatternCollector.process(query);
    for (StatementPattern statementPattern : statementPatterns) {
      triplePatternsOfQuery
          .add(new TriplePattern(statementPattern, skylineAttrs, skylinePrefFuncs));
    }
    triplePatterns.add(triplePatternsOfQuery);
    projectionElemList.add(pelOfQuery);
  }
}
