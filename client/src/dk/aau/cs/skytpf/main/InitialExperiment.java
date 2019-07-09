package dk.aau.cs.skytpf.main;

import java.io.File;
import java.io.IOException;
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
import dk.aau.cs.skytpf.model.TriplePattern;

public class InitialExperiment {

  private static List<ProjectionElem> projectionElemList;
  private static ArrayList<TriplePattern> triplePatterns;

  public static void main(String[] args)
      throws IllegalArgumentException, IOException, InterruptedException, ExecutionException {
    String startingFragment = "http://172.19.2.99:6855/";
    String[] dists = {"Ind", "Corr", "ACorr"};
    String[] numberOfTriples = {"5K", "10K"};
    ArrayList<String> skylineAttrs = new ArrayList<String>();
    skylineAttrs.add("?a1");
    skylineAttrs.add("?a2");
    skylineAttrs.add("?a3");
    ArrayList<SkylinePrefFunc> skylinePrefFuncs = new ArrayList<SkylinePrefFunc>();
    skylinePrefFuncs.add(SkylinePrefFunc.MAX);
    skylinePrefFuncs.add(SkylinePrefFunc.MAX);
    skylinePrefFuncs.add(SkylinePrefFunc.MAX);
    String queryFile = "input/query_generated.sparql";


    initializeQueryAndConfig(queryFile, skylineAttrs, skylinePrefFuncs);

    for (String dist : dists) {
      for (String nt : numberOfTriples) {
        String currSF = startingFragment + dist + "_3D_" + nt;
        SkylineQueryInput input = new SkylineQueryInput();
        input.setStartFragment(currSF);
        input.setSkylineAttributes(skylineAttrs);
        input.setSkylinePreferenceFunctions(skylinePrefFuncs);
        SkylineQueryProcessor sqp = new SkylineQueryProcessor(triplePatterns, projectionElemList,
            input, SkylineMethod.TPF_CLIENT_ONLY, true, false);
        sqp.processQuery();
        int numberOfSkylines = sqp.getSkylineBindings().size();
        long tpfClientOnlyQPT = sqp.getQueryProcessingTime();
        SkylineQueryProcessor sqp2 = new SkylineQueryProcessor(triplePatterns, projectionElemList,
            input, SkylineMethod.BRTPF_CLIENT_ONLY, true, false);
        sqp2.processQuery();
        int numberOfSkylines2 = sqp2.getSkylineBindings().size();
        long clientOnlyQPT = sqp2.getQueryProcessingTime();
        SkylineQueryProcessor sqp3 = new SkylineQueryProcessor(triplePatterns, projectionElemList,
            input, SkylineMethod.SKYTPF, true, false);
        sqp3.processQuery();
        long hybridQPT = sqp3.getQueryProcessingTime();
        int numberOfSkylines3 = sqp3.getSkylineBindings().size();
        if (numberOfSkylines != numberOfSkylines2 || numberOfSkylines != numberOfSkylines3) {
          System.err.println(dist + "," + nt);
        }
        System.out.println(dist + "," + nt + "," + Duration.ofMillis(tpfClientOnlyQPT) + ","
            + Duration.ofMillis(clientOnlyQPT) + "," + Duration.ofMillis(hybridQPT));
      }
    }
  }

  private static void initializeQueryAndConfig(String queryFile, ArrayList<String> skylineAttrs,
      ArrayList<SkylinePrefFunc> skylinePrefFuncs) throws IOException, IllegalArgumentException {
    triplePatterns = new ArrayList<TriplePattern>();
    String queryString = FileUtils.readFileToString(new File(queryFile), StandardCharsets.UTF_8);
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
      projectionElemList = proj.getProjectionElemList().getElements();
    } else {
      throw new IllegalArgumentException("The given query should be a select query.");
    }
    List<StatementPattern> statementPatterns = StatementPatternCollector.process(query);
    for (StatementPattern statementPattern : statementPatterns) {
      triplePatterns.add(new TriplePattern(statementPattern, skylineAttrs, skylinePrefFuncs));
    }
  }
}
