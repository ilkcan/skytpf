package dk.aau.cs.skytpf.main;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
import dk.aau.cs.skytpf.model.TriplePattern;

/**
 * @author Ilkcan Keles
 *
 */
public class SkyTPFClient {
  private static ArrayList<TriplePattern> triplePatterns = new ArrayList<TriplePattern>();
  private static List<ProjectionElem> projectionElemList;
  private static SkylineQueryInput input;
  private static SkylineMethod skylineMethod;

  public static void main(String[] args) {
    try {
      initializeInput(args);
      initializeQueryAndConfig();
      SkylineQueryProcessor sqp = new SkylineQueryProcessor(triplePatterns, projectionElemList,
          input, skylineMethod, true, true);
      sqp.processQuery();
      sqp.printBindings();
    } catch (

    ParseException e) {
      System.err.println("usage: java skytpf-client.jar -f startFragment -q query.sparql "
          + "-sa skylineAttributes -sp skylinePreferenceFunctions -sm skylineMethod");
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  private static void initializeQueryAndConfig() throws IOException, IllegalArgumentException {
    String queryString =
        FileUtils.readFileToString(new File(input.getQueryFile()), StandardCharsets.UTF_8);
    for (String skylineAttr : input.getSkylineAttributes()) {
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
      triplePatterns.add(new TriplePattern(statementPattern, input.getSkylineAttributes(),
          input.getSkylinePreferenceFunctions()));
    }
  }

  private static void initializeInput(String[] args)
      throws ParseException, IllegalArgumentException {
    Option optionF =
        Option.builder("f").required(true).desc("Start fragment").longOpt("startFr").build();
    optionF.setArgs(1);
    Option optionQ =
        Option.builder("q").required(true).desc("SPARQL query file").longOpt("query").build();
    optionQ.setArgs(1);
    Option optionSa = Option.builder("sa").required(true)
        .desc("Skyline attributes given in SPARQL variable format").longOpt("skyAttr").build();
    optionSa.setArgs(Option.UNLIMITED_VALUES);
    Option optionSp = Option.builder("sp").required(true)
        .desc("Skyline preference functions, MIN or MAX)").longOpt("skyPref").build();
    optionSp.setArgs(Option.UNLIMITED_VALUES);
    Option optionSm = Option.builder("sm").required(true)
        .desc("Skyline computation method 0: client-only 1: hybrid").longOpt("skyMethod").build();
    optionSm.setArgs(1);
    Options options = new Options();
    options.addOption(optionF);
    options.addOption(optionQ);
    options.addOption(optionSa);
    options.addOption(optionSp);
    options.addOption(optionSm);

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);
    input = new SkylineQueryInput();
    input.setStartFragment(commandLine.getOptionValue("f"));
    input.setQueryFile(commandLine.getOptionValue("q"));
    String[] skylineAttributes = commandLine.getOptionValues("sa");
    String[] skylinePreferenceFunctions = commandLine.getOptionValues("sp");

    if (skylineAttributes.length != skylinePreferenceFunctions.length) {
      throw new IllegalArgumentException(
          "The number of skyline attributes should be equal to the number of skyline preference functions");
    }
    input.setSkylineAttributes(skylineAttributes);
    input.setSkylinePreferenceFunctions(skylinePreferenceFunctions);
    int smArg = Integer.parseInt(commandLine.getOptionValue("sm"));
    if (smArg == 0) {
      skylineMethod = SkylineMethod.TPF_CLIENT_ONLY;
    } else if (smArg == 1) {
      skylineMethod = SkylineMethod.BRTPF_CLIENT_ONLY;
    } else {
      skylineMethod = SkylineMethod.SKYTPF;
    }

  }
}
