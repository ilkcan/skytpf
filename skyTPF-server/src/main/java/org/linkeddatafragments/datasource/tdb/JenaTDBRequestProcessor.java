package org.linkeddatafragments.datasource.tdb;

import java.io.File;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.tdb.TDBFactory;
import org.linkeddatafragments.brtpf.IBrTPFRequest;
import org.linkeddatafragments.datasource.AbstractBrTPFWorker;
import org.linkeddatafragments.datasource.AbstractRequestProcessor;
import org.linkeddatafragments.datasource.AbstractSkyTPFWorker;
import org.linkeddatafragments.datasource.IFragmentRequestProcessor;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragmentRequest;
import org.linkeddatafragments.skytpf.ISkyTPFRequest;

/**
 * Implementation of {@link IFragmentRequestProcessor} that processes
 * {@link ITriplePatternFragmentRequest}s over data stored in Jena TDB.
 *
 * @author <a href="mailto:bart.hanssens@fedict.be">Bart Hanssens</a>
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 * @author Ilkcan Keles
 */
public class JenaTDBRequestProcessor extends AbstractRequestProcessor<RDFNode, String, String> {
  private final Dataset tdb;
  private final String sparql = "CONSTRUCT WHERE { ?s ?p ?o } " + "ORDER BY ?s ?p ?o";

  private final String count = "SELECT (COUNT(?s) AS ?count) WHERE { ?s ?p ?o }";

  private final Query query = QueryFactory.create(sparql, Syntax.syntaxSPARQL_11);
  private final Query countQuery = QueryFactory.create(count, Syntax.syntaxSPARQL_11);

  /**
   * Constructor
   *
   * @param tdbdir directory used for TDB backing
   */
  public JenaTDBRequestProcessor(File tdbdir) {
    this.tdb = TDBFactory.createDataset(tdbdir.getAbsolutePath());
  }

  /**
   *
   * @param request
   * @return
   * @throws IllegalArgumentException
   */
  @Override
  protected JenaTDBTPFWorker getTPFSpecificWorker(
      final ITriplePatternFragmentRequest<RDFNode, String, String> request)
      throws IllegalArgumentException {
    JenaTDBTPFWorker worker = new JenaTDBTPFWorker(request);
    worker.setJenaDatasetAndQueries(tdb, query, countQuery);
    return worker;
  }

  @Override
  protected AbstractBrTPFWorker<RDFNode, String, String> getBrTPFSpecificWorker(
      IBrTPFRequest<RDFNode, String, String> request) throws IllegalArgumentException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected AbstractSkyTPFWorker<RDFNode, String, String> getSkyTPFSpecificWorker(
      ISkyTPFRequest<RDFNode, String, String> request) throws IllegalArgumentException {
    // TODO Auto-generated method stub
    return null;
  }



}
