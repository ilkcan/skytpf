package org.linkeddatafragments.datasource.tdb;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.QuerySolutionMap;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.linkeddatafragments.datasource.AbstractTPFWorker;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.tpf.ITriplePatternElement;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragmentRequest;

/**
 * TPF worker for Jena TDB
 * 
 * @author Ilkcan Keles
 */

public class JenaTDBTPFWorker extends AbstractTPFWorker<RDFNode, String, String> {

  private Dataset tdb;
  private Query query;
  private Query countQuery;

  /**
   *
   * @param req
   */
  public JenaTDBTPFWorker(final ITriplePatternFragmentRequest<RDFNode, String, String> req) {
    super(req);
  }

  public void setJenaDatasetAndQueries(final Dataset tdb, final Query query,
      final Query countQuery) {
    this.tdb = tdb;
    this.query = query;
    this.countQuery = countQuery;
  }

  /**
   *
   * @param subject
   * @param predicate
   * @param object
   * @param offset
   * @param limit
   * @return
   */
  @Override
  protected ILinkedDataFragment createFragment(
      final ITriplePatternElement<RDFNode, String, String> subject,
      final ITriplePatternElement<RDFNode, String, String> predicate,
      final ITriplePatternElement<RDFNode, String, String> object, final long offset,
      final long limit) {
    // FIXME: The following algorithm is incorrect for cases in which
    // the requested triple pattern contains a specific variable
    // multiple times;
    // e.g., (?x foaf:knows ?x ) or (_:bn foaf:knows _:bn)
    // see https://github.com/LinkedDataFragments/Server.Java/issues/24

    Model model = tdb.getDefaultModel();
    QuerySolutionMap map = new QuerySolutionMap();
    if (!subject.isVariable()) {
      map.add("s", subject.asConstantTerm());
    }
    if (!predicate.isVariable()) {
      map.add("p", predicate.asConstantTerm());
    }
    if (!object.isVariable()) {
      map.add("o", object.asConstantTerm());
    }

    query.setOffset(offset);
    query.setLimit(limit);

    Model triples = ModelFactory.createDefaultModel();

    try (QueryExecution qexec = QueryExecutionFactory.create(query, model, map)) {
      qexec.execConstruct(triples);
    }

    if (triples.isEmpty()) {
      return createEmptyTriplePatternFragment();
    }

    // Try to get an estimate
    long size = triples.size();
    long estimate = -1;

    try (QueryExecution qexec = QueryExecutionFactory.create(countQuery, model, map)) {
      ResultSet results = qexec.execSelect();
      if (results.hasNext()) {
        QuerySolution soln = results.nextSolution();
        Literal literal = soln.getLiteral("count");
        estimate = literal.getLong();
      }
    }

    /*
     * GraphStatisticsHandler stats = model.getGraph().getStatisticsHandler(); if (stats != null) {
     * Node s = (subject != null) ? subject.asNode() : null; Node p = (predicate != null) ?
     * predicate.asNode() : null; Node o = (object != null) ? object.asNode() : null; estimate =
     * stats.getStatistic(s, p, o); }
     */

    // No estimate or incorrect
    if (estimate < offset + size) {
      estimate = (size == limit) ? offset + size + 1 : offset + size;
    }

    // create the fragment
    final boolean isLastPage = (estimate < offset + limit);
    return createTriplePatternFragment(triples, estimate, isLastPage);
  }

} // end of class Worker
