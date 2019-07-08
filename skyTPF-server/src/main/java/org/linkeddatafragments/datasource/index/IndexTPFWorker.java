package org.linkeddatafragments.datasource.index;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.linkeddatafragments.datasource.AbstractTPFWorker;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.tpf.ITriplePatternElement;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragmentRequest;

/**
 * Worker for the index
 */
public class IndexTPFWorker extends AbstractTPFWorker<RDFNode, String, String> {

  private Model model;

  /**
   * Creates a Worker for the datasource index
   * 
   * @param req
   */
  public IndexTPFWorker(final ITriplePatternFragmentRequest<RDFNode, String, String> req) {
    super(req);
  }

  public void setModel(final Model model) {
    this.model = model;
  }

  /**
   *
   * @param s
   * @param p
   * @param o
   * @param offset
   * @param limit
   * @return
   */
  @Override
  protected ILinkedDataFragment createFragment(
      final ITriplePatternElement<RDFNode, String, String> s,
      final ITriplePatternElement<RDFNode, String, String> p,
      final ITriplePatternElement<RDFNode, String, String> o, final long offset, final long limit) {
    // FIXME: The following algorithm is incorrect for cases in which
    // the requested triple pattern contains a specific variable
    // multiple times;
    // e.g., (?x foaf:knows ?x ) or (_:bn foaf:knows _:bn)
    // see https://github.com/LinkedDataFragments/Server.Java/issues/25

    final Resource subject = s.isVariable() ? null : s.asConstantTerm().asResource();
    final Property predicate = p.isVariable() ? null
        : ResourceFactory.createProperty(p.asConstantTerm().asResource().getURI());
    final RDFNode object = o.isVariable() ? null : o.asConstantTerm();

    StmtIterator listStatements = model.listStatements(subject, predicate, object);
    Model result = ModelFactory.createDefaultModel();

    long index = 0;
    while (listStatements.hasNext() && index < offset) {
      listStatements.next();
      index++;
    }

    while (listStatements.hasNext() && index < (offset + limit)) {
      result.add(listStatements.next());
    }

    final boolean isLastPage = (result.size() < offset + limit);
    return createTriplePatternFragment(result, result.size(), isLastPage);
  }

} // end of class Worker
