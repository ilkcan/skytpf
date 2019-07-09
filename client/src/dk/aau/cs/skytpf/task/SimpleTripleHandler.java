package dk.aau.cs.skytpf.task;

import java.util.ArrayList;
import java.util.HashSet;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;

public class SimpleTripleHandler extends AbstractRDFHandler {
  private ArrayList<Statement> triples;
  private String fragmentURL;
  private HashSet<Statement> processedTriples;
  private static final int DATASET_HASH = new String("http://rdfs.org/ns/void#Dataset").hashCode();
  private static final int SUBSET_HASH = new String("http://rdfs.org/ns/void#subset").hashCode();

  /**
   * 
   */
  public SimpleTripleHandler(String fragmentURL) {
    this.triples = new ArrayList<Statement>();
    this.fragmentURL = fragmentURL;
    this.processedTriples = new HashSet<Statement>();
  }

  private boolean isTripleValid(Statement st) {
    if (st.getSubject().stringValue().equals(fragmentURL)) {
      return false;
    } else if (st.getPredicate().stringValue().contains("hydra/")
        || st.getObject().stringValue().contains("hydra/")
        || st.getObject().stringValue().hashCode() == DATASET_HASH
        || st.getPredicate().stringValue().hashCode() == SUBSET_HASH) {
      return false;
    } else {
      return true;
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.openrdf.rio.helpers.AbstractRDFHandler#handleStatement(org.openrdf.model.Statement)
   */
  @Override
  public void handleStatement(Statement st) throws RDFHandlerException {
    if (processedTriples.contains(st)) {
      return;
    } else {
      processedTriples.add(st);
    }
    if (isTripleValid(st)) {
      triples.add(st);
    }
  }

  public ArrayList<Statement> getTriples() {
    return triples;
  }
}
