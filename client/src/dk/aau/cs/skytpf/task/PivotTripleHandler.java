package dk.aau.cs.skytpf.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import dk.aau.cs.skytpf.model.BindingHashMap;
import dk.aau.cs.skytpf.model.TriplePattern;
import dk.aau.cs.skytpf.util.QueryProcessingUtils;

public class PivotTripleHandler extends AbstractRDFHandler {
  private String fragmentURL;
  private TriplePattern skylineTP;
  private boolean hasNoTriple;
  private HashSet<Statement> processedTriples;
  private HashMap<Integer, BindingHashMap> bindings;
  private ArrayList<String> skylineAttributes;
  private static final int DATASET_HASH = new String("http://rdfs.org/ns/void#Dataset").hashCode();
  private static final int SUBSET_HASH = new String("http://rdfs.org/ns/void#subset").hashCode();

  /**
   * @param nonSkylineOutput
   * 
   */
  public PivotTripleHandler(String fragmentURL, TriplePattern skylineTP,
      ArrayList<String> skylineAttributes) {
    this.fragmentURL = fragmentURL;
    this.skylineTP = skylineTP;
    hasNoTriple = true;
    processedTriples = new HashSet<Statement>();
    bindings = new HashMap<Integer, BindingHashMap>();
    this.skylineAttributes = skylineAttributes;
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
      hasNoTriple = false;
      BindingHashMap binding = QueryProcessingUtils.createBinding(skylineTP, st);
      bindings.put(binding.getHashKey(skylineAttributes), binding);
    }
  }

  /**
   * @return the hasNoTriple
   */
  public boolean hasNoTriple() {
    return hasNoTriple;
  }

  /**
   * @return the bindings
   */
  public HashMap<Integer, BindingHashMap> getBindings() {
    return bindings;
  }
}
