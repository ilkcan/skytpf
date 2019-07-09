/**
 * 
 */
package dk.aau.cs.skytpf.task;

import java.io.IOException;
import java.io.InputStream;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;

/**
 * @author Ilkcan Keles
 *
 */
public class CountMetadataHandler extends AbstractRDFHandler {
  private int triplesCount;
  private boolean isSet;
  private InputStream parsedStream;
  private boolean isClosed;

  private static final int RDF_TRIPLES_HASH =
      new String("http://rdfs.org/ns/void#triples").hashCode();
  private static final int HYDRA_TOTALITEMS_HASH =
      new String("http://www.w3.org/ns/hydra/core#totalItems").hashCode();


  /**
   * 
   */
  public CountMetadataHandler(InputStream parsedStream) {
    isSet = false;
    isClosed = false;
    triplesCount = 0;
    this.parsedStream = parsedStream;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.openrdf.rio.helpers.AbstractRDFHandler#handleStatement(org.openrdf.model.Statement)
   */
  @Override
  public void handleStatement(Statement st) throws RDFHandlerException {
    if (!isSet) {
      int predicateHashCode = st.getPredicate().hashCode();
      if (predicateHashCode == RDF_TRIPLES_HASH || predicateHashCode == HYDRA_TOTALITEMS_HASH) {
        triplesCount = Integer.parseInt(st.getObject().stringValue());
        isSet = true;
      }
    } else if (!isClosed) {
      try {
        parsedStream.close();
        isClosed = true;
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * @return the triplesCount
   */
  public int getTriplesCount() {
    return triplesCount;
  }
}
