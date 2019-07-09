package org.linkeddatafragments.brtpf;

import org.apache.jena.rdf.model.RDFNode;
import org.linkeddatafragments.util.TriplePatternElementParserForJena;

/**
 * An {@link BrTPFRequestParser} for Jena-based backends.
 *
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class BrTPFRequestParserForJenaBackends
    extends BrTPFRequestParser<RDFNode, String, String> {
  private static BrTPFRequestParserForJenaBackends instance = null;

  /**
   *
   * @return the instance
   */
  public static BrTPFRequestParserForJenaBackends getInstance() {
    if (instance == null) {
      instance = new BrTPFRequestParserForJenaBackends();
    }
    return instance;
  }

  /**
   *
   */
  protected BrTPFRequestParserForJenaBackends() {
    super(TriplePatternElementParserForJena.getInstance());
  }
}
