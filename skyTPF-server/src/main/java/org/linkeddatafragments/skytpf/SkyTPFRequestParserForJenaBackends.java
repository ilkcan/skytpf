package org.linkeddatafragments.skytpf;

import org.apache.jena.rdf.model.RDFNode;
import org.linkeddatafragments.util.TriplePatternElementParserForJena;

/**
 * An {@link BindingsRestrictedTPFRequestParser} for Jena-based backends.
 *
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class SkyTPFRequestParserForJenaBackends
    extends SkyTPFRequestParser<RDFNode, String, String> {
  private static SkyTPFRequestParserForJenaBackends instance = null;

  /**
   *
   * @return
   */
  public static SkyTPFRequestParserForJenaBackends getInstance() {
    if (instance == null) {
      instance = new SkyTPFRequestParserForJenaBackends();
    }
    return instance;
  }

  /**
   *
   */
  protected SkyTPFRequestParserForJenaBackends() {
    super(TriplePatternElementParserForJena.getInstance());
  }
}
