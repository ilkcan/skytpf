package org.linkeddatafragments.util;

import org.apache.jena.graph.Triple;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdtjena.NodeDictionary;

/**
 * Util class for the project
 * 
 * @author Ilkcan Keles
 *
 */

public class Utils {
  /**
   * Converts the HDT triple to a Jena Triple.
   *
   * @param tripleId the HDT triple
   * @return the Jena triple
   */
  public static Triple toTriple(NodeDictionary dictionary, TripleID tripleId) {
    return new Triple(dictionary.getNode(tripleId.getSubject(), TripleComponentRole.SUBJECT),
        dictionary.getNode(tripleId.getPredicate(), TripleComponentRole.PREDICATE),
        dictionary.getNode(tripleId.getObject(), TripleComponentRole.OBJECT));
  }
}
