package org.linkeddatafragments.brtpf;

import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

public class BindingsParser {
  /**
   * Parses the given value as set of bindings.
   * 
   * @param value containing the SPARQL bindings
   * @param foundVariables a list with variables found in the VALUES clause
   *
   * @return a list with solution mappings found in the VALUES clause
   */
  public ArrayList<Binding> parseAsSetOfBindings(final String value,
      final List<Var> foundVariables) {
    if (value == null) {
      return null;
    }
    String newString = "select * where {} VALUES " + value;
    Query q = QueryFactory.create(newString);
    foundVariables.addAll(q.getValuesVariables());
    return new ArrayList<Binding>(q.getValuesData());
  }
}
