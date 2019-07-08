package org.linkeddatafragments.brtpf;

import java.util.ArrayList;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragmentRequest;

/**
 * Represents a request of a Bindings-restricted Triple Pattern Fragment (brTPF).
 *
 * @param <ConstantTermType> type for representing constants in triple patterns (i.e., URIs and
 *        literals)
 * @param <NamedVarType> type for representing named variables in triple patterns
 * @param <AnonVarType> type for representing anonymous variables in triple patterns (i.e.,
 *        variables denoted by a blank node)
 *
 * @author Ilkcan Keles
 * @param <ConstantTermType>
 * @param <NamedVarType>
 * @param <AnonVarType>
 */

public interface IBrTPFRequest<ConstantTermType, NamedVarType, AnonVarType>
    extends ITriplePatternFragmentRequest<ConstantTermType, NamedVarType, AnonVarType> {
  /**
  *
  */
  public final static String PARAMETERNAME_BINDINGS = "values";

  /**
   * 
   */
  public final static String PARAMETERNAME_BOUNDVARS = "bound_vars";

  /**
   * Returns the bindings position of the requested triple pattern.
   * 
   * @return list of bindings
   */
  ArrayList<Binding> getBindings();

  /**
   * Returns the variables bounded by the bindings.
   *
   * @return list of bounded variables
   */
  ArrayList<Var> getBoundedVariables();
}
