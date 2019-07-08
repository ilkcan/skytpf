package org.linkeddatafragments.skytpf;

import java.util.ArrayList;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.brtpf.IBrTPFRequest;

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

public interface ISkyTPFRequest<ConstantTermType, NamedVarType, AnonVarType>
    extends IBrTPFRequest<ConstantTermType, NamedVarType, AnonVarType> {

  public enum SkylinePrefFunc {
    MIN, MAX
  }

  /**
  *
  */
  public final static String PARAMETERNAME_PIVOTBINDING = "pb";

  /**
   * 
   */
  public final static String PARAMETERNAME_PIVOTBOUNDVARS = "pb_bound_vars";

  /**
   * 
   */
  public final static String PARAMETERNAME_SKYLINEPREFERENCE = "sp";

  /**
   * 
   */
  public final static String PARAMETERNAME_SKYLINEFLAG = "sf";

  /**
   * Returns the pivot binding for the requested triple pattern.
   * 
   * @return
   */
  Binding getPivotBinding();

  /**
   * Returns the variables bounded by the pivot binding.
   */
  ArrayList<Var> getPivotBoundedVariables();

  /**
   * Returns the skyline flag
   */
  boolean isSkyTPFRequest();

  /**
   * Returns the skyline preference function: MIN or MAX
   */
  SkylinePrefFunc getSkylinePreferenceFunction();
}
