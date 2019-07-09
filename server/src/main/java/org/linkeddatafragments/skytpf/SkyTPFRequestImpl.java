package org.linkeddatafragments.skytpf;

import java.util.ArrayList;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.fragments.LinkedDataFragmentRequestBase;
import org.linkeddatafragments.fragments.tpf.ITriplePatternElement;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragmentRequest;

/**
 * An implementation of {@link ITriplePatternFragmentRequest}.
 *
 * @author Ilkcan Keles
 * @param <CTT>
 * @param <NVT>
 * @param <AVT>
 */
public class SkyTPFRequestImpl<CTT, NVT, AVT> extends LinkedDataFragmentRequestBase
    implements ISkyTPFRequest<CTT, NVT, AVT> {
  /**
  *
  */
  public final ITriplePatternElement<CTT, NVT, AVT> subject;

  /**
  *
  */
  public final ITriplePatternElement<CTT, NVT, AVT> predicate;

  /**
  *
  */
  public final ITriplePatternElement<CTT, NVT, AVT> object;

  /**
  * 
  */
  public final ArrayList<Binding> bindings;


  public final ArrayList<Var> boundedVariables;
  /**
   * 
   */
  public Binding pivotBinding;

  /**
   * 
   */
  public ArrayList<Var> pivotBoundedVariables;

  /**
   * 
   */
  public boolean skylineFlag;

  /**
   * 
   */
  public SkylinePrefFunc skylinePreferenceFunction;

  /**
   *
   * @param fragmentURL
   * @param datasetURL
   * @param pageNumberWasRequested
   * @param pageNumber
   * @param subject
   * @param predicate
   * @param object
   * @param bindings
   * @param boundedVariables
   * @param pivotBinding
   * @param pivotBoundedVariables
   * @param skylineFlag
   * @param skylinePreferenceFunction
   */
  public SkyTPFRequestImpl(final String fragmentURL, final String datasetURL,
      final boolean pageNumberWasRequested, final long pageNumber,
      final ITriplePatternElement<CTT, NVT, AVT> subject,
      final ITriplePatternElement<CTT, NVT, AVT> predicate,
      final ITriplePatternElement<CTT, NVT, AVT> object, final ArrayList<Binding> bindings,
      final ArrayList<Var> boundedVariables, final Binding pivotBinding,
      final ArrayList<Var> pivotBoundedVariables, final boolean skylineFlag,
      final SkylinePrefFunc skylinePreferenceFunction) {
    super(fragmentURL, datasetURL, pageNumberWasRequested, pageNumber);

    if (subject == null)
      throw new IllegalArgumentException();

    if (predicate == null)
      throw new IllegalArgumentException();

    if (object == null)
      throw new IllegalArgumentException();


    this.subject = subject;
    this.predicate = predicate;
    this.object = object;
    this.bindings = bindings;
    this.boundedVariables = boundedVariables;

    this.skylineFlag = skylineFlag;
    if (skylinePreferenceFunction == null)
      throw new IllegalArgumentException();

    this.pivotBinding = pivotBinding;
    this.pivotBoundedVariables = pivotBoundedVariables;
    this.skylinePreferenceFunction = skylinePreferenceFunction;


  }

  @Override
  public ITriplePatternElement<CTT, NVT, AVT> getSubject() {
    return subject;
  }

  @Override
  public ITriplePatternElement<CTT, NVT, AVT> getPredicate() {
    return predicate;
  }

  @Override
  public ITriplePatternElement<CTT, NVT, AVT> getObject() {
    return object;
  }

  @Override
  public ArrayList<Binding> getBindings() {
    return bindings;
  }

  @Override
  public ArrayList<Var> getBoundedVariables() {
    return boundedVariables;
  }



  @Override
  public String toString() {
    return "BindingsRestrictedTpfRequest(" + "class: " + getClass().getName() + ", subject: "
        + subject.toString() + ", predicate: " + predicate.toString() + ", object: "
        + object.toString() + ", fragmentURL: " + fragmentURL + ", isPageRequest: "
        + pageNumberWasRequested + ", pageNumber: " + pageNumber + ", number of bindings: "
        + bindings.size() + ", number of pivot variables:  " + pivotBoundedVariables.size() + ")";
  }

  @Override
  public Binding getPivotBinding() {
    return pivotBinding;
  }

  @Override
  public ArrayList<Var> getPivotBoundedVariables() {
    return pivotBoundedVariables;
  }

  @Override
  public boolean isSkyTPFRequest() {
    return skylineFlag;
  }

  @Override
  public SkylinePrefFunc getSkylinePreferenceFunction() {
    return skylinePreferenceFunction;
  }

}
