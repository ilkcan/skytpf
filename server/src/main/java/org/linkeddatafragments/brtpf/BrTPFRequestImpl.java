package org.linkeddatafragments.brtpf;

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
 * @param <CTT> type for representing constants in triple patterns (i.e., URIs and
 *        literals)
 * @param <NVT> type for representing named variables in triple patterns
 * @param <AVT> type for representing anonymous variables in triple patterns (i.e.,
 *        variables denoted by a blank node)
 */
public class BrTPFRequestImpl<CTT, NVT, AVT> extends LinkedDataFragmentRequestBase
    implements IBrTPFRequest<CTT, NVT, AVT> {

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
   * @param fragmentURL - fragment url
   * @param datasetURL - dataset url
   * @param pageNumberWasRequested - page number flag
   * @param pageNumber - page number
   * @param subject - 
   * @param predicate - 
   * @param object - 
   * @param bindings - 
   * @param boundedVariables - 
   */
  public BrTPFRequestImpl(final String fragmentURL, final String datasetURL,
      final boolean pageNumberWasRequested, final long pageNumber,
      final ITriplePatternElement<CTT, NVT, AVT> subject,
      final ITriplePatternElement<CTT, NVT, AVT> predicate,
      final ITriplePatternElement<CTT, NVT, AVT> object, final ArrayList<Binding> bindings,
      final ArrayList<Var> boundedVariables) {
    super(fragmentURL, datasetURL, pageNumberWasRequested, pageNumber);

    if (subject == null)
      throw new IllegalArgumentException();

    if (predicate == null)
      throw new IllegalArgumentException();

    if (object == null)
      throw new IllegalArgumentException();

    if (bindings == null)
      throw new IllegalArgumentException();

    this.subject = subject;
    this.predicate = predicate;
    this.object = object;
    this.bindings = bindings;
    this.boundedVariables = boundedVariables;
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
  public String toString() {
    return "BindingsRestrictedTpfRequest(" + "class: " + getClass().getName() + ", subject: "
        + subject.toString() + ", predicate: " + predicate.toString() + ", object: "
        + object.toString() + ", fragmentURL: " + fragmentURL + ", isPageRequest: "
        + pageNumberWasRequested + ", pageNumber: " + pageNumber + ", number of bindings: "
        + bindings.size() + ")";
  }

  @Override
  public ArrayList<Binding> getBindings() {
    return bindings;
  }

  @Override
  public ArrayList<Var> getBoundedVariables() {
    return boundedVariables;
  }
}
