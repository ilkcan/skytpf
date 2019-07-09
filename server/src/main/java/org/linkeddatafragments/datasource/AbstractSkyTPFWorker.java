package org.linkeddatafragments.datasource;

import java.util.ArrayList;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.brtpf.IBrTPFRequest;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.fragments.tpf.ITriplePatternElement;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragment;
import org.linkeddatafragments.fragments.tpf.TriplePatternFragmentImpl;
import org.linkeddatafragments.skytpf.ISkyTPFRequest;
import org.linkeddatafragments.skytpf.ISkyTPFRequest.SkylinePrefFunc;

/**
 * Base class for implementations of brTPF Worker.
 *
 * @param <CTT>
 * @param <NVT>
 * @param <AVT>
 * 
 * @author Ilkcan Keles
 */

public abstract class AbstractSkyTPFWorker<CTT, NVT, AVT> extends AbstractWorker<CTT, NVT, AVT> {

  /**
   *
   * @param request
   */
  public AbstractSkyTPFWorker(final IBrTPFRequest<CTT, NVT, AVT> request) {
    super(request);
  }

  /**
   *
   * @return
   * @throws IllegalArgumentException
   */
  @Override
  public ILinkedDataFragment createRequestedFragment() throws IllegalArgumentException {
    final long limit = ILinkedDataFragmentRequest.TRIPLESPERPAGE;
    final long offset;
    if (request.isPageRequest())
      offset = limit * (request.getPageNumber() - 1L);
    else
      offset = 0L;

    @SuppressWarnings("unchecked")
    final ISkyTPFRequest<CTT, NVT, AVT> skyTpfRequest = (ISkyTPFRequest<CTT, NVT, AVT>) request;
    if (skyTpfRequest.getPivotBinding() == null) {
      return createSkyTpfFragment(skyTpfRequest.getSubject(), skyTpfRequest.getPredicate(),
          skyTpfRequest.getObject(), skyTpfRequest.getSkylinePreferenceFunction(), offset, limit);
    } else {
      return createSkyTpfFragment(skyTpfRequest.getSubject(), skyTpfRequest.getPredicate(),
          skyTpfRequest.getObject(), skyTpfRequest.getBindings(),
          skyTpfRequest.getBoundedVariables(), skyTpfRequest.getPivotBinding(),
          skyTpfRequest.getPivotBoundedVariables(), skyTpfRequest.getSkylinePreferenceFunction(),
          offset, limit);
    }


  }

  /**
   * @param subject
   * @param predicate
   * @param object
   * @param skylinePreferenceFunction
   * @param offset
   * @param limit
   * @return
   */
  abstract protected ILinkedDataFragment createSkyTpfFragment(
      ITriplePatternElement<CTT, NVT, AVT> subject, ITriplePatternElement<CTT, NVT, AVT> predicate,
      ITriplePatternElement<CTT, NVT, AVT> object, SkylinePrefFunc skylinePreferenceFunction,
      long offset, long limit);

  /**
   *
   * @param subj
   * @param pred
   * @param obj
   * @param bindings
   * @param boundVariables
   * @param offset
   * @param limit
   * @return
   * @throws IllegalArgumentException
   */
  abstract protected ILinkedDataFragment createSkyTpfFragment(
      final ITriplePatternElement<CTT, NVT, AVT> subj,
      final ITriplePatternElement<CTT, NVT, AVT> pred,
      final ITriplePatternElement<CTT, NVT, AVT> obj, final ArrayList<Binding> bindings,
      final ArrayList<Var> boundVariables, final Binding pivotBinding,
      final ArrayList<Var> pivotBoundedVariables, final SkylinePrefFunc skylinePreferenceFunction,
      final long offset, final long limit) throws IllegalArgumentException;

  /**
   *
   * @return
   */
  protected ITriplePatternFragment createEmptyTriplePatternFragment() {
    return new TriplePatternFragmentImpl(request.getFragmentURL(), request.getDatasetURL());
  }

  /**
   *
   * @param triples
   * @param totalSize
   * @param isLastPage
   * @return
   */
  protected ITriplePatternFragment createTriplePatternFragment(final Model triples,
      final long totalSize, final boolean isLastPage) {
    return new TriplePatternFragmentImpl(triples, totalSize, request.getFragmentURL(),
        request.getDatasetURL(), request.getPageNumber(), isLastPage);
  }

}
