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

/**
 * Base class for implementations of brTPF Worker.
 *
 * @param <CTT>
 * @param <NVT>
 * @param <AVT>
 * 
 * @author Ilkcan Keles
 */

public abstract class AbstractBrTPFWorker<CTT, NVT, AVT> extends AbstractWorker<CTT, NVT, AVT> {

  /**
   *
   * @param request
   */
  public AbstractBrTPFWorker(final IBrTPFRequest<CTT, NVT, AVT> request) {
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
    final IBrTPFRequest<CTT, NVT, AVT> brTpfRequest =
        (IBrTPFRequest<CTT, NVT, AVT>) request;

    return createBrTPFFragment(brTpfRequest.getSubject(), brTpfRequest.getPredicate(),
        brTpfRequest.getObject(), brTpfRequest.getBindings(), brTpfRequest.getBoundedVariables(),
        offset, limit);
  }

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
  abstract protected ILinkedDataFragment createBrTPFFragment(
      final ITriplePatternElement<CTT, NVT, AVT> subj,
      final ITriplePatternElement<CTT, NVT, AVT> pred,
      final ITriplePatternElement<CTT, NVT, AVT> obj, final ArrayList<Binding> bindings,
      final ArrayList<Var> boundVariables, final long offset, final long limit)
      throws IllegalArgumentException;

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
