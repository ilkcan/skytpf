package org.linkeddatafragments.datasource;

import org.linkeddatafragments.brtpf.IBrTPFRequest;
import org.linkeddatafragments.fragments.FragmentRequestParserWorkerBase;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragmentRequest;
import org.linkeddatafragments.skytpf.ISkyTPFRequest;

/**
 * Base class for implementations of {@link IFragmentRequestProcessor} that process
 * {@link ITriplePatternFragmentRequest}s.
 *
 * @param <CTT> type for representing constants in triple patterns (i.e., URIs and literals)
 * @param <NVT> type for representing named variables in triple patterns
 * @param <AVT> type for representing anonymous variables in triple patterns (i.e., variables
 *        denoted by a blank node)
 *
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 * @author Ilkcan Keles
 */
public abstract class AbstractRequestProcessor<CTT, NVT, AVT> implements IFragmentRequestProcessor {
  @Override
  public void close() {}

  /**
   * Create an {@link ILinkedDataFragment} from {@link ILinkedDataFragmentRequest}
   *
   * @param request
   * @return
   * @throws IllegalArgumentException
   */
  @Override
  final public ILinkedDataFragment createRequestedFragment(final ILinkedDataFragmentRequest request)
      throws IllegalArgumentException {
    return getWorker(request).createRequestedFragment();
  }

  /**
   * Get the {@link Worker} from {@link ILinkedDataFragmentRequest}
   *
   * @param request
   * @return
   * @throws IllegalArgumentException
   */
  @SuppressWarnings("unchecked")
  protected final AbstractWorker<CTT, NVT, AVT> getWorker(final ILinkedDataFragmentRequest request)
      throws IllegalArgumentException {
    if (request instanceof ISkyTPFRequest<?, ?, ?>) {
      final ISkyTPFRequest<CTT, NVT, AVT> skyTpfRequest = (ISkyTPFRequest<CTT, NVT, AVT>) request;
      return getSkyTPFSpecificWorker(skyTpfRequest);
    } else if (request instanceof IBrTPFRequest<?, ?, ?>) {
      final IBrTPFRequest<CTT, NVT, AVT> brTpfRequest = (IBrTPFRequest<CTT, NVT, AVT>) request;
      return getBrTPFSpecificWorker(brTpfRequest);
    } else {
      final ITriplePatternFragmentRequest<CTT, NVT, AVT> tpfRequest =
          (ITriplePatternFragmentRequest<CTT, NVT, AVT>) request;
      return getTPFSpecificWorker(tpfRequest);
    }
  }

  /**
   *
   * @param request
   * @return
   * @throws IllegalArgumentException
   */
  abstract protected AbstractSkyTPFWorker<CTT, NVT, AVT> getSkyTPFSpecificWorker(
      final ISkyTPFRequest<CTT, NVT, AVT> request) throws IllegalArgumentException;

  /**
   *
   * @param request
   * @return
   * @throws IllegalArgumentException
   */
  abstract protected AbstractTPFWorker<CTT, NVT, AVT> getTPFSpecificWorker(
      final ITriplePatternFragmentRequest<CTT, NVT, AVT> request) throws IllegalArgumentException;

  /**
   *
   * @param request
   * @return
   * @throws IllegalArgumentException
   */
  abstract protected AbstractBrTPFWorker<CTT, NVT, AVT> getBrTPFSpecificWorker(
      final IBrTPFRequest<CTT, NVT, AVT> request) throws IllegalArgumentException;
}
