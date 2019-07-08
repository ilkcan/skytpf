package org.linkeddatafragments.datasource;

import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;

/**
 * Processes {@link ILinkedDataFragmentRequest}s
 * 
 * @author Ilkcan Keles
 */
public abstract class AbstractWorker<CTT, NVT, AVT> {

  /**
   * The {@link ILinkedDataFragmentRequest} to process
   */
  public final ILinkedDataFragmentRequest request;

  /**
   * Create a Worker
   * 
   * @param request
   */
  public AbstractWorker(final ILinkedDataFragmentRequest request) {
    this.request = request;
  }

  /**
   * Create the requested {@link ILinkedDataFragment}
   * 
   * @return The ILinkedDataFragment
   * @throws IllegalArgumentException
   */
  abstract public ILinkedDataFragment createRequestedFragment() throws IllegalArgumentException;

}
