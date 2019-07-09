package org.linkeddatafragments.fragments;

import javax.servlet.http.HttpServletRequest;
import org.linkeddatafragments.config.ConfigReader;

/**
 * Base class for implementations of {@link IFragmentRequestParser}.
 *
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
abstract public class FragmentRequestParserBase implements IFragmentRequestParser {
  @Override
  final public ILinkedDataFragmentRequest parseIntoFragmentRequest(
      final HttpServletRequest httpRequest, final ConfigReader config)
      throws IllegalArgumentException {
    return getWorker(httpRequest, config).createFragmentRequest();
  }

  /**
   *
   * @param httpRequest - http request
   * @param config - config reader
   * @return the worker
   * @throws IllegalArgumentException If the given HTTP request cannot be interpreted (perhaps due
   *         to missing request parameters).
   */
  abstract protected FragmentRequestParserWorkerBase getWorker(final HttpServletRequest httpRequest,
      final ConfigReader config) throws IllegalArgumentException;
}
