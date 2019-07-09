package org.linkeddatafragments.fragments.tpf;

import javax.servlet.http.HttpServletRequest;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.fragments.FragmentRequestParserBase;
import org.linkeddatafragments.fragments.IFragmentRequestParser;
import org.linkeddatafragments.util.TriplePatternElementParser;

/**
 * An {@link IFragmentRequestParser} for {@link ITriplePatternFragmentRequest}s.
 *
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 * 
 * @param <ConstantTermType> type for representing constants in triple patterns (i.e., URIs and
 *        literals)
 * @param <NamedVarType> type for representing named variables in triple patterns
 * @param <AnonVarType> type for representing anonymous variables in triple patterns (i.e.,
 *        variables denoted by a blank node)
 */
public class TPFRequestParser<ConstantTermType, NamedVarType, AnonVarType>
    extends FragmentRequestParserBase {

  public final TriplePatternElementParser<ConstantTermType, NamedVarType, AnonVarType> elmtParser;

  /**
   *
   * @param elmtParser triple pattern element parser
   */
  public TPFRequestParser(
      final TriplePatternElementParser<ConstantTermType, NamedVarType, AnonVarType> elmtParser) {
    this.elmtParser = elmtParser;
  }

  /**
   *
   * @param httpRequest - http request
   * @param config - config reader
   * @return the worker
   * @throws IllegalArgumentException -
   */
  @Override
  protected TPFRequestParserWorker<ConstantTermType, NamedVarType, AnonVarType> getWorker(
      final HttpServletRequest httpRequest, final ConfigReader config)
      throws IllegalArgumentException {
    return new TPFRequestParserWorker<ConstantTermType, NamedVarType, AnonVarType>(httpRequest,
        config, elmtParser);
  }
}
