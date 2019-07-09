package org.linkeddatafragments.brtpf;

import javax.servlet.http.HttpServletRequest;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.fragments.IFragmentRequestParser;
import org.linkeddatafragments.fragments.tpf.TPFRequestParser;
import org.linkeddatafragments.util.TriplePatternElementParser;

/**
 * An {@link IFragmentRequestParser} for {@link IBrTPFRequest}s.
 *
 * @author Ilkcan Keles
 *
 * @param <ConstantTermType> type for representing constants in triple patterns (i.e., URIs and
 *        literals)
 * @param <NamedVarType> type for representing named variables in triple patterns
 * @param <AnonVarType> type for representing anonymous variables in triple patterns (i.e.,
 *        variables denoted by a blank node)
 */
public class BrTPFRequestParser<ConstantTermType, NamedVarType, AnonVarType>
    extends TPFRequestParser<ConstantTermType, NamedVarType, AnonVarType> {


  public BrTPFRequestParser(
      TriplePatternElementParser<ConstantTermType, NamedVarType, AnonVarType> elmtParser) {
    super(elmtParser);
  }

  /**
   *
   * @param httpRequest - http request
   * @param config - config reader
   * @return the worker
   * @throws IllegalArgumentException - 
   */
  @Override
  protected BrTPFRequestParserWorker<ConstantTermType, NamedVarType, AnonVarType> getWorker(
      final HttpServletRequest httpRequest, final ConfigReader config)
      throws IllegalArgumentException {
    return new BrTPFRequestParserWorker<ConstantTermType, NamedVarType, AnonVarType>(httpRequest,
        config, elmtParser);
  }
}
