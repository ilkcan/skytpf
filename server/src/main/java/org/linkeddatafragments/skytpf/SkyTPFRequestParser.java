package org.linkeddatafragments.skytpf;

import javax.servlet.http.HttpServletRequest;
import org.linkeddatafragments.brtpf.IBrTPFRequest;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.fragments.FragmentRequestParserBase;
import org.linkeddatafragments.fragments.IFragmentRequestParser;
import org.linkeddatafragments.util.TriplePatternElementParser;

/**
 * An {@link IFragmentRequestParser} for {@link IBrTPFRequest}s.
 *
 * @author Ilkcan Keles
 *
 * @param <ConstantTermType>
 * @param <NamedVarType>
 * @param <AnonVarType>
 */
public class SkyTPFRequestParser<ConstantTermType, NamedVarType, AnonVarType>
    extends FragmentRequestParserBase {
  public final TriplePatternElementParser<ConstantTermType, NamedVarType, AnonVarType> elmtParser;

  public SkyTPFRequestParser(
      TriplePatternElementParser<ConstantTermType, NamedVarType, AnonVarType> elmtParser) {
    this.elmtParser = elmtParser;
  }

  /**
   *
   * @param httpRequest
   * @param config
   * @return
   * @throws IllegalArgumentException
   */
  @Override
  protected SkyTPFRequestParserWorker<ConstantTermType, NamedVarType, AnonVarType> getWorker(
      final HttpServletRequest httpRequest, final ConfigReader config)
      throws IllegalArgumentException {
    return new SkyTPFRequestParserWorker<ConstantTermType, NamedVarType, AnonVarType>(httpRequest,
        config, elmtParser);
  }
}
