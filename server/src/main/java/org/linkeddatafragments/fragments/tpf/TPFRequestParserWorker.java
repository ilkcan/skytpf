package org.linkeddatafragments.fragments.tpf;

import javax.servlet.http.HttpServletRequest;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.fragments.FragmentRequestParserWorkerBase;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.util.TriplePatternElementParser;

public class TPFRequestParserWorker<ConstantTermType, NamedVarType, AnonVarType>
    extends FragmentRequestParserWorkerBase {

  public final TriplePatternElementParser<ConstantTermType, NamedVarType, AnonVarType> elmtParser;

  /**
   *
   * @param request
   * @param config
   */
  public TPFRequestParserWorker(final HttpServletRequest request, final ConfigReader config,
      final TriplePatternElementParser<ConstantTermType, NamedVarType, AnonVarType> elmtParser) {
    super(request, config);
    this.elmtParser = elmtParser;
  }

  /**
   *
   * @return
   * @throws IllegalArgumentException
   */
  @Override
  public ILinkedDataFragmentRequest createFragmentRequest() throws IllegalArgumentException {
    return new TriplePatternFragmentRequestImpl<ConstantTermType, NamedVarType, AnonVarType>(
        getFragmentURL(), getDatasetURL(), pageNumberWasRequested, pageNumber, getSubject(),
        getPredicate(), getObject());
  }

  /**
   *
   * @return
   */
  public ITriplePatternElement<ConstantTermType, NamedVarType, AnonVarType> getSubject() {
    return getParameterAsTriplePatternElement(ITriplePatternFragmentRequest.PARAMETERNAME_SUBJ);
  }

  /**
   *
   * @return
   */
  public ITriplePatternElement<ConstantTermType, NamedVarType, AnonVarType> getPredicate() {
    return getParameterAsTriplePatternElement(ITriplePatternFragmentRequest.PARAMETERNAME_PRED);
  }

  /**
   *
   * @return
   */
  public ITriplePatternElement<ConstantTermType, NamedVarType, AnonVarType> getObject() {
    return getParameterAsTriplePatternElement(ITriplePatternFragmentRequest.PARAMETERNAME_OBJ);
  }

  /**
   *
   * @param paramName
   * @return
   */
  public ITriplePatternElement<ConstantTermType, NamedVarType, AnonVarType> getParameterAsTriplePatternElement(
      final String paramName) {
    final String parameter = request.getParameter(paramName);
    return elmtParser.parseIntoTriplePatternElement(parameter);
  }

} // end of class Worker
