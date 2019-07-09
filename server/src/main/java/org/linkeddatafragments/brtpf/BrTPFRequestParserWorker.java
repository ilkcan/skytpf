package org.linkeddatafragments.brtpf;

import java.util.ArrayList;
import javax.servlet.http.HttpServletRequest;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.fragments.tpf.TPFRequestParserWorker;
import org.linkeddatafragments.fragments.tpf.TriplePatternFragmentRequestImpl;
import org.linkeddatafragments.util.TriplePatternElementParser;

public class BrTPFRequestParserWorker<ConstantTermType, NamedVarType, AnonVarType>
    extends TPFRequestParserWorker<ConstantTermType, NamedVarType, AnonVarType> {
  protected ArrayList<Var> foundVariables;
  protected ArrayList<Binding> bindings;
  protected final BindingsParser bindingsParser;

  /**
   *
   * @param request 
   * @param config 
   */
  public BrTPFRequestParserWorker(final HttpServletRequest request, final ConfigReader config,
      final TriplePatternElementParser<ConstantTermType, NamedVarType, AnonVarType> elmtParser) {
    super(request, config, elmtParser);
    bindingsParser = new BindingsParser();
  }

  /**
   *
   * @return the request
   * @throws IllegalArgumentException 
   */
  @Override
  public ILinkedDataFragmentRequest createFragmentRequest() throws IllegalArgumentException {
    parseBindings();
    if (bindings != null) {
      return new BrTPFRequestImpl<ConstantTermType, NamedVarType, AnonVarType>(getFragmentURL(),
          getDatasetURL(), pageNumberWasRequested, pageNumber, getSubject(), getPredicate(),
          getObject(), bindings, foundVariables);
    } else {
      return new TriplePatternFragmentRequestImpl<ConstantTermType, NamedVarType, AnonVarType>(
          getFragmentURL(), getDatasetURL(), pageNumberWasRequested, pageNumber, getSubject(),
          getPredicate(), getObject());
    }
  }

  protected void parseBindings() {
    foundVariables = new ArrayList<Var>();
    final String parameter = request.getParameter(IBrTPFRequest.PARAMETERNAME_BINDINGS);
    bindings = bindingsParser.parseAsSetOfBindings(parameter, foundVariables);
  }

}
