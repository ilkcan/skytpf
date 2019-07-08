package org.linkeddatafragments.skytpf;

import java.util.ArrayList;
import javax.servlet.http.HttpServletRequest;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.brtpf.BrTPFRequestImpl;
import org.linkeddatafragments.brtpf.BrTPFRequestParserWorker;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.fragments.tpf.TriplePatternFragmentRequestImpl;
import org.linkeddatafragments.skytpf.ISkyTPFRequest.SkylinePrefFunc;
import org.linkeddatafragments.util.TriplePatternElementParser;

/**
   *
   */
public class SkyTPFRequestParserWorker<ConstantTermType, NamedVarType, AnonVarType>
    extends BrTPFRequestParserWorker<ConstantTermType, NamedVarType, AnonVarType> {

  private Binding pivotBinding;
  private ArrayList<Var> pivotBoundedVariables;
  private boolean skylineFlag;
  private SkylinePrefFunc skylinePreferenceFunction;

  /**
   *
   * @param request
   * @param config
   */
  public SkyTPFRequestParserWorker(final HttpServletRequest request, final ConfigReader config,
      final TriplePatternElementParser<ConstantTermType, NamedVarType, AnonVarType> elmtParser) {
    super(request, config, elmtParser);
  }

  /**
   *
   * @return
   * @throws IllegalArgumentException
   */
  @Override
  public ILinkedDataFragmentRequest createFragmentRequest() throws IllegalArgumentException {
    parseBindings();
    initSkylineFlag();
    if (bindings != null && !skylineFlag) {
      return new BrTPFRequestImpl<ConstantTermType, NamedVarType, AnonVarType>(getFragmentURL(),
          getDatasetURL(), pageNumberWasRequested, pageNumber, getSubject(), getPredicate(),
          getObject(), bindings, foundVariables);
    } else if (bindings != null && skylineFlag) {
      parsePivotBinding();
      initSkylinePreferenceFunction();
      return new SkyTPFRequestImpl<ConstantTermType, NamedVarType, AnonVarType>(getFragmentURL(),
          getDatasetURL(), pageNumberWasRequested, pageNumber, getSubject(), getPredicate(),
          getObject(), bindings, foundVariables, pivotBinding, pivotBoundedVariables, skylineFlag,
          skylinePreferenceFunction);
    } else if (skylineFlag && pivotBinding != null) {
      parsePivotBinding();
      initSkylinePreferenceFunction();
      return new SkyTPFRequestImpl<ConstantTermType, NamedVarType, AnonVarType>(getFragmentURL(),
          getDatasetURL(), pageNumberWasRequested, pageNumber, getSubject(), getPredicate(),
          getObject(), null, null, pivotBinding, pivotBoundedVariables, skylineFlag,
          skylinePreferenceFunction);
    } else if (skylineFlag) {
      initSkylinePreferenceFunction();
      return new SkyTPFRequestImpl<ConstantTermType, NamedVarType, AnonVarType>(getFragmentURL(),
          getDatasetURL(), pageNumberWasRequested, pageNumber, getSubject(), getPredicate(),
          getObject(), null, null, null, null, skylineFlag, skylinePreferenceFunction);
    } else {
      return new TriplePatternFragmentRequestImpl<ConstantTermType, NamedVarType, AnonVarType>(
          getFragmentURL(), getDatasetURL(), pageNumberWasRequested, pageNumber, getSubject(),
          getPredicate(), getObject());
    }

  }

  protected void parsePivotBinding() {
    pivotBoundedVariables = new ArrayList<Var>();
    final String parameter = request.getParameter(ISkyTPFRequest.PARAMETERNAME_PIVOTBINDING);
    if (parameter == null) {
      return;
    }
    ArrayList<Binding> pivotBindings =
        bindingsParser.parseAsSetOfBindings(parameter, pivotBoundedVariables);
    if (pivotBindings.size() == 1) {
      pivotBinding = pivotBindings.get(0);
    } else {
      throw new IllegalArgumentException("Pivot binding should contain only a single binding");
    }
  }

  private void initSkylinePreferenceFunction() {
    String param = request.getParameter(ISkyTPFRequest.PARAMETERNAME_SKYLINEPREFERENCE);
    if (!param.equals("MIN") && !param.equals("MAX")) {
      throw new IllegalArgumentException(
          "Accepted values for skyline preference function are 'MAX' and 'MIN'.");
    }
    skylinePreferenceFunction = param.equals("MIN") ? SkylinePrefFunc.MIN : SkylinePrefFunc.MAX;
  }

  private void initSkylineFlag() {
    String sfParam = request.getParameter(ISkyTPFRequest.PARAMETERNAME_SKYLINEFLAG);

    if (sfParam != null && sfParam.equals("t")) {
      skylineFlag = true;
    } else {
      skylineFlag = false;
    }
  }
}
