package dk.aau.cs.skytpf.task;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.eclipse.rdf4j.query.algebra.Var;
import dk.aau.cs.skytpf.main.SkylineQueryInput.SkylinePrefFunc;
import dk.aau.cs.skytpf.model.BindingHashMap;
import dk.aau.cs.skytpf.model.HttpRequestConfig;
import dk.aau.cs.skytpf.model.TriplePattern;

public class SkylineRequestTask {
  private TriplePattern skylineTP;
  private String startingFragment;
  private ArrayList<BindingHashMap> bindings;
  private BindingHashMap pivotBinding;
  private String fragmentURL;
  private SkylinePrefFunc skylinePrefFunc;
  private ArrayList<String> skylineAttributes;
  private boolean skylineFlag;
  private static URLCodec urlCodec = new URLCodec("utf8");
  private ConcurrentHashMap<Integer, BindingHashMap> outputBindings;


  public SkylineRequestTask(TriplePattern skylineTP, String startingFragment,
      ArrayList<BindingHashMap> bindings, BindingHashMap pivotBinding,
      SkylinePrefFunc skylinePrefFunc, ArrayList<String> skylineAttributes,
      ConcurrentHashMap<Integer, BindingHashMap> outputBindings) {
    this.skylineTP = skylineTP;
    this.startingFragment = startingFragment;
    this.bindings = bindings;
    this.pivotBinding = pivotBinding;
    this.skylineAttributes = skylineAttributes;
    if (this.bindings.size() == 1 && this.pivotBinding == null) {
      skylineFlag = false;
    } else {
      skylineFlag = true;
    }
    this.outputBindings = outputBindings;
    this.skylinePrefFunc = skylinePrefFunc;
    try {
      this.fragmentURL = constructURL();
    } catch (EncoderException e) {
      e.printStackTrace();
    }
  }

  public SkylineRequestTask(TriplePattern skylineTP, String startingFragment,
      ArrayList<BindingHashMap> bindings, BindingHashMap pivotBinding,
      SkylinePrefFunc skylinePrefFunc, ArrayList<String> skylineAttributes,
      ConcurrentHashMap<Integer, BindingHashMap> outputBindings, String fragmentURL) {
    this.skylineTP = skylineTP;
    this.startingFragment = startingFragment;
    this.bindings = bindings;
    this.pivotBinding = pivotBinding;
    this.skylinePrefFunc = skylinePrefFunc;
    this.outputBindings = outputBindings;
    this.fragmentURL = fragmentURL;
  }

  public String getStartingFragment() {
    return startingFragment;
  }

  public ArrayList<BindingHashMap> getBindings() {
    return bindings;
  }

  private String constructURL() throws EncoderException {
    boolean isQuestionMarkAdded = false;
    StringBuilder sb = new StringBuilder();
    isQuestionMarkAdded = appendUrlParam(sb, skylineTP.getSubjectVar(),
        HttpRequestConfig.SUBJECT_PARAM, isQuestionMarkAdded);
    isQuestionMarkAdded = appendUrlParam(sb, skylineTP.getPredicateVar(),
        HttpRequestConfig.PREDICATE_PARAM, isQuestionMarkAdded);
    isQuestionMarkAdded = appendUrlParam(sb, skylineTP.getObjectVar(),
        HttpRequestConfig.OBJECT_PARAM, isQuestionMarkAdded);
    appendBindings(sb);
    if (skylineFlag) {
      appendSkylineRelatedParams(sb);
    }
    return startingFragment + sb.toString();
  }

  private void appendSkylineRelatedParams(StringBuilder sb) throws EncoderException {
    sb.append("&sf=t&sp=").append(skylinePrefFunc.toString());
    if (pivotBinding != null) {
      StringBuilder pivotSb = new StringBuilder();
      String subjectVarName = skylineTP.getSubjectVarName();
      pivotSb.append("(");
      pivotSb.append(subjectVarName);
      pivotSb.append("){");

      pivotSb.append("(");
      pivotSb.append(pivotBinding.get(subjectVarName).toString());
      pivotSb.append(")");
      pivotSb.append("}");
      sb.append("&").append(HttpRequestConfig.PIVOTBINDING_PARAM).append("=")
          .append(urlCodec.encode(pivotSb.toString()));
    }

  }

  private boolean appendUrlParam(StringBuilder sb, Var var, String paramName,
      Boolean isQuestionMarkAdded) throws EncoderException {
    if (isQuestionMarkAdded) {
      if (!var.isAnonymous()) {
        sb.append("&").append(paramName).append("=?").append(var.getName());
      } else if (var.isAnonymous() && var.isConstant()) {
        sb.append("&").append(paramName).append("=")
            .append(urlCodec.encode(var.getValue().stringValue()));
      }
    } else {
      if (!var.isAnonymous()) {
        sb.append("?").append(paramName).append("=?").append(var.getName());
        return true;
      } else if (var.isAnonymous() && var.isConstant()) {
        sb.append("?").append(paramName).append("=")
            .append(urlCodec.encode(var.getValue().stringValue()));
        return true;
      }
    }
    return isQuestionMarkAdded;
  }

  private void appendBindings(StringBuilder sb) throws EncoderException {
    if (!bindings.isEmpty()) {
      StringBuilder valuesSb = new StringBuilder();
      valuesSb.append("(");
      valuesSb.append(skylineTP.getSubjectVarName());
      valuesSb.append("){");


      for (int i = 0; i < bindings.size(); i++) {
        valuesSb.append("(");
        valuesSb.append(bindings.get(i).get(skylineTP.getSubjectVarName()));
        valuesSb.append(")");
      }
      valuesSb.append("}");
      sb.append("&").append(HttpRequestConfig.BINDINGS_PARAM).append("=")
          .append(urlCodec.encode(valuesSb.toString()));
    }
  }

  public String getFragmentURL() {
    return fragmentURL;
  }

  public TriplePattern getSkylineTP() {
    return skylineTP;
  }

  /**
   * @return the skylineAttributes
   */
  public ArrayList<String> getSkylineAttributes() {
    return skylineAttributes;
  }

  public ConcurrentHashMap<Integer, BindingHashMap> getOutputBindings() {
    return outputBindings;
  }

  public void setStartingFragment(String startingFragment) {
    this.startingFragment = startingFragment;
  }

  public BindingHashMap getPivotBinding() {
    return pivotBinding;
  }

  /**
   * @return the skylinePrefFunc
   */
  public SkylinePrefFunc getSkylinePrefFunc() {
    return skylinePrefFunc;
  }
}
