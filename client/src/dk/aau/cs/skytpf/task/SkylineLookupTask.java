package dk.aau.cs.skytpf.task;

import java.util.ArrayList;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.eclipse.rdf4j.query.algebra.Var;
import dk.aau.cs.skytpf.model.BindingHashMap;
import dk.aau.cs.skytpf.model.HttpRequestConfig;
import dk.aau.cs.skytpf.model.TriplePattern;

public class SkylineLookupTask {
  private TriplePattern skylineTP;
  private String startingFragment;
  private ArrayList<BindingHashMap> bindings;
  private ArrayList<String> skylineAttributes;
  private static URLCodec urlCodec = new URLCodec("utf8");


  public SkylineLookupTask(TriplePattern skylineTP, String startingFragment,
      ArrayList<BindingHashMap> bindings, ArrayList<String> skylineAttributes) {
    this.skylineTP = skylineTP;
    this.startingFragment = startingFragment;
    this.bindings = bindings;
    this.skylineAttributes = skylineAttributes;
  }

  public String getStartingFragment() {
    return startingFragment;
  }

  public String constructURL() throws EncoderException {
    boolean isQuestionMarkAdded = false;
    StringBuilder sb = new StringBuilder();
    isQuestionMarkAdded = appendUrlParam(sb, skylineTP.getSubjectVar(),
        HttpRequestConfig.SUBJECT_PARAM, isQuestionMarkAdded);
    isQuestionMarkAdded = appendUrlParam(sb, skylineTP.getPredicateVar(),
        HttpRequestConfig.PREDICATE_PARAM, isQuestionMarkAdded);
    isQuestionMarkAdded = appendUrlParam(sb, skylineTP.getObjectVar(),
        HttpRequestConfig.OBJECT_PARAM, isQuestionMarkAdded);
    appendBindings(sb);
    return startingFragment + sb.toString();
  }

  private void appendBindings(StringBuilder sb) throws EncoderException {
    StringBuilder valuesSb = new StringBuilder();
    valuesSb.append("(");
    valuesSb.append(skylineTP.getSubjectVarName());
    valuesSb.append("){");

    for (BindingHashMap binding : bindings) {
      valuesSb.append("(");
      valuesSb.append(binding.get(skylineTP.getSubjectVarName()).toString());
      valuesSb.append(")");
    }

    valuesSb.append("}");
    sb.append("&").append(HttpRequestConfig.BINDINGS_PARAM).append("=")
        .append(urlCodec.encode(valuesSb.toString()));

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

  public void setStartingFragment(String startingFragment) {
    this.startingFragment = startingFragment;
  }

  /**
   * @return the skylineTP
   */
  public TriplePattern getSkylineTP() {
    return skylineTP;
  }

  public ArrayList<BindingHashMap> getBindings() {
    return bindings;
  }

  public ArrayList<String> getSkylineAttributes() {
    return skylineAttributes;
  }
}
