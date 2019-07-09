package dk.aau.cs.skytpf.task;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.eclipse.rdf4j.query.algebra.Var;
import com.github.jsonldjava.shaded.com.google.common.collect.Sets;
import dk.aau.cs.skytpf.model.BindingHashMap;
import dk.aau.cs.skytpf.model.HttpRequestConfig;
import dk.aau.cs.skytpf.model.TriplePattern;

public class BrtpfHttpRequestTask {
  private ArrayList<TriplePattern> tpOrder;
  private String startingFragment;
  private ArrayList<BindingHashMap> bindings;
  private int tpIdx;
  private String fragmentURL;
  private ConcurrentLinkedQueue<BindingHashMap> outputBindings;
  private static URLCodec urlCodec = new URLCodec("utf8");


  public BrtpfHttpRequestTask(ArrayList<TriplePattern> tpOrder, String startingFragment,
      ArrayList<BindingHashMap> bindings, int tpIdx,
      ConcurrentLinkedQueue<BindingHashMap> outputBindings) {
    this.tpOrder = tpOrder;
    this.startingFragment = startingFragment;
    this.bindings = bindings;
    this.tpIdx = tpIdx;
    this.outputBindings = outputBindings;
    try {
      this.fragmentURL = constructURL();
    } catch (EncoderException e) {
      e.printStackTrace();
    }
  }

  public BrtpfHttpRequestTask(ArrayList<TriplePattern> tpOrder, ArrayList<BindingHashMap> bindings,
      int tpIdx, String fragmentURL, ConcurrentLinkedQueue<BindingHashMap> outputBindings) {
    this.tpOrder = tpOrder;
    this.bindings = bindings;
    this.tpIdx = tpIdx;
    this.fragmentURL = fragmentURL;
    this.outputBindings = outputBindings;
  }

  public String getStartingFragment() {
    return startingFragment;
  }

  public ArrayList<BindingHashMap> getBindings() {
    return bindings;
  }

  private String constructURL() throws EncoderException {
    TriplePattern tp = tpOrder.get(tpIdx);
    boolean isQuestionMarkAdded = false;
    StringBuilder sb = new StringBuilder();
    isQuestionMarkAdded = appendUrlParam(sb, tp.getSubjectVar(), HttpRequestConfig.SUBJECT_PARAM,
        isQuestionMarkAdded);
    isQuestionMarkAdded = appendUrlParam(sb, tp.getPredicateVar(),
        HttpRequestConfig.PREDICATE_PARAM, isQuestionMarkAdded);
    isQuestionMarkAdded =
        appendUrlParam(sb, tp.getObjectVar(), HttpRequestConfig.OBJECT_PARAM, isQuestionMarkAdded);
    if (!bindings.isEmpty()) {
      appendBindings(sb);
    }
    return startingFragment + sb.toString();
  }

  private void appendBindings(StringBuilder sb) throws EncoderException {
    if (!bindings.isEmpty()) {
      TriplePattern tp = tpOrder.get(tpIdx);
      Set<String> varsInTP = tp.getListOfVars().stream().collect(Collectors.toSet());
      StringBuilder valuesSb = new StringBuilder();
      Set<String> boundVars = bindings.get(0).keySet();
      ArrayList<String> varsInURL = new ArrayList<String>(Sets.intersection(varsInTP, boundVars));
      valuesSb.append("(");
      valuesSb.append(String.join(" ", varsInURL));
      valuesSb.append("){");

      for (int i = 0; i < bindings.size(); i++) {
        ArrayList<String> bindingsStrList = new ArrayList<String>();
        for (int j = 0; j < varsInURL.size(); j++) {
          bindingsStrList.add(bindings.get(i).get(varsInURL.get(j)).toString());
        }
        valuesSb.append("(");
        valuesSb.append(String.join(" ", bindingsStrList));
        valuesSb.append(")");
      }
      valuesSb.append("}");
      sb.append("&").append(HttpRequestConfig.BINDINGS_PARAM).append("=")
          .append(urlCodec.encode(valuesSb.toString()));
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

  public int getTpIdx() {
    return tpIdx;
  }

  public TriplePattern getTriplePattern() {
    return tpOrder.get(tpIdx);
  }

  public String getFragmentURL() {
    return fragmentURL;
  }

  public ArrayList<TriplePattern> getTpOrder() {
    return tpOrder;
  }

  public ConcurrentLinkedQueue<BindingHashMap> getOutputBindings() {
    return outputBindings;
  }

  /**
   * @param startingFragment the startingFragment to set
   */
  public void setStartingFragment(String startingFragment) {
    this.startingFragment = startingFragment;
  }
}
