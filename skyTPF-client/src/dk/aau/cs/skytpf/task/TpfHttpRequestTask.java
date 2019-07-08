package dk.aau.cs.skytpf.task;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.eclipse.rdf4j.query.algebra.Var;
import dk.aau.cs.skytpf.model.BindingHashMap;
import dk.aau.cs.skytpf.model.HttpRequestConfig;
import dk.aau.cs.skytpf.model.TriplePattern;

public class TpfHttpRequestTask {
  private ArrayList<TriplePattern> tpOrder;
  private String startingFragment;
  private BindingHashMap binding;
  private int tpIdx;
  private String fragmentURL;
  private ConcurrentLinkedQueue<BindingHashMap> outputBindings;
  private static URLCodec urlCodec = new URLCodec("utf8");


  public TpfHttpRequestTask(ArrayList<TriplePattern> tpOrder, String startingFragment,
      BindingHashMap binding, int tpIdx, ConcurrentLinkedQueue<BindingHashMap> outputBindings) {
    this.tpOrder = tpOrder;
    this.startingFragment = startingFragment;
    this.binding = binding;
    this.tpIdx = tpIdx;
    this.outputBindings = outputBindings;
    try {
      this.fragmentURL = constructURL();
    } catch (EncoderException e) {
      e.printStackTrace();
    }
  }

  public TpfHttpRequestTask(ArrayList<TriplePattern> tpOrder, BindingHashMap binding, int tpIdx,
      String fragmentURL, ConcurrentLinkedQueue<BindingHashMap> outputBindings) {
    this.tpOrder = tpOrder;
    this.binding = binding;
    this.tpIdx = tpIdx;
    this.fragmentURL = fragmentURL;
    this.outputBindings = outputBindings;

  }

  public String getStartingFragment() {
    return startingFragment;
  }

  public BindingHashMap getBinding() {
    return binding;
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
    return startingFragment + sb.toString();
  }

  private boolean appendUrlParam(StringBuilder sb, Var var, String paramName,
      Boolean isQuestionMarkAdded) throws EncoderException {
    if (isQuestionMarkAdded) {
      if (!var.isAnonymous()) {
        if (binding != null && binding.containsKey("?" + var.getName())) {
          sb.append("&").append(paramName).append("=")
              .append(urlCodec.encode(binding.get("?" + var.getName()).getValue()));
        } else {
          sb.append("&").append(paramName).append("=?").append(var.getName());
        }
      } else if (var.isAnonymous() && var.isConstant()) {
        sb.append("&").append(paramName).append("=")
            .append(urlCodec.encode(var.getValue().stringValue()));
      }
    } else {
      if (!var.isAnonymous()) {
        if (binding != null && binding.containsKey("?" + var.getName())) {
          sb.append("?").append(paramName).append("=")
              .append(urlCodec.encode(binding.get("?" + var.getName()).getValue()));
        } else {
          sb.append("?").append(paramName).append("=?").append(var.getName());
        }
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
