package dk.aau.cs.skytpf.task;

import java.util.ArrayList;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.eclipse.rdf4j.query.algebra.Var;
import dk.aau.cs.skytpf.model.HttpRequestConfig;
import dk.aau.cs.skytpf.model.TriplePattern;

public class SkylinePivotHttpRequestTask {
  private TriplePattern skylineTP;
  private String startingFragment;
  private ArrayList<String> skylineAttributes;
  private int pageNumber;
  private static URLCodec urlCodec = new URLCodec("utf8");


  public SkylinePivotHttpRequestTask(TriplePattern skylineTP, String startingFragment,
      ArrayList<String> skylineAttributes) {
    this.skylineTP = skylineTP;
    this.startingFragment = startingFragment;
    this.pageNumber = 1;
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
    sb.append("&sf=t&sp=").append(skylineTP.getSkylinePrefFunc().toString());
    if (pageNumber != 1) {
      sb.append("&page=").append(pageNumber);
    }
    return startingFragment + sb.toString();
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

  public TriplePattern getSkylineTP() {
    return skylineTP;
  }

  public void setPageNumber(int pageNumber) {
    this.pageNumber = pageNumber;
  }

  /**
   * @return the skylineAttributes
   */
  public ArrayList<String> getSkylineAttributes() {
    return skylineAttributes;
  }
}
