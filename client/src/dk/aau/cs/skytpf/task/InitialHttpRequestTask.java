package dk.aau.cs.skytpf.task;

import org.apache.commons.codec.EncoderException;
import dk.aau.cs.skytpf.model.TriplePattern;
import dk.aau.cs.skytpf.util.QueryProcessingUtils;

public class InitialHttpRequestTask {
  private String startingFragment;
  private TriplePattern triplePattern;
  private String fragmentURL;


  public InitialHttpRequestTask(String startingFragment, TriplePattern triplePattern) {
    this.startingFragment = startingFragment;
    this.triplePattern = triplePattern;
    try {
      this.fragmentURL = QueryProcessingUtils.constructFragmentURL(startingFragment, triplePattern);
    } catch (EncoderException e) {
      e.printStackTrace();
    }
  }

  public String getStartingFragment() {
    return startingFragment;
  }

  public TriplePattern getTriplePattern() {
    return triplePattern;
  }

  public String getFragmentURL() {
    return fragmentURL;
  }
}
