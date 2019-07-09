package dk.aau.cs.skytpf.main;

import java.util.ArrayList;

public class SkylineQueryInput {
  private String startFragment;
  private String queryFile;
  private ArrayList<String> skylineAttributes;
  private ArrayList<SkylinePrefFunc> skylinePreferenceFunctions;

  public enum SkylineMethod {
    TPF_CLIENT_ONLY, BRTPF_CLIENT_ONLY, SKYTPF
  }

  public enum SkylinePrefFunc {
    MIN, MAX
  }

  public SkylineQueryInput() {

  }

  public String getStartFragment() {
    return startFragment;
  }

  public String getQueryFile() {
    return queryFile;
  }

  public ArrayList<String> getSkylineAttributes() {
    return skylineAttributes;
  }

  public ArrayList<SkylinePrefFunc> getSkylinePreferenceFunctions() {
    return skylinePreferenceFunctions;
  }

  public void setStartFragment(String startFragment) {
    this.startFragment = startFragment;
  }

  public void setQueryFile(String queryFile) {
    this.queryFile = queryFile;
  }

  public void setSkylineAttributes(String[] skylineAttributes) {
    this.skylineAttributes = new ArrayList<String>();
    for (String skylineAttr : skylineAttributes) {
      this.skylineAttributes.add(skylineAttr);
    }
  }

  public void setSkylineAttributes(ArrayList<String> skylineAttributes) {
    this.skylineAttributes = skylineAttributes;
  }

  public void setSkylinePreferenceFunctions(String[] skylinePreferenceFunctionsInput) {
    this.skylinePreferenceFunctions = new ArrayList<SkylinePrefFunc>();
    for (String skylinePrefFunc : skylinePreferenceFunctionsInput) {
      if (skylinePrefFunc.equals("MIN")) {
        this.skylinePreferenceFunctions.add(SkylinePrefFunc.MIN);
      } else {
        this.skylinePreferenceFunctions.add(SkylinePrefFunc.MAX);
      }
    }
  }

  public void setSkylinePreferenceFunctions(ArrayList<SkylinePrefFunc> skylinePreferenceFunctions) {
    this.skylinePreferenceFunctions = skylinePreferenceFunctions;
  }
}
