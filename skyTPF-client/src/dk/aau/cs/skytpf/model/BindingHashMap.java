package dk.aau.cs.skytpf.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class BindingHashMap {
  private HashMap<String, VarBinding> bindingMap;

  public BindingHashMap() {
    bindingMap = new HashMap<String, VarBinding>();
  }

  public BindingHashMap(BindingHashMap bindingHashMap) {
    this.bindingMap = new HashMap<String, VarBinding>(bindingHashMap.bindingMap);
  }

  public void put(String variable, VarBinding binding) {
    bindingMap.put(variable, binding);
  }

  public VarBinding get(String variable) {
    if (bindingMap.containsKey(variable)) {
      return bindingMap.get(variable);
    } else {
      return null;
    }
  }

  public Set<String> keySet() {
    return bindingMap.keySet();
  }

  public boolean containsKey(String variable) {
    return bindingMap.containsKey(variable);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    Set<String> varNames = bindingMap.keySet();
    StringBuffer output = new StringBuffer();
    for (String varName : varNames) {
      output.append(varName).append(": ").append(bindingMap.get(varName).toString()).append(" ");
    }
    return output.toString();
  }

  public int getHashKey(ArrayList<String> skylineAttributes) {
    HashSet<String> varNames = new HashSet<String>(bindingMap.keySet());
    varNames.removeAll(skylineAttributes);
    StringBuffer valuesListSb = new StringBuffer();
    for (String var : varNames) {
      valuesListSb.append(var);
      valuesListSb.append(bindingMap.get(var).getValue());
    }
    return valuesListSb.toString().hashCode();

  }

  public int getHashKey() {
    HashSet<String> varNames = new HashSet<String>(bindingMap.keySet());
    StringBuffer valuesListSb = new StringBuffer();
    for (String var : varNames) {
      valuesListSb.append(var);
      valuesListSb.append(bindingMap.get(var).getValue());
    }
    return valuesListSb.toString().hashCode();

  }

  public int getHashKeyOfSubjectBinding(String subjectVariable) {
    return bindingMap.get(subjectVariable).getValue().hashCode();
  }

  public int getNumberOfSkylineAttributes(ArrayList<String> skylineAttributes) {
    int count = 0;
    for (String skylineAttr : skylineAttributes) {
      if (bindingMap.containsKey(skylineAttr)) {
        count++;
      }
    }
    return count;

  }

  /**
   * @param skylineAttributes
   * @return
   */
  public boolean containsAtLeastOneKey(ArrayList<String> skylineAttributes) {
    for (String skylineAttr : skylineAttributes) {
      if (bindingMap.containsKey(skylineAttr)) {
        return true;
      }
    }
    return false;
  }

  public boolean containsAllKeys(ArrayList<String> skylineAttributes) {
    for (String skylineAttr : skylineAttributes) {
      if (!bindingMap.containsKey(skylineAttr)) {
        return false;
      }
    }
    return true;
  }

  public void putAll(BindingHashMap bindingHashMap) {
    bindingMap.putAll(bindingHashMap.bindingMap);
  }
}
