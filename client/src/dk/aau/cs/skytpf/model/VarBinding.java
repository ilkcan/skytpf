package dk.aau.cs.skytpf.model;

import java.text.ParseException;

public class VarBinding {
  private String value;
  private VarBindingType type;
  private Double numberValue;

  public enum VarBindingType {
    IRI, LITERAL
  }

  public VarBinding(String value, VarBindingType type) {
    this.value = value;
    this.type = type;
  }

  @Override
  public String toString() {
    if (type == VarBindingType.IRI) {
      return "<" + value + ">";
    } else {
      return value;
    }
  }

  public String getValue() {
    return value;
  }

  public Double getNumberValue() throws ParseException {
    if (numberValue == null) {
      String[] splitted = this.value.split("\\^\\^");
      numberValue = Double.parseDouble(splitted[0].replace("\"", ""));
    }
    return numberValue;
  }

  public int compareTo(VarBinding otherVarBinding) {
    try {
      Double val = this.getNumberValue();
      Double otherVal = otherVarBinding.getNumberValue();
      return val.compareTo(otherVal);
    } catch (Exception e) {
      return value.compareTo(otherVarBinding.value);
    }
  }
}
