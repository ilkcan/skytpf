package dk.aau.cs.skytpf.model;

import java.util.ArrayList;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import dk.aau.cs.skytpf.main.SkylineQueryInput.SkylinePrefFunc;

public class TriplePattern {
  private StatementPattern statementPattern;
  private ArrayList<String> listOfVars;
  private String subjectVarName;
  private String predicateVarName;
  private String objectVarName;
  private boolean isSkyline;
  private String skylineAttribute;
  private SkylinePrefFunc skylinePrefFunc;
  private int triplesCount;


  public TriplePattern(StatementPattern statementPattern, ArrayList<String> skylineAttributes,
      ArrayList<SkylinePrefFunc> skylinePrefFuncs) {
    this.statementPattern = statementPattern;
    this.listOfVars = new ArrayList<String>();
    subjectVarName = null;
    Var subjectVar = statementPattern.getSubjectVar();
    if (!subjectVar.isAnonymous() && !subjectVar.isConstant()) {
      subjectVarName = "?" + subjectVar.getName();
      listOfVars.add(subjectVarName);
    }
    predicateVarName = null;
    Var predicateVar = statementPattern.getPredicateVar();
    if (!predicateVar.isAnonymous() && !predicateVar.isConstant()) {
      predicateVarName = "?" + predicateVar.getName();
      listOfVars.add(predicateVarName);
    }
    objectVarName = null;
    Var objectVar = statementPattern.getObjectVar();
    if (!objectVar.isAnonymous() && !objectVar.isConstant()) {
      objectVarName = "?" + objectVar.getName();
      listOfVars.add(objectVarName);
    }
    if (objectVarName != null) {
      for (int i = 0; i < skylineAttributes.size(); i++) {
        String currSkylineAttribute = skylineAttributes.get(i);
        if (objectVarName.equals(currSkylineAttribute)) {
          isSkyline = true;
          this.skylineAttribute = currSkylineAttribute;
          this.skylinePrefFunc = skylinePrefFuncs.get(i);
          break;
        }
      }
    }
  }

  public boolean containsVar(String varName) {
    return listOfVars.contains(varName);
  }

  public StatementPattern getStatementPattern() {
    return statementPattern;
  }

  public int getNumberOfBoundVariables(ArrayList<String> boundVars) {
    int numberOfBV = 0;
    for (String boundVar : boundVars) {
      if (containsVar(boundVar)) {
        numberOfBV++;
      }
    }
    return numberOfBV;
  }

  public ArrayList<String> getListOfVars() {
    return listOfVars;
  }

  public Var getObjectVar() {
    return statementPattern.getObjectVar();
  }

  public Var getSubjectVar() {
    return statementPattern.getSubjectVar();
  }

  public Var getPredicateVar() {
    return statementPattern.getPredicateVar();
  }

  public String getSubjectVarName() {
    return subjectVarName;
  }

  public String getObjectVarName() {
    return objectVarName;
  }

  public String getPredicateVarName() {
    return predicateVarName;
  }

  public boolean isSkyline() {
    return isSkyline;
  }

  public String getSkylineAttribute() {
    return skylineAttribute;
  }

  public SkylinePrefFunc getSkylinePrefFunc() {
    return skylinePrefFunc;
  }

  public int getTriplesCount() {
    return triplesCount;
  }

  public void setTriplesCount(int triplesCount) {
    this.triplesCount = triplesCount;
  }
}
