package dk.aau.cs.skytpf.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.algebra.Var;
import com.github.jsonldjava.shaded.com.google.common.collect.Sets;
import dk.aau.cs.skytpf.model.BindingHashMap;
import dk.aau.cs.skytpf.model.HttpRequestConfig;
import dk.aau.cs.skytpf.model.TriplePattern;
import dk.aau.cs.skytpf.model.VarBinding;
import dk.aau.cs.skytpf.model.VarBinding.VarBindingType;

public class QueryProcessingUtils {

  private static URLCodec urlCodec = new URLCodec("utf8");

  public static ArrayList<String> getBoundVariables(ArrayList<TriplePattern> triplePatterns) {
    ArrayList<String> boundedVariables = new ArrayList<String>();
    for (TriplePattern triplePattern : triplePatterns) {
      boundedVariables.addAll(triplePattern.getListOfVars());
    }
    return boundedVariables;
  }

  public static TriplePattern findAndRemoveNextWithMaxNumberOfBV(
      ArrayList<TriplePattern> nonSkylineTPs, ArrayList<String> boundVariables) {
    if (nonSkylineTPs.isEmpty()) {
      return null;
    } else if (nonSkylineTPs.size() == 1) {
      return nonSkylineTPs.remove(0);
    }
    int maxNoOfBV = 0;
    int indexOfNextTP = 0;
    for (int i = 0; i < nonSkylineTPs.size(); i++) {
      TriplePattern currTP = nonSkylineTPs.get(i);
      int noOfBV = currTP.getNumberOfBoundVariables(boundVariables);
      if (noOfBV > maxNoOfBV) {
        maxNoOfBV = noOfBV;
        indexOfNextTP = i;
      }
    }
    return nonSkylineTPs.remove(indexOfNextTP);
  }

  public static String constructFragmentURL(String startingFragment, TriplePattern tp)
      throws EncoderException {
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

  private static boolean appendUrlParam(StringBuilder sb, Var var, String paramName,
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

  private static boolean matchesWithBinding(TriplePattern tp, Statement triple,
      BindingHashMap binding, boolean skylineFlag) {
    String subjectVarName = tp.getSubjectVarName();
    if (binding.containsKey(subjectVarName)) {
      if (!binding.get(subjectVarName).getValue().equals(triple.getSubject().toString())) {
        return false;
      }
    }

    if (!skylineFlag) {
      String predicateVarName = tp.getPredicateVarName();
      if (binding.containsKey(predicateVarName)) {
        if (!binding.get(predicateVarName).getValue().equals(triple.getPredicate().toString())) {
          return false;
        }
      }
      String objectVarName = tp.getObjectVarName();
      if (binding.containsKey(objectVarName)) {
        if (!binding.get(objectVarName).getValue().equals(triple.getObject().toString())) {
          return false;
        }
      }
    }
    return true;
  }

  public static void extendBinding(TriplePattern tp, BindingHashMap binding, Statement triple) {
    String subjectVarName = tp.getSubjectVarName();
    if (subjectVarName != null && !binding.containsKey(subjectVarName)) {
      binding.put(subjectVarName,
          new VarBinding(triple.getSubject().toString(), VarBindingType.IRI));
    }
    String predicateVarName = tp.getPredicateVarName();
    if (predicateVarName != null && !binding.containsKey(predicateVarName)) {
      binding.put(predicateVarName,
          new VarBinding(triple.getPredicate().toString(), VarBindingType.IRI));
    }
    String objectVarName = tp.getObjectVarName();
    if (objectVarName != null && !binding.containsKey(objectVarName)) {
      if (triple.getObject() instanceof Literal) {
        binding.put(objectVarName,
            new VarBinding(triple.getObject().toString(), VarBindingType.LITERAL));
      } else {
        binding.put(objectVarName,
            new VarBinding(triple.getObject().toString(), VarBindingType.IRI));
      }
    }
  }

  public static BindingHashMap createBinding(TriplePattern tp, Statement triple) {
    BindingHashMap binding = new BindingHashMap();
    String subjectVarName = tp.getSubjectVarName();
    if (subjectVarName != null) {
      binding.put(subjectVarName,
          new VarBinding(triple.getSubject().toString(), VarBindingType.IRI));
    }
    String predicateVarName = tp.getPredicateVarName();
    if (predicateVarName != null) {
      binding.put(predicateVarName,
          new VarBinding(triple.getPredicate().toString(), VarBindingType.IRI));
    }
    String objectVarName = tp.getObjectVarName();
    if (objectVarName != null) {
      if (triple.getObject() instanceof Literal) {
        binding.put(objectVarName,
            new VarBinding(triple.getObject().toString(), VarBindingType.LITERAL));
      } else {
        binding.put(objectVarName,
            new VarBinding(triple.getObject().toString(), VarBindingType.IRI));
      }
    }
    return binding;
  }

  public static void extendBinding(BindingHashMap firstBHM, BindingHashMap secondBHM) {
    Set<String> secondVarNames = secondBHM.keySet();
    Set<String> firstVarNames = firstBHM.keySet();
    Set<String> differentVarNames = Sets.difference(secondVarNames, firstVarNames);
    for (String differentVarName : differentVarNames) {
      firstBHM.put(differentVarName, secondBHM.get(differentVarName));
    }
  }

  public static ArrayList<BindingHashMap> extendBindings(ArrayList<BindingHashMap> bindings,
      TriplePattern tp, Collection<Statement> triples, boolean skylineFlag) {
    ArrayList<BindingHashMap> extendedBindings = new ArrayList<BindingHashMap>();
    if (bindings.isEmpty() && !skylineFlag) {
      for (Statement triple : triples) {
        BindingHashMap binding = new BindingHashMap();
        extendBinding(tp, binding, triple);
        extendedBindings.add(binding);
      }
    } else {
      for (BindingHashMap currentBinding : bindings) {
        for (Statement triple : triples) {
          if (matchesWithBinding(tp, triple, currentBinding, skylineFlag)) {
            BindingHashMap newBinding = new BindingHashMap(currentBinding);
            extendBinding(tp, newBinding, triple);
            extendedBindings.add(newBinding);
            break;
          }
        }
      }

    }
    return extendedBindings;
  }

  public static BindingHashMap extendBindingWithSingleTriple(BindingHashMap currentBinding,
      TriplePattern tp, Statement triple) {
    if (currentBinding == null) {
      BindingHashMap extendedBinding = new BindingHashMap();
      extendBinding(tp, extendedBinding, triple);
      return extendedBinding;
    } else {
      if (matchesWithBinding(tp, triple, currentBinding, false)) {
        BindingHashMap newBinding = new BindingHashMap(currentBinding);
        extendBinding(tp, newBinding, triple);
        return newBinding;
      } else {
        return null;
      }
    }
  }
}
