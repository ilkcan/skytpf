/**
 * 
 */
package dk.aau.cs.skytpf.util;

import java.util.ArrayList;
import java.util.Iterator;
import dk.aau.cs.skytpf.main.SkylineQueryInput.SkylinePrefFunc;
import dk.aau.cs.skytpf.model.BindingHashMap;
import dk.aau.cs.skytpf.model.VarBinding;

/**
 * @author Ilkcan Keles
 *
 */
public class SkylineUtils {
  /**
   * 
   */
  public static ArrayList<BindingHashMap> computeSkyline(Iterator<BindingHashMap> bindingIterator,
      ArrayList<String> skylineAttributes, ArrayList<SkylinePrefFunc> skylinePreferenceFunctions) {
    ArrayList<BindingHashMap> skylineBindings = new ArrayList<BindingHashMap>();
    while (bindingIterator.hasNext()) {
      BindingHashMap currBinding = bindingIterator.next();
      if (skylineBindings.isEmpty()) {
        skylineBindings.add(currBinding);
      } else {
        boolean isDominated = false;
        for (BindingHashMap skylineBinding : skylineBindings) {
          isDominated = checkWhetherDominates(skylineBinding, currBinding, skylineAttributes,
              skylinePreferenceFunctions);
          if (isDominated) {
            break;
          }
        }
        if (!isDominated) {
          skylineBindings.removeIf(x -> checkWhetherDominates(currBinding, x, skylineAttributes,
              skylinePreferenceFunctions));
          skylineBindings.add(currBinding);
        }
      }
    }
    return skylineBindings;
  }

  /**
   * @param bindingA
   * @param bindingB
   * @param skylineAttributes
   * @param skylinePreferenceFunctions
   * @return
   */
  private static boolean checkWhetherDominates(BindingHashMap bindingA, BindingHashMap bindingB,
      ArrayList<String> skylineAttributes, ArrayList<SkylinePrefFunc> skylinePreferenceFunctions) {
    boolean dominates = true;
    int noOfSkylineAttrs = skylineAttributes.size();
    for (int i = 0; i < noOfSkylineAttrs; i++) {
      String skylineAttr = skylineAttributes.get(i);
      SkylinePrefFunc skylinePrefFunc = skylinePreferenceFunctions.get(i);
      VarBinding varBindingA = bindingA.get(skylineAttr);
      VarBinding varBindingB = bindingB.get(skylineAttr);
      int comparisonResult = varBindingA.compareTo(varBindingB);
      if (skylinePrefFunc == SkylinePrefFunc.MIN) {
        if (comparisonResult > 0) {
          dominates = false;
        }
      } else if (skylinePrefFunc == SkylinePrefFunc.MAX) {
        if (comparisonResult < 0) {
          dominates = false;
        }
      }
    }
    return dominates;
  }

  public static void mergeSortedBindings(ArrayList<BindingHashMap> outputSortedBindings,
      ArrayList<BindingHashMap> sortedBindings, String skylineAttribute,
      SkylinePrefFunc skylinePrefFunc) {
    int startingIdx = 0;
    for (int bindingIdx = 0; bindingIdx < sortedBindings.size(); bindingIdx++) {
      BindingHashMap bindingHM = sortedBindings.get(bindingIdx);
      VarBinding currBinding = bindingHM.get(skylineAttribute);
      boolean isAdded = false;
      for (int i = startingIdx; i < outputSortedBindings.size(); i++) {
        VarBinding outputBinding = outputSortedBindings.get(i).get(skylineAttribute);
        int comparisonResult = currBinding.compareTo(outputBinding);
        if (skylinePrefFunc == SkylinePrefFunc.MAX) {
          if (comparisonResult <= 0) {
            continue;
          } else if (comparisonResult > 0) {
            outputSortedBindings.add(i, bindingHM);
            isAdded = true;
            startingIdx = i + 1;
            break;
          }
        } else {
          if (comparisonResult >= 0) {
            continue;
          } else if (comparisonResult < 0) {
            isAdded = true;
            outputSortedBindings.add(i, bindingHM);
            startingIdx = i + 1;
            break;
          }
        }
      }
      if (!isAdded) {
        outputSortedBindings.addAll(sortedBindings.subList(bindingIdx, sortedBindings.size()));
        break;
      }
    }
  }
}
