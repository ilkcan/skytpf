package dk.aau.cs.skytpf.main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import dk.aau.cs.skytpf.callable.SkylineLookupThread;
import dk.aau.cs.skytpf.callable.SkylinePivotHttpRequestThread;
import dk.aau.cs.skytpf.callable.SkylineRequestThread;
import dk.aau.cs.skytpf.main.SkylineQueryInput.SkylinePrefFunc;
import dk.aau.cs.skytpf.model.BindingHashMap;
import dk.aau.cs.skytpf.model.HttpRequestConfig;
import dk.aau.cs.skytpf.model.TriplePattern;
import dk.aau.cs.skytpf.task.SkylineLookupTask;
import dk.aau.cs.skytpf.task.SkylinePivotHttpRequestTask;
import dk.aau.cs.skytpf.task.SkylineRequestTask;
import dk.aau.cs.skytpf.util.QueryProcessingUtils;
import dk.aau.cs.skytpf.util.SkylineUtils;

public class HybridAlgorithmImpl {

  private ArrayList<BindingHashMap> nonSkylineOutput;
  private ArrayList<TriplePattern> skylineTPs;
  private SkylineQueryInput input;
  private BindingHashMap pivotBinding;
  private ExecutorService executorService;
  private ConcurrentHashMap<Integer, BindingHashMap> candidateBindings =
      new ConcurrentHashMap<Integer, BindingHashMap>();
  private HashMap<Integer, BindingHashMap> nonSkylineOutputHashMap;
  private final int nThreads;
  private boolean printOutput;

  private ExecutorCompletionService<Boolean> skylineECS;

  public HybridAlgorithmImpl(ArrayList<BindingHashMap> nonSkylineOutput,
      ArrayList<TriplePattern> skylineTPs, SkylineQueryInput input, int nThreads,
      boolean printOutput) {
    this.nThreads = nThreads;
    this.nonSkylineOutput = nonSkylineOutput;
    this.executorService = Executors.newFixedThreadPool(this.nThreads);
    this.skylineECS = new ExecutorCompletionService<Boolean>(executorService);
    this.nonSkylineOutputHashMap = new HashMap<Integer, BindingHashMap>();
    for (BindingHashMap bhm : nonSkylineOutput) {
      this.nonSkylineOutputHashMap.put(bhm.getHashKey(), bhm);
    }
    this.skylineTPs = skylineTPs;
    this.input = input;
    this.printOutput = printOutput;
  }

  public ArrayList<BindingHashMap> processQuery() {
    ArrayList<BindingHashMap> skylineBindings = new ArrayList<BindingHashMap>();
    try {
      if (nonSkylineOutput.isEmpty()) {
        return skylineBindings;
      }
      pivotBinding = determinePivotBinding();
      if (pivotBinding == null) {
        return skylineBindings;
      }
      processSkylineTPs();
      this.executorService.shutdown();
      this.executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

      if (printOutput) {
        System.out.println(candidateBindings.size());
      }
      getMissingAttributes();
      if (printOutput) {
        System.out.println(candidateBindings.size());
      }
      ArrayList<BindingHashMap> candidateBindingsList =
          new ArrayList<BindingHashMap>(candidateBindings.values());
      SkylineQueryProcessor.NUMBER_OF_SKYLINE_CANDIDATES = candidateBindings.size();
      return SkylineUtils.computeSkyline(candidateBindingsList.iterator(),
          input.getSkylineAttributes(), input.getSkylinePreferenceFunctions());

    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return skylineBindings;
  }

  /**
   * @param candidateBindings
   * @param skylineAttribute
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void getMissingAttributes() throws InterruptedException, ExecutionException {
    int numberOfSkylineTPs = skylineTPs.size();
    ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
    ExecutorCompletionService<HashMap<Integer, BindingHashMap>> lookupECS =
        new ExecutorCompletionService<HashMap<Integer, BindingHashMap>>(executorService);
    ArrayList<ArrayList<BindingHashMap>> bindingsWithMissingAttributes =
        new ArrayList<ArrayList<BindingHashMap>>(numberOfSkylineTPs);
    HashSet<Integer> bindingsToBeRemoved = new HashSet<Integer>();
    Set<Integer> hashKeysForBindings = candidateBindings.keySet();
    for (int i = 0; i < numberOfSkylineTPs; i++) {
      bindingsWithMissingAttributes.add(new ArrayList<BindingHashMap>());
      TriplePattern skylineTP = skylineTPs.get(i);
      String skylineAttr = skylineTP.getSkylineAttribute();
      for (int keyForBinding : hashKeysForBindings) {
        BindingHashMap candidateBinding = candidateBindings.get(keyForBinding);
        if (!candidateBinding.containsKey(skylineAttr)) {
          bindingsWithMissingAttributes.get(i).add(candidateBinding);
          bindingsToBeRemoved.add(keyForBinding);
        }
      }
    }
    for (Integer keyForBinding : bindingsToBeRemoved) {
      candidateBindings.remove(keyForBinding);
    }

    AtomicInteger numberOfTasks = new AtomicInteger(0);
    for (int i = 0; i < numberOfSkylineTPs; i++) {
      ArrayList<BindingHashMap> bindingsWithMissingAttribute = bindingsWithMissingAttributes.get(i);
      int numberOfBindings = bindingsWithMissingAttribute.size();
      TriplePattern skylineTP = skylineTPs.get(i);
      int nt = numberOfBindings / HttpRequestConfig.MAX_NUMBER_OF_BINDINGS;
      if (numberOfBindings % HttpRequestConfig.MAX_NUMBER_OF_BINDINGS != 0) {
        nt++;
      }
      numberOfTasks.addAndGet(nt);
      for (int tId = 0; tId < nt; tId++) {
        int fromIdx = tId * HttpRequestConfig.MAX_NUMBER_OF_BINDINGS;
        int toIdx = fromIdx + HttpRequestConfig.MAX_NUMBER_OF_BINDINGS;
        if (toIdx > numberOfBindings) {
          toIdx = numberOfBindings;
        }
        ArrayList<BindingHashMap> inputBindings =
            new ArrayList<BindingHashMap>(bindingsWithMissingAttribute.subList(fromIdx, toIdx));
        lookupECS.submit(new SkylineLookupThread(new SkylineLookupTask(skylineTP,
            input.getStartFragment(), inputBindings, input.getSkylineAttributes())));
      }
    }
    HashMap<Integer, BindingHashMap> lookupOutputs = new HashMap<Integer, BindingHashMap>();
    while (numberOfTasks.get() != 0) {
      HashMap<Integer, BindingHashMap> output = lookupECS.take().get();
      Set<Integer> keysForBinding = output.keySet();
      for (Integer keyForBinding : keysForBinding) {
        BindingHashMap outputBinding = output.get(keyForBinding);
        if (lookupOutputs.containsKey(keyForBinding)) {
          QueryProcessingUtils.extendBinding(lookupOutputs.get(keyForBinding), outputBinding);
        } else {
          lookupOutputs.put(keyForBinding, outputBinding);
        }
      }
      numberOfTasks.decrementAndGet();
    }

    Set<Integer> lookupOutputKeys = lookupOutputs.keySet();
    for (Integer keyForBinding : lookupOutputKeys) {
      BindingHashMap bhm = lookupOutputs.get(keyForBinding);
      if (bhm.containsAllKeys(input.getSkylineAttributes())) {
        candidateBindings.put(keyForBinding, bhm);
      }
    }

    executorService.shutdown();
    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
  }

  private void processSkylineTPs() throws InterruptedException, ExecutionException {
    ArrayList<String> skylineAttributes = input.getSkylineAttributes();
    int numberOfNonSkylineBindings = nonSkylineOutput.size();
    int numberOfTasksForEachTP =
        numberOfNonSkylineBindings / HttpRequestConfig.MAX_NUMBER_OF_SKYLINE_BINDINGS;
    if (numberOfNonSkylineBindings % HttpRequestConfig.MAX_NUMBER_OF_SKYLINE_BINDINGS != 0) {
      numberOfTasksForEachTP++;
    }
    AtomicInteger numberOfTasks = new AtomicInteger(numberOfTasksForEachTP * skylineTPs.size());
    for (int i = 0; i < numberOfTasksForEachTP; i++) {
      int fromIdx = i * HttpRequestConfig.MAX_NUMBER_OF_SKYLINE_BINDINGS;
      int toIdx = fromIdx + HttpRequestConfig.MAX_NUMBER_OF_SKYLINE_BINDINGS;
      if (toIdx > numberOfNonSkylineBindings) {
        toIdx = numberOfNonSkylineBindings;
      }
      for (TriplePattern skylineTP : skylineTPs) {
        ArrayList<BindingHashMap> currBindings =
            new ArrayList<BindingHashMap>(nonSkylineOutput.subList(fromIdx, toIdx));
        int inpIdx = skylineAttributes.indexOf(skylineTP.getObjectVarName());
        SkylinePrefFunc skylinePrefFunc = input.getSkylinePreferenceFunctions().get(inpIdx);
        SkylineRequestTask skyTPFRequestTask =
            new SkylineRequestTask(skylineTP, input.getStartFragment(), currBindings, pivotBinding,
                skylinePrefFunc, input.getSkylineAttributes(), candidateBindings);
        SkylineRequestThread strt =
            new SkylineRequestThread(skyTPFRequestTask, skylineECS, numberOfTasks);
        skylineECS.submit(strt);
      }
    }
    while (numberOfTasks.get() != 0) {
      skylineECS.take();
      numberOfTasks.decrementAndGet();
    }
    executorService.shutdown();
    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
  }



  /*
   * private BindingHashMap determinePivotBinding() throws InterruptedException, ExecutionException
   * { ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
   * ExecutorCompletionService<BindingHashMap> lookupECS = new
   * ExecutorCompletionService<BindingHashMap>(executorService); BindingHashMap pivotBinding =
   * lookupECS .submit(new SkylinePivotLookupThread(new SkylinePivotLookupTask(skylineTPs,
   * input.getStartFragment(), nonSkylineOutputHashMap, input.getSkylineAttributes()))) .get();
   * executorService.shutdownNow(); return pivotBinding; }
   */

  private BindingHashMap determinePivotBinding() throws InterruptedException, ExecutionException {
    ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
    ExecutorCompletionService<HashMap<Integer, BindingHashMap>> pivotHttpECS =
        new ExecutorCompletionService<HashMap<Integer, BindingHashMap>>(executorService);
    HashMap<Integer, BindingHashMap> allBindings = new HashMap<Integer, BindingHashMap>();
    boolean foundInCurrentPage = false;
    int numberOfTasks = skylineTPs.size();
    int numberOfRemainingTasks;
    int currentPageNumber = 1;
    List<SkylinePivotHttpRequestTask> httpRequestTasks =
        skylineTPs
            .stream().map(skylineTP -> new SkylinePivotHttpRequestTask(skylineTP,
                input.getStartFragment(), input.getSkylineAttributes()))
            .collect(Collectors.toList());
    while (!foundInCurrentPage) {
      numberOfRemainingTasks = numberOfTasks;
      for (int i = 0; i < numberOfTasks; i++) {
        SkylinePivotHttpRequestTask httpRequestTask = httpRequestTasks.get(i);
        httpRequestTask.setPageNumber(currentPageNumber);
        pivotHttpECS.submit(new SkylinePivotHttpRequestThread(httpRequestTask));
      }

      int numberOfEmptyResults = 0;
      while (numberOfRemainingTasks != 0) {
        HashMap<Integer, BindingHashMap> bindings = pivotHttpECS.take().get();
        if (bindings.isEmpty()) {
          numberOfEmptyResults++;
        } else {
          Set<Integer> keys = bindings.keySet();
          for (Integer key : keys) {
            if (!nonSkylineOutputHashMap.containsKey(key)) {
              continue;
            }
            if (allBindings.containsKey(key)) {
              BindingHashMap currBinding = allBindings.get(key);
              currBinding.putAll(bindings.get(key));
              if (currBinding.containsAllKeys(input.getSkylineAttributes())) {
                executorService.shutdownNow();
                return currBinding;
              }
            } else {
              allBindings.put(key, bindings.get(key));
              if (bindings.get(key).containsAllKeys(input.getSkylineAttributes())) {
                executorService.shutdownNow();
                return bindings.get(key);
              }
            }
          }
        }
        numberOfRemainingTasks--;
      }
      if (numberOfEmptyResults == numberOfTasks) {
        return null;
      }
      currentPageNumber++;
    }
    executorService.shutdownNow();
    return null;
  }
}
