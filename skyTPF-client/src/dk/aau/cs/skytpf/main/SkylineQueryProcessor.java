/**
 * 
 */
package dk.aau.cs.skytpf.main;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.client.fluent.Content;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import dk.aau.cs.skytpf.callable.BrtpfHttpRequestThread;
import dk.aau.cs.skytpf.callable.InitialHttpRequestThread;
import dk.aau.cs.skytpf.callable.TpfHttpRequestThread;
import dk.aau.cs.skytpf.main.SkylineQueryInput.SkylineMethod;
import dk.aau.cs.skytpf.model.BindingHashMap;
import dk.aau.cs.skytpf.model.TriplePattern;
import dk.aau.cs.skytpf.task.BrtpfHttpRequestTask;
import dk.aau.cs.skytpf.task.InitialHttpRequestTask;
import dk.aau.cs.skytpf.task.TpfHttpRequestTask;
import dk.aau.cs.skytpf.util.QueryProcessingUtils;
import dk.aau.cs.skytpf.util.SkylineUtils;

/**
 * @author Ilkcan Keles
 *
 */
public class SkylineQueryProcessor {
  private ConcurrentHashMap<String, Content> httpResponseCache;
  private ArrayList<TriplePattern> triplePatterns;
  private List<ProjectionElem> projectionElemList;
  private final int nThreads;
  private ExecutorService executorService;
  private ExecutorCompletionService<Boolean> executorCompletionService;
  private ArrayList<TriplePattern> nonSkylineTPs;
  private ArrayList<TriplePattern> nonSkylineTPOrder = new ArrayList<TriplePattern>();
  private ArrayList<TriplePattern> skylineTPs;

  private ConcurrentLinkedQueue<BindingHashMap> outputBindings;
  private ArrayList<BindingHashMap> skylineBindings;
  private AtomicInteger numberOfTasks;
  private SkylineQueryInput input;
  private long queryProcessingTime;
  private SkylineMethod skylineMethod;
  public boolean printOutput;
  public static AtomicInteger NUMBER_OF_HTTP_REQUESTS = new AtomicInteger(0);
  public static AtomicInteger NUMBER_OF_BINDINGS_SENT = new AtomicInteger(0);
  public static AtomicInteger NUMBER_OF_BINDINGS_RECEIVED = new AtomicInteger(0);
  public static int NUMBER_OF_SKYLINE_CANDIDATES = 0;

  /**
   * 
   */
  public SkylineQueryProcessor(ArrayList<TriplePattern> triplePatterns,
      List<ProjectionElem> projectionElemList, SkylineQueryInput input, SkylineMethod skylineMethod,
      boolean isMultiThreaded, boolean printOutput) {
    this.triplePatterns = triplePatterns;
    this.projectionElemList = projectionElemList;
    this.input = input;
    nonSkylineTPs = new ArrayList<TriplePattern>();
    nonSkylineTPOrder = new ArrayList<TriplePattern>();
    skylineTPs = new ArrayList<TriplePattern>();
    httpResponseCache = new ConcurrentHashMap<String, Content>();
    outputBindings = new ConcurrentLinkedQueue<BindingHashMap>();
    this.skylineMethod = skylineMethod;
    if (isMultiThreaded) {
      nThreads = Runtime.getRuntime().availableProcessors();
    } else {
      nThreads = 1;
    }
    this.printOutput = printOutput;
  }


  /**
   * @throws ExecutionException
   * @throws InterruptedException
   * 
   */
  private void initializeOrderOfTriplePatterns() throws InterruptedException, ExecutionException {
    int minTriplesCount = Integer.MAX_VALUE;
    TriplePattern firstTriplePattern = null;
    ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
    for (TriplePattern triplePattern : triplePatterns) {
      if (skylineMethod == SkylineMethod.SKYTPF && triplePattern.isSkyline()) {
        skylineTPs.add(triplePattern);
      } else {
        InitialHttpRequestTask httpRequestTask =
            new InitialHttpRequestTask(input.getStartFragment(), triplePattern);
        int triplesCount = executorService
            .submit(new InitialHttpRequestThread(httpRequestTask, httpResponseCache)).get();

        nonSkylineTPs.add(triplePattern);
        if (triplesCount < minTriplesCount) {
          minTriplesCount = triplesCount;
          firstTriplePattern = triplePattern;
        }

      }
    }
    executorService.shutdown();
    if (firstTriplePattern != null) {
      nonSkylineTPOrder.add(firstTriplePattern);
      nonSkylineTPs.remove(firstTriplePattern);
      orderRemainingNonSkylineTPs();
    }
  }

  private void orderRemainingNonSkylineTPs() {
    while (!nonSkylineTPs.isEmpty()) {
      ArrayList<String> boundVariables = QueryProcessingUtils.getBoundVariables(nonSkylineTPOrder);
      TriplePattern nextTP =
          QueryProcessingUtils.findAndRemoveNextWithMaxNumberOfBV(nonSkylineTPs, boundVariables);
      nonSkylineTPOrder.add(nextTP);
    }
  }

  private void initializeProcessingNonSkylineQuery() {
    if (skylineMethod == SkylineMethod.TPF_CLIENT_ONLY) {
      executorService = Executors.newFixedThreadPool(nThreads);
      executorCompletionService = new ExecutorCompletionService<Boolean>(executorService);
      TpfHttpRequestTask httpRequestTask = new TpfHttpRequestTask(nonSkylineTPOrder,
          input.getStartFragment(), null, 0, outputBindings);
      numberOfTasks = new AtomicInteger(1);
      TpfHttpRequestThread hrt = new TpfHttpRequestThread(httpRequestTask, httpResponseCache,
          executorCompletionService, numberOfTasks);
      executorCompletionService.submit(hrt);
    } else {
      executorService = Executors.newFixedThreadPool(nThreads);
      executorCompletionService = new ExecutorCompletionService<Boolean>(executorService);
      BrtpfHttpRequestTask httpRequestTask = new BrtpfHttpRequestTask(nonSkylineTPOrder,
          input.getStartFragment(), new ArrayList<BindingHashMap>(), 0, outputBindings);
      numberOfTasks = new AtomicInteger(1);
      BrtpfHttpRequestThread hrt = new BrtpfHttpRequestThread(httpRequestTask, httpResponseCache,
          executorCompletionService, numberOfTasks);
      executorCompletionService.submit(hrt);
    }
  }

  public void processQuery() throws InterruptedException, ExecutionException {
    long start = System.currentTimeMillis();
    initializeOrderOfTriplePatterns();
    initializeProcessingNonSkylineQuery();
    while (numberOfTasks.get() != 0) {
      executorCompletionService.take();
      numberOfTasks.decrementAndGet();
    }
    executorService.shutdown();
    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    if (skylineMethod == SkylineMethod.BRTPF_CLIENT_ONLY
        || skylineMethod == SkylineMethod.TPF_CLIENT_ONLY) {
      if (printOutput) {
        System.out.println(outputBindings.size());
      }
      NUMBER_OF_SKYLINE_CANDIDATES = outputBindings.size();
      skylineBindings = SkylineUtils.computeSkyline(outputBindings.iterator(),
          input.getSkylineAttributes(), input.getSkylinePreferenceFunctions());
    } else if (skylineMethod == SkylineMethod.SKYTPF) {
      if (printOutput) {
        System.out.println(outputBindings.size());
      }
      HybridAlgorithmImpl hybridAlgorithm = new HybridAlgorithmImpl(
          new ArrayList<BindingHashMap>(outputBindings), skylineTPs, input, nThreads, printOutput);
      skylineBindings = hybridAlgorithm.processQuery();
    }
    long end = System.currentTimeMillis();
    queryProcessingTime = end - start;
  }

  public void printBindings() {
    Iterator<BindingHashMap> outputBindingsIterator = skylineBindings.iterator();
    while (outputBindingsIterator.hasNext()) {
      BindingHashMap currentBinding = outputBindingsIterator.next();
      System.out.println(currentBinding.toString());
    }
    System.err.println("Number of skyline entities: " + skylineBindings.size());
    System.err.println("Runtime: " + Duration.ofMillis(queryProcessingTime));
  }

  public long getQueryProcessingTime() {
    return queryProcessingTime;
  }

  public ArrayList<BindingHashMap> getSkylineBindings() {
    return skylineBindings;
  }
}
