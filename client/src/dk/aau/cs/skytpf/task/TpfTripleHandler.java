package dk.aau.cs.skytpf.task;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.client.fluent.Content;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import dk.aau.cs.skytpf.callable.TpfHttpRequestThread;
import dk.aau.cs.skytpf.main.SkylineQueryProcessor;
import dk.aau.cs.skytpf.model.BindingHashMap;
import dk.aau.cs.skytpf.model.TriplePattern;
import dk.aau.cs.skytpf.util.QueryProcessingUtils;

public class TpfTripleHandler extends AbstractRDFHandler {
  private ExecutorCompletionService<Boolean> executorCompletionService;
  private TpfParseResponseTask parseResponseTask;
  private ConcurrentHashMap<String, Content> httpResponseCache;
  private AtomicInteger numberOfTasks;
  private HashSet<Statement> processedTriples;
  private static final int HYDRA_NEXTPAGE_HASH =
      new String("http://www.w3.org/ns/hydra/core#nextPage").hashCode();
  private static final int DATASET_HASH = new String("http://rdfs.org/ns/void#Dataset").hashCode();
  private static final int SUBSET_HASH = new String("http://rdfs.org/ns/void#subset").hashCode();

  /**
   * 
   */
  public TpfTripleHandler(ExecutorCompletionService<Boolean> executorCompletionService,
      TpfParseResponseTask parseResponseTask, ConcurrentHashMap<String, Content> httpResponseCache,
      AtomicInteger numberOfTasks) {
    this.executorCompletionService = executorCompletionService;
    this.parseResponseTask = parseResponseTask;
    this.httpResponseCache = httpResponseCache;
    this.numberOfTasks = numberOfTasks;
    this.processedTriples = new HashSet<Statement>();
  }

  private boolean isTripleValid(Statement st) {
    if (st.getSubject().stringValue()
        .equals(parseResponseTask.getHttpRequestTask().getFragmentURL())) {
      if (st.getPredicate().stringValue().hashCode() == HYDRA_NEXTPAGE_HASH) {
        String fragmentURL = st.getObject().stringValue();
        TpfHttpRequestTask currHttpRequestTask = parseResponseTask.getHttpRequestTask();
        ArrayList<TriplePattern> tpOrder = currHttpRequestTask.getTpOrder();
        TpfHttpRequestTask httpRequestTask = new TpfHttpRequestTask(tpOrder,
            currHttpRequestTask.getBinding(), currHttpRequestTask.getTpIdx(), fragmentURL,
            currHttpRequestTask.getOutputBindings());
        httpRequestTask.setStartingFragment(currHttpRequestTask.getStartingFragment());
        numberOfTasks.incrementAndGet();
        TpfHttpRequestThread hrt = new TpfHttpRequestThread(httpRequestTask, httpResponseCache,
            executorCompletionService, numberOfTasks);
        executorCompletionService.submit(hrt);
      }
      return false;
    } else if (st.getPredicate().stringValue().contains("hydra/")
        || st.getObject().stringValue().contains("hydra/")
        || st.getObject().stringValue().hashCode() == DATASET_HASH
        || st.getPredicate().stringValue().hashCode() == SUBSET_HASH) {
      return false;
    } else {
      return true;
    }

  }

  @Override
  public void endRDF() throws RDFHandlerException {}

  /*
   * (non-Javadoc)
   * 
   * @see org.openrdf.rio.helpers.AbstractRDFHandler#handleStatement(org.openrdf.model.Statement)
   */
  @Override
  public void handleStatement(Statement st) throws RDFHandlerException {
    if (processedTriples.contains(st)) {
      return;
    } else {
      processedTriples.add(st);
    }
    if (isTripleValid(st)) {
      SkylineQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.addAndGet(1);
      sendRequestWithTriple(st);
    }
  }


  private void sendRequestWithTriple(Statement st) {
    TpfHttpRequestTask currHttpRequestTask = parseResponseTask.getHttpRequestTask();
    BindingHashMap extendedBinding = QueryProcessingUtils.extendBindingWithSingleTriple(
        currHttpRequestTask.getBinding(), currHttpRequestTask.getTriplePattern(), st);
    if (extendedBinding != null) {
      ConcurrentLinkedQueue<BindingHashMap> outputBindings =
          currHttpRequestTask.getOutputBindings();
      ArrayList<TriplePattern> tpOrder = currHttpRequestTask.getTpOrder();
      int noOfTPs = tpOrder.size();
      int tpIdx = currHttpRequestTask.getTpIdx();
      if (tpIdx == noOfTPs - 1) {
        outputBindings.add(extendedBinding);
      } else {
        TpfHttpRequestTask httpRequestTask = new TpfHttpRequestTask(tpOrder,
            currHttpRequestTask.getStartingFragment(), extendedBinding, tpIdx + 1, outputBindings);
        numberOfTasks.incrementAndGet();
        TpfHttpRequestThread hrt = new TpfHttpRequestThread(httpRequestTask, httpResponseCache,
            executorCompletionService, numberOfTasks);
        executorCompletionService.submit(hrt);
      }
    }
  }
}
