package dk.aau.cs.skytpf.task;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import dk.aau.cs.skytpf.callable.SkylineRequestThread;
import dk.aau.cs.skytpf.main.SkylineQueryProcessor;
import dk.aau.cs.skytpf.model.BindingHashMap;
import dk.aau.cs.skytpf.util.QueryProcessingUtils;

public class SkylineTripleHandler extends AbstractRDFHandler {
  private ArrayList<Statement> triples;
  private String fragmentURL;
  private ExecutorCompletionService<Boolean> executorCompletionService;
  private SkylineRequestTask skyTPFRequestTask;
  private AtomicInteger numberOfTasks;
  private HashSet<Statement> processedTriples;
  private static final int DATASET_HASH = new String("http://rdfs.org/ns/void#Dataset").hashCode();
  private static final int SUBSET_HASH = new String("http://rdfs.org/ns/void#subset").hashCode();
  private static final int HYDRA_NEXTPAGE_HASH =
      new String("http://www.w3.org/ns/hydra/core#nextPage").hashCode();

  /**
   * 
   */
  public SkylineTripleHandler(ExecutorCompletionService<Boolean> executorCompletionService,
      SkylineRequestTask skyTPFRequestTask, AtomicInteger numberOfTasks, String fragmentURL) {
    this.triples = new ArrayList<Statement>();
    this.fragmentURL = fragmentURL;
    this.processedTriples = new HashSet<Statement>();
    this.executorCompletionService = executorCompletionService;
    this.skyTPFRequestTask = skyTPFRequestTask;
    this.numberOfTasks = numberOfTasks;
  }

  private boolean isTripleValid(Statement st) {
    if (st.getSubject().stringValue().equals(fragmentURL)) {
      if (st.getPredicate().stringValue().hashCode() == HYDRA_NEXTPAGE_HASH) {
        String fragmentURL = st.getObject().stringValue();
        SkylineRequestTask newSkyTPFRequestTask = new SkylineRequestTask(
            skyTPFRequestTask.getSkylineTP(), skyTPFRequestTask.getStartingFragment(),
            skyTPFRequestTask.getBindings(), skyTPFRequestTask.getPivotBinding(),
            skyTPFRequestTask.getSkylinePrefFunc(), skyTPFRequestTask.getSkylineAttributes(),
            skyTPFRequestTask.getOutputBindings(), fragmentURL);
        numberOfTasks.incrementAndGet();
        SkylineRequestThread srt = new SkylineRequestThread(newSkyTPFRequestTask,
            executorCompletionService, numberOfTasks);
        executorCompletionService.submit(srt);
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
  public void endRDF() throws RDFHandlerException {
    extendBindings();
  }

  private void extendBindings() {
    SkylineQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.addAndGet(triples.size());
    ArrayList<BindingHashMap> bindings = skyTPFRequestTask.getBindings();
    ArrayList<BindingHashMap> extendedBindings = QueryProcessingUtils.extendBindings(bindings,
        skyTPFRequestTask.getSkylineTP(), triples, true);
    ConcurrentHashMap<Integer, BindingHashMap> outputBindings =
        skyTPFRequestTask.getOutputBindings();

    for (BindingHashMap bhm : extendedBindings) {
      int hashCode = bhm.getHashKey(skyTPFRequestTask.getSkylineAttributes());
      synchronized (outputBindings) {
        if (outputBindings.containsKey(hashCode)) {
          QueryProcessingUtils.extendBinding(outputBindings.get(hashCode), bhm);
        } else {
          outputBindings.put(hashCode, bhm);
        }
      }

    }
  }


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
      triples.add(st);
    }
  }

  public ArrayList<Statement> getTriples() {
    return triples;
  }
}
