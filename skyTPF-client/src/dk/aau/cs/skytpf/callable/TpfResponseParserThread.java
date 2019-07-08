package dk.aau.cs.skytpf.callable;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.client.fluent.Content;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.turtle.TurtleParser;
import dk.aau.cs.skytpf.task.TpfParseResponseTask;
import dk.aau.cs.skytpf.task.TpfTripleHandler;

public class TpfResponseParserThread implements Callable<Boolean> {

  private TpfParseResponseTask parseResponseTask;
  private ExecutorCompletionService<Boolean> executorCompletionService;
  private ConcurrentHashMap<String, Content> httpResponseCache;
  private AtomicInteger numberOfTasks;

  public TpfResponseParserThread(TpfParseResponseTask parseResponseTask,
      ExecutorCompletionService<Boolean> executorCompletionService,
      ConcurrentHashMap<String, Content> httpResponseCache, AtomicInteger numberOfTasks) {
    this.parseResponseTask = parseResponseTask;
    this.executorCompletionService = executorCompletionService;
    this.httpResponseCache = httpResponseCache;
    this.numberOfTasks = numberOfTasks;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.concurrent.Callable#call()
   */
  @Override
  public Boolean call() throws RDFParseException, RDFHandlerException, IOException {
    TurtleParser turtleParser = new TurtleParser();
    turtleParser.set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
    turtleParser.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
    turtleParser.set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);

    TpfTripleHandler tripleHandler = new TpfTripleHandler(executorCompletionService,
        parseResponseTask, httpResponseCache, numberOfTasks);
    turtleParser.setRDFHandler(tripleHandler);
    turtleParser.parse(parseResponseTask.getResponseStream(), "");
    return true;
  }

}
