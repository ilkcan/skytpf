package dk.aau.cs.skytpf.callable;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.turtle.TurtleParser;
import dk.aau.cs.skytpf.main.SkylineQueryProcessor;
import dk.aau.cs.skytpf.task.SkylineRequestTask;
import dk.aau.cs.skytpf.task.SkylineTripleHandler;

public class SkylineRequestThread implements Callable<Boolean> {
  private SkylineRequestTask skyTPFRequestTask;
  private ExecutorCompletionService<Boolean> executorCompletionService;
  private AtomicInteger numberOfTasks;

  public SkylineRequestThread(SkylineRequestTask skyTPFRequestTask,
      ExecutorCompletionService<Boolean> executorCompletionService, AtomicInteger numberOfTasks) {
    this.skyTPFRequestTask = skyTPFRequestTask;
    this.executorCompletionService = executorCompletionService;
    this.numberOfTasks = numberOfTasks;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.concurrent.Callable#call()
   */
  @Override
  public Boolean call() throws ClientProtocolException, IOException {
    String httpUrl = skyTPFRequestTask.getFragmentURL();
    SkylineQueryProcessor.NUMBER_OF_HTTP_REQUESTS.incrementAndGet();
    SkylineQueryProcessor.NUMBER_OF_BINDINGS_SENT
        .addAndGet(skyTPFRequestTask.getBindings().size() + 1);
    // System.out.println(httpUrl);
    Content content =
        Request.Get(httpUrl).addHeader("accept", "text/turtle").execute().returnContent();

    if (content != null) {
      TurtleParser turtleParser = new TurtleParser();
      turtleParser.set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
      turtleParser.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
      turtleParser.set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);
      SkylineTripleHandler skylineTripleHandler =
          new SkylineTripleHandler(executorCompletionService, skyTPFRequestTask, numberOfTasks,
              skyTPFRequestTask.getFragmentURL());
      turtleParser.setRDFHandler(skylineTripleHandler);
      turtleParser.parse(content.asStream(), "");
    }
    return true;
  }
}
