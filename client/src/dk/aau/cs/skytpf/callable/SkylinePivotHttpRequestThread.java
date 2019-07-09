package dk.aau.cs.skytpf.callable;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import org.apache.commons.codec.EncoderException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.turtle.TurtleParser;
import dk.aau.cs.skytpf.main.SkylineQueryProcessor;
import dk.aau.cs.skytpf.model.BindingHashMap;
import dk.aau.cs.skytpf.task.PivotTripleHandler;
import dk.aau.cs.skytpf.task.SkylinePivotHttpRequestTask;

public class SkylinePivotHttpRequestThread implements Callable<HashMap<Integer, BindingHashMap>> {
  private SkylinePivotHttpRequestTask skylinePivotHttpRequestTask;

  public SkylinePivotHttpRequestThread(SkylinePivotHttpRequestTask skylinePivotHttpRequestTask) {
    this.skylinePivotHttpRequestTask = skylinePivotHttpRequestTask;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.concurrent.Callable#call()
   */
  @Override
  public HashMap<Integer, BindingHashMap> call() throws ClientProtocolException, IOException,
      EncoderException, InterruptedException, ExecutionException {

    String httpUrl = skylinePivotHttpRequestTask.constructURL();
    SkylineQueryProcessor.NUMBER_OF_HTTP_REQUESTS.incrementAndGet();
    Content content =
        Request.Get(httpUrl).addHeader("accept", "text/turtle").execute().returnContent();
    if (content != null) {
      InputStream stream = content.asStream();
      PivotTripleHandler pivotTripleHandler =
          new PivotTripleHandler(httpUrl, skylinePivotHttpRequestTask.getSkylineTP(),
              skylinePivotHttpRequestTask.getSkylineAttributes());

      TurtleParser turtleParser = new TurtleParser();
      turtleParser.set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
      turtleParser.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
      turtleParser.set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);

      turtleParser.setRDFHandler(pivotTripleHandler);
      turtleParser.parse(stream, "");
      HashMap<Integer, BindingHashMap> outputBindings = pivotTripleHandler.getBindings();
      SkylineQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.addAndGet(outputBindings.size());
      return outputBindings;
    }
    return null;

  }
}
