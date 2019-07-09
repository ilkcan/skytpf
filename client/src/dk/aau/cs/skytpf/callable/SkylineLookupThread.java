package dk.aau.cs.skytpf.callable;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import org.apache.commons.codec.EncoderException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.turtle.TurtleParser;
import dk.aau.cs.skytpf.main.SkylineQueryProcessor;
import dk.aau.cs.skytpf.model.BindingHashMap;
import dk.aau.cs.skytpf.model.TriplePattern;
import dk.aau.cs.skytpf.task.SimpleTripleHandler;
import dk.aau.cs.skytpf.task.SkylineLookupTask;
import dk.aau.cs.skytpf.util.QueryProcessingUtils;

public class SkylineLookupThread implements Callable<HashMap<Integer, BindingHashMap>> {
  private SkylineLookupTask skylineLookupTask;

  public SkylineLookupThread(SkylineLookupTask skylineLookupTask) {
    this.skylineLookupTask = skylineLookupTask;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.concurrent.Callable#call()
   */
  @Override
  public HashMap<Integer, BindingHashMap> call()
      throws ClientProtocolException, IOException, EncoderException {
    HashMap<Integer, BindingHashMap> outputBindings = new HashMap<Integer, BindingHashMap>();
    TriplePattern skylineTP = skylineLookupTask.getSkylineTP();
    String httpUrl = skylineLookupTask.constructURL();
    SkylineQueryProcessor.NUMBER_OF_HTTP_REQUESTS.incrementAndGet();
    SkylineQueryProcessor.NUMBER_OF_BINDINGS_SENT.addAndGet(skylineLookupTask.getBindings().size());
    Content content =
        Request.Get(httpUrl).addHeader("accept", "text/turtle").execute().returnContent();
    if (content != null) {
      InputStream stream = content.asStream();
      SimpleTripleHandler simpleTripleHandler = new SimpleTripleHandler(httpUrl);

      TurtleParser turtleParser = new TurtleParser();
      turtleParser.set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
      turtleParser.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
      turtleParser.set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);

      turtleParser.setRDFHandler(simpleTripleHandler);
      turtleParser.parse(stream, "");

      ArrayList<Statement> triples = simpleTripleHandler.getTriples();
      SkylineQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.addAndGet(triples.size());
      if (triples.size() == 0) {
        return new HashMap<Integer, BindingHashMap>();
      }
      ArrayList<BindingHashMap> bindings = QueryProcessingUtils
          .extendBindings(skylineLookupTask.getBindings(), skylineTP, triples, false);
      for (BindingHashMap binding : bindings) {
        outputBindings.put(binding.getHashKey(skylineLookupTask.getSkylineAttributes()), binding);
      }
    }

    return outputBindings;
  }
}
