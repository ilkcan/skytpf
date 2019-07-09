package dk.aau.cs.skytpf.callable;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.turtle.TurtleParser;
import dk.aau.cs.skytpf.main.SkylineQueryProcessor;
import dk.aau.cs.skytpf.task.CountMetadataHandler;
import dk.aau.cs.skytpf.task.InitialHttpRequestTask;

public class InitialHttpRequestThread implements Callable<Integer> {
  private InitialHttpRequestTask httpRequestTask;
  private ConcurrentHashMap<String, Content> httpResponseCache;

  public InitialHttpRequestThread(InitialHttpRequestTask httpRequestTask,
      ConcurrentHashMap<String, Content> httpResponseCache) {
    this.httpRequestTask = httpRequestTask;
    this.httpResponseCache = httpResponseCache;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.concurrent.Callable#call()
   */
  @Override
  public Integer call() throws Exception {
    int triplesCount = 0;
    try {
      String httpUrl = httpRequestTask.getFragmentURL();
      Content content = null;
      boolean cacheContains = false;
      if (httpResponseCache.containsKey(httpUrl)) {
        cacheContains = true;
        content = httpResponseCache.get(httpUrl);
      } else {
        SkylineQueryProcessor.NUMBER_OF_HTTP_REQUESTS.incrementAndGet();
        content = Request.Get(httpUrl).addHeader("accept", "text/turtle").execute().returnContent();
      }
      InputStream stream = content.asStream();
      CountMetadataHandler countMetadataHandler = new CountMetadataHandler(stream);

      TurtleParser turtleParser = new TurtleParser();
      turtleParser.set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
      turtleParser.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
      turtleParser.set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);

      turtleParser.setRDFHandler(countMetadataHandler);
      turtleParser.parse(stream, "");
      if (!cacheContains) {
        httpResponseCache.put(httpUrl, content);
      }
      triplesCount = countMetadataHandler.getTriplesCount();
    } catch (ClientProtocolException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return triplesCount;
  }
}
