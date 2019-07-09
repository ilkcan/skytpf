package org.linkeddatafragments.datasource.index;


import java.util.HashMap;
import java.util.Map;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.PropertyImpl;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.linkeddatafragments.brtpf.IBrTPFRequest;
import org.linkeddatafragments.datasource.AbstractBrTPFWorker;
import org.linkeddatafragments.datasource.AbstractRequestProcessor;
import org.linkeddatafragments.datasource.AbstractSkyTPFWorker;
import org.linkeddatafragments.datasource.AbstractTPFWorker;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IFragmentRequestProcessor;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragmentRequest;
import org.linkeddatafragments.skytpf.ISkyTPFRequest;

/**
 * Implementation of {@link IFragmentRequestProcessor} that processes
 * {@link ITriplePatternFragmentRequest}s over an index that provides an overview of all available
 * datasets.
 *
 * @author Miel Vander Sande
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class IndexRequestProcessor extends AbstractRequestProcessor<RDFNode, String, String> {
  final static String RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
  final static String RDFS = "http://www.w3.org/2000/01/rdf-schema#";
  final static String DC = "http://purl.org/dc/terms/";
  final static String VOID = "http://rdfs.org/ns/void#";

  protected final Model model;

  /**
   *
   * @param baseUrl
   * @param datasources
   */
  public IndexRequestProcessor(final String baseUrl,
      final HashMap<String, IDataSource> datasources) {
    this.model = ModelFactory.createDefaultModel();

    for (Map.Entry<String, IDataSource> entry : datasources.entrySet()) {
      String datasourceName = entry.getKey();
      IDataSource datasource = entry.getValue();

      Resource datasourceUrl = new ResourceImpl(baseUrl + "/" + datasourceName);

      model.add(datasourceUrl, new PropertyImpl(RDF + "type"), new ResourceImpl(VOID + "Dataset"));
      model.add(datasourceUrl, new PropertyImpl(RDFS + "label"), datasource.getTitle());
      model.add(datasourceUrl, new PropertyImpl(DC + "title"), datasource.getTitle());
      model.add(datasourceUrl, new PropertyImpl(DC + "description"), datasource.getDescription());
    }
  }

  @Override
  protected AbstractTPFWorker<RDFNode, String, String> getTPFSpecificWorker(
      ITriplePatternFragmentRequest<RDFNode, String, String> request)
      throws IllegalArgumentException {
    IndexTPFWorker worker = new IndexTPFWorker(request);
    worker.setModel(model);
    return worker;
  }

  @Override
  protected AbstractBrTPFWorker<RDFNode, String, String> getBrTPFSpecificWorker(
      IBrTPFRequest<RDFNode, String, String> request) throws IllegalArgumentException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected AbstractSkyTPFWorker<RDFNode, String, String> getSkyTPFSpecificWorker(
      ISkyTPFRequest<RDFNode, String, String> request) throws IllegalArgumentException {
    // TODO Auto-generated method stub
    return null;
  }
}
