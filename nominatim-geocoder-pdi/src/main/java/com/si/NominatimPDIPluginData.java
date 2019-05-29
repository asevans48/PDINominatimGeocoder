/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.si;

import com.mapbox.api.geocoding.v5.MapboxGeocoding;
import com.mapbox.api.geocoding.v5.models.GeocodingResponse;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;


public class NominatimPDIPluginData extends BaseStepData implements StepDataInterface {
  public RowMetaInterface outputRowMeta;
  private HttpClient client;

  /**
   * Setup the data class
   */
  public NominatimPDIPluginData() {
    super();
    client = HttpClientBuilder.create().build();
  }

  /**
   * Package teh response
   *
   * @param response      The HttpResponse
   * @return              The latLong string array
   */
  private String[] packageResponse(HttpResponse response) throws IOException, ParseException {
    String[] latLong = new String[2];
    JSONParser parser = new JSONParser();
    InputStream is = response.getEntity().getContent();
    try {
      Reader reader = new InputStreamReader(is);
      try {
        JSONObject jsonObj = (JSONObject) parser.parse(reader);
        String lat = (String) jsonObj.get("lat");
        String lon = (String) jsonObj.get("lon");
        latLong[0] = lat;
        latLong[1] = lon;
      } finally {
        reader.close();
      }
    }finally{
      is.close();
    }
    return latLong;
  }

  /**
   * Request data from nominatim
   *
   * @param uri         The uri
   * @param street      The street
   * @param city        The city
   * @param state       The state
   * @param zip         The zip
   * @return            The http response
   * @throws ClientProtocolException
   * @throws IOException
   */
  public String[] nominatimRequest(URI uri, String street, String city, String state, String zip) throws ParseException, URISyntaxException, ClientProtocolException, IOException {
    URI outputURI = new URIBuilder(uri)
            .addParameter("city", city)
            .addParameter("street", street)
            .addParameter("state", state)
            .addParameter("postalcode", zip)
            .build();
    HttpUriRequest request = new HttpGet(outputURI);
    HttpResponse response =  this.client.execute(request);
    int code = response.getStatusLine().getStatusCode();
    if(code != 200) {
      throw new IOException("Request Failed with Status Code " + code);
    }
    return this.packageResponse(response);
  }


  /**
   * Request data from mapbox
   *
   * @param uri         The uri
   * @param token       The mapbox token
   * @param street      The street
   * @param city        The city
   * @param state       The state
   * @param zip         The zip
   * @return            The http response
   * @throws ClientProtocolException
   * @throws IOException
   */
  public String[] mapBoxRequest(URI uri, String token, String street, String city, String state, String zip)
          throws InterruptedException, ParseException, URISyntaxException, ClientProtocolException, IOException {
    String addr = street;
    addr = addr + " " + city;
    addr = addr.trim() + " " + state;
    addr = addr.trim() + " " + zip;
    addr = addr.trim();

    return this.packageResponse(response);
  }
}