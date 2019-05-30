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

import net.sf.saxon.functions.Parse;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;


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
        JSONArray jsonArray = (JSONArray) parser.parse(reader);
        if(jsonArray != null && jsonArray.size() > 0) {
          JSONObject jsonObj = (JSONObject) jsonArray.get(0);
          String lat = (String) jsonObj.get("lat");
          String lon = (String) jsonObj.get("lon");
          latLong[0] = lat;
          latLong[1] = lon;
        }
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
    String reason = response.getStatusLine().getReasonPhrase();
    if(code != 200){
      throw new IOException("Nominatim Request Failed with Status Code " + code + "\n" + reason);
    }
    return this.packageResponse(response);
  }

  /**
   * Packages geojson from mapbox to lat long array.
   *
   * @param response          The mapbox response
   * @return                  The lat long array
   */
  private String[] packageGeoJson(HttpResponse response) throws IOException, ParseException {
    String[] latLong = new String[2];
    JSONParser parser = new JSONParser();
    InputStream is = response.getEntity().getContent();
    try {
      Reader reader = new InputStreamReader(is);
      try {
        JSONObject fullObj = (JSONObject) parser.parse(reader);
        if(fullObj != null){
          JSONArray jarr = (JSONArray) fullObj.get("features");
          if(jarr != null && jarr.size() > 0){
            JSONObject jobj = (JSONObject) jarr.get(0);
            jarr = (JSONArray) jobj.get("coordinates");
            if(jarr.size() == 2){
              latLong[0] = String.valueOf((float)jarr.get(0));
              latLong[1] = String.valueOf((float)jarr.get(1));
            }
          }
        }
      } finally {
        reader.close();
      }
    }finally{
      is.close();
    }
    return latLong;
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
          throws IOException, ParseException, URISyntaxException, ClientProtocolException, IOException {
    String addr = street;
    addr = addr + " " + city;
    addr = addr.trim() + " " + state;
    addr = addr.trim() + " " + zip;
    addr = addr.trim();
    URI mboxUri = new URL(uri.toURL(), String.format("/geocoding/v5/mapbox.places/%s.json", addr)).toURI();
    mboxUri = new URIBuilder(mboxUri).addParameter("access_token", token).build();
    HttpUriRequest uriRequest = new HttpGet(mboxUri);
    HttpResponse response = this.client.execute(uriRequest);
    int code = response.getStatusLine().getStatusCode();
    String reason = response.getStatusLine().getReasonPhrase();
    if(code != 200){
      throw new IOException("Mapbox Request Failed with Status Code " + code + "\n" + reason);
    }
    return this.packageGeoJson(response);
  }
}