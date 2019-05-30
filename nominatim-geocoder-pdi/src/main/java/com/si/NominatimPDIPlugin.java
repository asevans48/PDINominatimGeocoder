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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Describe your step plugin.
 * 
 */
public class NominatimPDIPlugin extends BaseStep implements StepInterface {
  private NominatimPDIPluginMeta meta;
  private NominatimPDIPluginData data;
  
  private static Class<?> PKG = NominatimPDIPluginMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  
  public NominatimPDIPlugin( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }
  
  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param stepMetaInterface
   *          The metadata to work with
   * @param stepDataInterface
   *          The data to initialize
   */
  public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    meta = (NominatimPDIPluginMeta) meta;
    data = (NominatimPDIPluginData) data;
    return super.init( stepMetaInterface, stepDataInterface );
  }


  private String[] gecodeMapBox(String address, String city, String state, String zip){
    String[] latLong = new String[2];
    if(meta.getMapboxUrl() != null && meta.getMapboxUrl().trim().length() > 0) {
      try {
        URI uri = new URI(meta.getMapboxUrl());
        data.mapBoxRequest(uri, meta.getMapBoxKey(), address, city, state, zip);
      }catch(URISyntaxException e){
        if(isBasic()){
          logBasic("Failed to Parse Mapbox URL");
          e.printStackTrace();
        }
      }catch(Exception e){
        if(isBasic()){
          logBasic("Failed to Obtain mapbox geocode data");
        }
      }
    }
    return latLong;
  }


  /**
   * Geocode from nominatim
   * @param address         The address
   * @param city            The city
   * @param state           The state
   * @param zip             The postal code
   * @return                An array containing the latitude and longitude
   */
  private String[] geocodeNominatim(String address, String city, String state, String zip){
    String[] latLong = null;
    String url = meta.getNominatimUrl();
    if(url !=null && url.trim().length() > 0){
      url = url.trim();
      try {
        URI uri = new URI(url);
        latLong = data.nominatimRequest(uri, address, city, state, zip);
      }catch(Exception e){
        if(isBasic()){
          logBasic("Failed to Geocode Address");
          logBasic(e.getMessage());
          e.printStackTrace();
        }
      }
    }else{
      if(isBasic()){
        logBasic("No Nominatim URL Provided in Geocoder");
      }
    }
    return latLong;
  }

  /**
   * Extract the field.
   *
   * @param fieldName     The field name to extract
   * @param rmi           The row meta interface
   * @return              The extracted value primitive
   */
  private Object extractField(Object[] r, String fieldName, RowMetaInterface rmi){
    if(fieldName != null && fieldName.trim().length() > 0) {
      int idx = rmi.indexOfValue(fieldName);
      if (idx > -1) {
        return r[idx];
      }
    }else if(fieldName == null){
      if(isBasic()){
        logBasic("Field " + fieldName + " Not Provided in Geocoding");
      }
    }
    return null;
  }


  /**
   * Check if the address was obtained
   * @param r           The row to check
   * @param rmi         The row meta interface
   * @return            Whether the address was parsed
   */
  private boolean wasGeocodeObtained(Object[] r, RowMetaInterface rmi){
    boolean found = false;
    String latField = meta.getLatitudeField();
    String lonField = meta.getLongitudeField();
    int idx = rmi.indexOfValue(latField);

    if(idx > -1) {
      if(r[idx] != null){
        idx = rmi.indexOfValue(lonField);
        if(r[idx] == null) {
          found = true;
        }
      }
    }

    return found;
  }

  /**
   * Resize the row
   *
   * @param r     The row
   * @return      The object row representation
   */
  private Object[] resizeRow(Object[] r){
    Object[] orow = r.clone();
    if(r.length < data.outputRowMeta.size()){
      RowDataUtil.resizeArray(orow, data.outputRowMeta.size());
    }
    return orow;
  }

  /**
   * Package the row after obtaining lat and long values
   *
   * @param latLong       The latitude and longitude values
   * @param r             The row to package
   * @param rmi           The row meta interface
   * @return              The updated row
   */
  private Object[] packageRow(String[] latLong, Object[] r, RowMetaInterface rmi){
    int idx = rmi.indexOfValue(meta.getLatitudeField());
    if(idx > -1){
      String lat = latLong[0];
      r[idx] = lat;
      idx = rmi.indexOfValue(meta.getLongitudeField());
      if(idx > -1) {
        String lon = latLong[1];
        r[idx] = lon;
      }else{
        if(isBasic()){
          logBasic("Longitude Field Not Provided for Geocoder");
        }
      }
    }else{
      if(isBasic()){
        logBasic("Latitude Field Not Provided for Geocoder");
      }
    }
    return r;
  }

  /**
   * Wait for a specified time to slow down request rate.
   *
   * @param waitTime        The wait time long
   */
  private void doWait(long waitTime){
    try {
      Thread.sleep(waitTime);
    }catch(InterruptedException e){
      if(isBasic()){
        logBasic(String.format("Failed to Wait for %d seconds in Geocoder", waitTime));
        e.printStackTrace();
      }
    }
  }

  /**
   * Geocode the address in a row
   *
   * @param inrow       The input row
   * @param rmi         The row meta interface
   * @return            The updated row
   */
  private Object[] getLatLong(Object[] inrow, RowMetaInterface rmi){
    Object[] outrow = this.resizeRow(inrow);
    boolean fallThrough = meta.isUseMapBoxFallbackIfPresent();
    String cityO = (String) this.extractField(outrow, meta.getCityField(), rmi);
    String streetO = (String) this.extractField(outrow, meta.getStreetField(), rmi);
    String stateO = (String) this.extractField(outrow, meta.getStateField(), rmi);
    Object zipO = this.extractField(outrow, meta.getZipField(), rmi);
    if(cityO != null && streetO != null && stateO != null && zipO != null) {
      if(zipO != null && !(zipO instanceof String)){
        zipO = String.valueOf(zipO);
      }else if(zipO == null){
        zipO = "";
      }
      String zip = (String) zipO;

      if (meta.getNominatimUrl() != null) {
        String[] latLong = this.geocodeNominatim(streetO, cityO, stateO, zip);
        if(meta.getPostNominatimWaitMillis() > 0L){
          this.doWait(meta.getPostNominatimWaitMillis());
        }
        if(latLong != null && latLong.length == 2 && (latLong[0] != null || latLong[1] != null)){
          outrow = this.packageRow(latLong, outrow, rmi);
        }else{
          fallThrough = false;
        }
      }

      if (fallThrough && !this.wasGeocodeObtained(outrow, rmi)) {
        String[] latLong = this.gecodeMapBox(streetO, cityO, stateO, zip);
        if(meta.getPostMapboxWaitMillis() > 0L){
          this.doWait(meta.getPostMapboxWaitMillis());
        }
        if(latLong != null && latLong.length == 2 && (latLong[0] != null || latLong[1] != null)) {
          outrow = this.packageRow(latLong, outrow, rmi);
        }
      }
    }

    return outrow;
  }

  /**
   * Setup the processor.
   *
   * @throws KettleException
   */
  private void setupProcessor() throws KettleException{
    RowMetaInterface inMeta = getInputRowMeta().clone();
    data.outputRowMeta = inMeta;
    meta.getFields(data.outputRowMeta, getStepname(), null, null, this, null, null);
    first = false;
  }


  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) {
      // no more input to be expected...
      setOutputDone();
      return false;
    }

    if(first){
      this.setupProcessor();
    }

    r = this.getLatLong(r, getInputRowMeta());
    putRow(data.outputRowMeta, r);

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() )
        logBasic( BaseMessages.getString( PKG, "NominatimPDIPlugin.Log.LineNumber" ) + getLinesRead() );
    }
      
    return true;
  }
}