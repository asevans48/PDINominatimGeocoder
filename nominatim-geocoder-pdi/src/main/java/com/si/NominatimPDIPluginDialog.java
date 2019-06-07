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

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class NominatimPDIPluginDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = NominatimPDIPluginMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private static final int MARGIN_SIZE = 15;
  private static final int LABEL_SPACING = 5;
  private static final int ELEMENT_SPACING = 10;

  private static final int LARGE_FIELD = 350;
  private static final int MEDIUM_FIELD = 250;
  private static final int SMALL_FIELD = 75;

  private NominatimPDIPluginMeta meta;

  private Label wlStepname;
  private Text wStepname;
  private FormData fdStepname, fdlStepname;

  private Label streetName;
  private CCombo wStreetCombo;
  private FormData fdlStreetName, fdlStreet;

  private Label stateName;
  private CCombo wStateCombo;
  private FormData fdlStateName, fdlState;

  private Label cityName;
  private CCombo wCityCombo;
  private FormData fdlCityName, fdlCity;

  private Label zipName;
  private CCombo wZipCombo;
  private FormData fdlZipName, fdlZip;

  private Label nominatimName;
  private TextVar wNominatimField;
  private FormData fdlNominatimName, fdlNominatimField;

  private Label mapBoxName;
  private TextVar wMapBoxField;
  private FormData fdlMapBoxName, fdlMapBoxField;

  private Label latitudeName;
  private TextVar wLatitudeField;
  private FormData fdlLatitudeName, fdlLatitudeField;

  private Label longitudeName;
  private TextVar wLongitudeField;
  private FormData fdlLongitudeName, fdlLongitudeField;

  private Label mapBoxKeyName;
  private TextVar wMapBoxKeyField;
  private FormData fdlMapBoxKeyName, fdlMapBoxKeyField;

  private Label mapBoxWaitName;
  private TextVar wMapBoxWaitField;
  private FormData fdlMapBoxWaitName, fdlMapBoxWaitField;

  private Label nominatimWaitName;
  private TextVar wNominatimWaitField;
  private FormData fdlNominatimWaitName, fdlNominatimWaitField;

  private Label useMboxName;
  private Button wUseMbox;
  private FormData fdlUseMboxName, fdlUseMbox;

  private Button wCancel;
  private Button wOK;
  private ModifyListener lsMod;
  private Listener lsCancel;
  private Listener lsOK;
  private SelectionAdapter lsDef;
  private boolean changed;

  public NominatimPDIPluginDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    meta = (NominatimPDIPluginMeta) in;
  }

  public String open() {
    // store some convenient SWT variables
    Shell parent = getParent();
    Display display = parent.getDisplay();

    // SWT code for preparing the dialog
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, meta);

    // Save the value of the changed flag on the meta object. If the user cancels
    // the dialog, it will be restored to this saved value.
    // The "changed" variable is inherited from BaseStepDialog
    changed = meta.hasChanged();

    // The ModifyListener used on all controls. It will update the meta object to
    // indicate that changes are being made.
    ModifyListener lsMod = new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        meta.setChanged();
      }
    };

    // ------------------------------------------------------- //
    // SWT code for building the actual settings dialog        //
    // ------------------------------------------------------- //
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Shell.Title"));
    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label(shell, SWT.RIGHT);
    wlStepname.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Stepname.Label"));
    props.setLook(wlStepname);
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment(0, 0);
    fdlStepname.right = new FormAttachment(middle, -margin);
    fdlStepname.top = new FormAttachment(0, margin);
    wlStepname.setLayoutData(fdlStepname);

    wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wStepname.setText(stepname);
    props.setLook(wStepname);
    wStepname.addModifyListener(lsMod);
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment(middle, 0);
    fdStepname.top = new FormAttachment(0, margin);
    fdStepname.right = new FormAttachment(100, 0);
    wStepname.setLayoutData(fdStepname);

    // Set the street
    streetName = new Label( shell, SWT.RIGHT );
    streetName.setText( BaseMessages.getString( PKG, "NominatimPDIPluginDialog.Fields.Street" ) );
    props.setLook( streetName );
    fdlStreetName = new FormData();
    fdlStreetName.left = new FormAttachment( 0, 0 );
    fdlStreetName.right = new FormAttachment( middle, -margin );
    fdlStreetName.top = new FormAttachment( wStepname, 15 );
    streetName.setLayoutData( fdlStreetName );

    wStreetCombo = new CCombo( shell, SWT.BORDER );
    props.setLook( wStreetCombo );
    StepMeta stepinfo = transMeta.findStep( stepname );
    if ( stepinfo != null ) {
      try {
        String[] fields = transMeta.getStepFields(stepname).getFieldNames();
        for (int i = 0; i < fields.length; i++) {
          wStreetCombo.add(fields[i]);
        }
      }catch(KettleException e){
        if ( log.isBasic())
          logBasic("Failed to Get Step Fields");
      }
    }

    wStreetCombo.addModifyListener( lsMod );
    fdlStreet = new FormData();
    fdlStreet.left = new FormAttachment( middle, 0 );
    fdlStreet.top = new FormAttachment( wStepname, 15 );
    fdlStreet.right = new FormAttachment( 100, 0 );
    wStreetCombo.setLayoutData( fdlStreet );

    //city
    cityName = new Label( shell, SWT.RIGHT );
    cityName.setText( BaseMessages.getString( PKG, "NominatimPDIPluginDialog.Fields.City" ) );
    props.setLook( cityName );
    fdlCityName = new FormData();
    fdlCityName.left = new FormAttachment( 0, 0 );
    fdlCityName.right = new FormAttachment( middle, -margin );
    fdlCityName.top = new FormAttachment( wStreetCombo, 15 );
    cityName.setLayoutData( fdlCityName );

    wCityCombo = new CCombo( shell, SWT.BORDER );
    props.setLook( wCityCombo );
    if ( stepinfo != null ) {
      try {
        String[] fields = transMeta.getStepFields(stepname).getFieldNames();
        for (int i = 0; i < fields.length; i++) {
          wCityCombo.add(fields[i]);
        }
      }catch(KettleException e){
        if ( log.isBasic())
          logBasic("Failed to Get Step Fields");
      }
    }

    wCityCombo.addModifyListener( lsMod );
    fdlCity = new FormData();
    fdlCity.left = new FormAttachment( middle, 0 );
    fdlCity.top = new FormAttachment( wStreetCombo, 15 );
    fdlCity.right = new FormAttachment( 100, 0 );
    wCityCombo.setLayoutData( fdlCity );

    //state
    stateName = new Label( shell, SWT.RIGHT );
    stateName.setText( BaseMessages.getString( PKG, "NominatimPDIPluginDialog.Fields.State" ) );
    props.setLook( stateName );
    fdlStateName = new FormData();
    fdlStateName.left = new FormAttachment( 0, 0 );
    fdlStateName.right = new FormAttachment( middle, -margin );
    fdlStateName.top = new FormAttachment( wCityCombo, 15 );
    stateName.setLayoutData( fdlStateName );

    wStateCombo = new CCombo( shell, SWT.BORDER );
    props.setLook( wStateCombo );
    if ( stepinfo != null ) {
      try {
        String[] fields = transMeta.getStepFields(stepname).getFieldNames();
        for (int i = 0; i < fields.length; i++) {
          wStateCombo.add(fields[i]);
        }
      }catch(KettleException e){
        if ( log.isBasic())
          logBasic("Failed to Get Step Fields");
      }
    }

    wStateCombo.addModifyListener( lsMod );
    fdlState = new FormData();
    fdlState.left = new FormAttachment( middle, 0 );
    fdlState.top = new FormAttachment( wCityCombo, 15 );
    fdlState.right = new FormAttachment( 100, 0 );
    wStateCombo.setLayoutData( fdlState );

    //zip
    zipName = new Label( shell, SWT.RIGHT );
    zipName.setText( BaseMessages.getString( PKG, "NominatimPDIPluginDialog.Fields.Zip" ) );
    props.setLook( zipName );
    fdlZipName = new FormData();
    fdlZipName.left = new FormAttachment( 0, 0 );
    fdlZipName.right = new FormAttachment( middle, -margin );
    fdlZipName.top = new FormAttachment( wStateCombo, 15 );
    zipName.setLayoutData( fdlZipName );

    wZipCombo = new CCombo( shell, SWT.BORDER );
    props.setLook( wZipCombo );
    if ( stepinfo != null ) {
      try {
        String[] fields = transMeta.getStepFields(stepname).getFieldNames();
        for (int i = 0; i < fields.length; i++) {
          wZipCombo.add(fields[i]);
        }
      }catch(KettleException e){
        if ( log.isBasic())
          logBasic("Failed to Get Step Fields");
      }
    }

    wZipCombo.addModifyListener( lsMod );
    fdlZip = new FormData();
    fdlZip.left = new FormAttachment( middle, 0 );
    fdlZip.top = new FormAttachment( wStateCombo, 15 );
    fdlZip.right = new FormAttachment( 100, 0 );
    wZipCombo.setLayoutData( fdlZip );

    //latitude
    latitudeName = new Label(shell, SWT.RIGHT);
    latitudeName.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Out.Latitude"));
    props.setLook(latitudeName);
    fdlLatitudeName = new FormData();
    fdlLatitudeName.left = new FormAttachment(0, 0);
    fdlLatitudeName.top = new FormAttachment(wZipCombo, 15);
    fdlLatitudeName.right = new FormAttachment(middle, -margin);
    latitudeName.setLayoutData(fdlLatitudeName);
    wLatitudeField = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wLatitudeField.setText("");
    wLatitudeField.addModifyListener(lsMod);
    props.setLook(wLatitudeField);
    fdlLatitudeField = new FormData();
    fdlLatitudeField.left = new FormAttachment(middle, 0);
    fdlLatitudeField.top = new FormAttachment(wZipCombo, 15);
    fdlLatitudeField.right = new FormAttachment(100, 0);
    wLatitudeField.setLayoutData(fdlLatitudeField);

    //longitude
    longitudeName = new Label(shell, SWT.RIGHT);
    longitudeName.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Out.Longitude"));
    props.setLook(longitudeName);
    fdlLongitudeName = new FormData();
    fdlLongitudeName.left = new FormAttachment(0, 0);
    fdlLongitudeName.top = new FormAttachment(latitudeName, 15);
    fdlLongitudeName.right = new FormAttachment(middle, -margin);
    longitudeName.setLayoutData(fdlLongitudeName);
    wLongitudeField = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wLongitudeField.setText("");
    wLongitudeField.addModifyListener(lsMod);
    props.setLook(wLongitudeField);
    fdlLongitudeField = new FormData();
    fdlLongitudeField.left = new FormAttachment(middle, 0);
    fdlLongitudeField.top = new FormAttachment(latitudeName, 15);
    fdlLongitudeField.right = new FormAttachment(100, 0);
    wLongitudeField.setLayoutData(fdlLongitudeField);

    //nominatim url
    nominatimName = new Label(shell, SWT.RIGHT);
    nominatimName.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Config.Nominatim"));
    props.setLook(nominatimName);
    fdlNominatimName = new FormData();
    fdlNominatimName.left = new FormAttachment(0, 0);
    fdlNominatimName.top = new FormAttachment(longitudeName, 15);
    fdlNominatimName.right = new FormAttachment(middle, -margin);
    nominatimName.setLayoutData(fdlNominatimName);
    wNominatimField = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wNominatimField.setText("");
    wNominatimField.addModifyListener(lsMod);
    props.setLook(wNominatimField);
    fdlNominatimField = new FormData();
    fdlNominatimField.left = new FormAttachment(middle, 0);
    fdlNominatimField.top = new FormAttachment(longitudeName, 15);
    fdlNominatimField.right = new FormAttachment(100, 0);
    wNominatimField.setLayoutData(fdlNominatimField);

    //nominatim wait
    nominatimWaitName = new Label(shell, SWT.RIGHT);
    nominatimWaitName.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Config.NominatimWait"));
    props.setLook(nominatimWaitName);
    fdlNominatimWaitName = new FormData();
    fdlNominatimWaitName.left = new FormAttachment(0, 0);
    fdlNominatimWaitName.top = new FormAttachment(nominatimName, 15);
    fdlNominatimWaitName.right = new FormAttachment(middle, -margin);
    nominatimWaitName.setLayoutData(fdlNominatimWaitName);
    wNominatimWaitField = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wNominatimWaitField.setText("");
    wNominatimWaitField.addModifyListener(lsMod);
    props.setLook(wNominatimWaitField);
    fdlNominatimWaitField = new FormData();
    fdlNominatimWaitField.left = new FormAttachment(middle, 0);
    fdlNominatimWaitField.top = new FormAttachment(nominatimName, 15);
    fdlNominatimWaitField.right = new FormAttachment(100, 0);
    wNominatimWaitField.setLayoutData(fdlNominatimWaitField);

    //mapboxurl
    mapBoxName = new Label(shell, SWT.RIGHT);
    mapBoxName.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Config.MapBox"));
    props.setLook(mapBoxName);
    fdlMapBoxName = new FormData();
    fdlMapBoxName.left = new FormAttachment(0, 0);
    fdlMapBoxName.top = new FormAttachment(nominatimWaitName, 15);
    fdlMapBoxName.right = new FormAttachment(middle, -margin);
    mapBoxName.setLayoutData(fdlMapBoxName);
    wMapBoxField = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wMapBoxField.setText("");
    wMapBoxField.addModifyListener(lsMod);
    props.setLook(wMapBoxField);
    fdlMapBoxField = new FormData();
    fdlMapBoxField.left = new FormAttachment(middle, 0);
    fdlMapBoxField.top = new FormAttachment(nominatimWaitName, 15);
    fdlMapBoxField.right = new FormAttachment(100, 0);
    wMapBoxField.setLayoutData(fdlMapBoxField);

    //mapboxkey
    mapBoxKeyName = new Label(shell, SWT.RIGHT);
    mapBoxKeyName.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Config.MapBoxKey"));
    props.setLook(mapBoxKeyName);
    fdlMapBoxKeyName = new FormData();
    fdlMapBoxKeyName.left = new FormAttachment(0, 0);
    fdlMapBoxKeyName.top = new FormAttachment(mapBoxName, 15);
    fdlMapBoxKeyName.right = new FormAttachment(middle, -margin);
    mapBoxKeyName.setLayoutData(fdlMapBoxKeyName);
    wMapBoxKeyField = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wMapBoxKeyField.setText("");
    wMapBoxKeyField.addModifyListener(lsMod);
    props.setLook(wMapBoxKeyField);
    fdlMapBoxKeyField = new FormData();
    fdlMapBoxKeyField.left = new FormAttachment(middle, 0);
    fdlMapBoxKeyField.top = new FormAttachment(mapBoxName, 15);
    fdlMapBoxKeyField.right = new FormAttachment(100, 0);
    wMapBoxKeyField.setLayoutData(fdlMapBoxKeyField);

    //mapboxwait
    mapBoxWaitName = new Label(shell, SWT.RIGHT);
    mapBoxWaitName.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Config.MapBoxWait"));
    props.setLook(mapBoxWaitName);
    fdlMapBoxWaitName = new FormData();
    fdlMapBoxWaitName.left = new FormAttachment(0, 0);
    fdlMapBoxWaitName.top = new FormAttachment(mapBoxKeyName, 15);
    fdlMapBoxWaitName.right = new FormAttachment(middle, -margin);
    mapBoxWaitName.setLayoutData(fdlMapBoxWaitName);
    wMapBoxWaitField = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wMapBoxWaitField.setText("");
    wMapBoxWaitField.addModifyListener(lsMod);
    props.setLook(wMapBoxWaitField);
    fdlMapBoxWaitField = new FormData();
    fdlMapBoxWaitField.left = new FormAttachment(middle, 0);
    fdlMapBoxWaitField.top = new FormAttachment(mapBoxKeyName, 15);
    fdlMapBoxWaitField.right = new FormAttachment(100, 0);
    wMapBoxWaitField.setLayoutData(fdlMapBoxWaitField);

    //use mapbox fallback
    useMboxName = new Label(shell,SWT.RIGHT);
    useMboxName.setText(BaseMessages.getString(PKG,"NominatimPDIPluginDialog.Config.UseMapBox"));
    props.setLook(useMboxName);
    fdlUseMboxName = new FormData();
    fdlUseMboxName.left = new FormAttachment(0, 0);
    fdlUseMboxName.top = new FormAttachment(mapBoxWaitName, 15);
    fdlUseMboxName.right = new FormAttachment(middle, -margin);
    useMboxName.setLayoutData(fdlUseMboxName);
    wUseMbox = new Button(shell, SWT.CHECK);
    props.setLook(wUseMbox);
    fdlUseMbox = new FormData();
    fdlUseMbox.left = new FormAttachment(middle, 0);
    fdlUseMbox.top = new FormAttachment(mapBoxWaitName, 15);
    fdlUseMbox.right = new FormAttachment(100, 0);
    wUseMbox.setLayoutData(fdlUseMbox);


    // OK and cancel buttons
    wOK = new Button(shell, SWT.PUSH);
    wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    setButtonPositions(new Button[]{wOK, wCancel}, margin, useMboxName);

    // Add listeners for cancel and OK
    lsCancel = new Listener() {
      public void handleEvent(Event e) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent(Event e) {
        ok();
      }
    };
    wCancel.addListener(SWT.Selection, lsCancel);
    wOK.addListener(SWT.Selection, lsOK);

    // default listener (for hitting "enter")
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };
    wStepname.addSelectionListener(lsDef);
    wCityCombo.addSelectionListener(lsDef);
    wLatitudeField.addSelectionListener(lsDef);
    wLongitudeField.addSelectionListener(lsDef);
    wMapBoxField.addSelectionListener(lsDef);
    wMapBoxKeyField.addSelectionListener(lsDef);
    wMapBoxWaitField.addSelectionListener(lsDef);
    wNominatimField.addSelectionListener(lsDef);
    wNominatimWaitField.addSelectionListener(lsDef);
    wStateCombo.addSelectionListener(lsDef);
    wStreetCombo.addSelectionListener(lsDef);
    wUseMbox.addSelectionListener(lsDef);
    wZipCombo.addSelectionListener(lsDef);



    // Detect X or ALT-F4 or something that kills this window and cancel the dialog properly
    shell.addShellListener(new ShellAdapter() {
      public void shellClosed(ShellEvent e) {
        cancel();
      }
    });

    // Set/Restore the dialog size based on last position on screen
    // The setSize() method is inherited from BaseStepDialog
    setSize();

    // populate the dialog with the values from the meta object
    getData();

    // restore the changed flag to original value, as the modify listeners fire during dialog population
    meta.setChanged(changed);

    // open dialog and enter event loop
    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    // at this point the dialog has closed, so either ok() or cancel() have been executed
    // The "stepname" variable is inherited from BaseStepDialog
    return stepname;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wStepname.selectAll();
    wCityCombo.setText(Const.NVL(meta.getCityField(), ""));
    wLatitudeField.setText(Const.NVL(meta.getLatitudeField(),""));
    wLongitudeField.setText(Const.NVL(meta.getLongitudeField(),""));
    wMapBoxField.setText(Const.NVL(meta.getMapboxUrl(), ""));
    wMapBoxKeyField.setText(Const.NVL(meta.getMapBoxKey(), ""));
    wMapBoxWaitField.setText(String.valueOf(meta.getPostMapboxWaitMillis()));
    wNominatimField.setText(Const.NVL(meta.getNominatimUrl(), ""));
    wNominatimWaitField.setText(String.valueOf(meta.getPostNominatimWaitMillis()));
    wStateCombo.setText(Const.NVL(meta.getStateField(), ""));
    wStreetCombo.setText(Const.NVL(meta.getStreetField(), ""));
    wUseMbox.setSelection(meta.isUseMapBoxFallbackIfPresent());
    wZipCombo.setText(Const.NVL(meta.getZipField(), ""));
    wStepname.setFocus();
  }

  private Image getImage() {
    PluginInterface plugin =
        PluginRegistry.getInstance().getPlugin( StepPluginType.class, stepMeta.getStepMetaInterface() );
    String id = plugin.getIds()[0];
    if ( id != null ) {
      return GUIResource.getInstance().getImagesSteps().get( id ).getAsBitmapForSize( shell.getDisplay(),
          ConstUI.ICON_SIZE, ConstUI.ICON_SIZE );
    }
    return null;
  }

  private void cancel() {
    dispose();
  }

  private void ok() {
    stepname = wStepname.getText();
    String city = wCityCombo.getText();
    String latField = wLatitudeField.getText();
    String longField = wLongitudeField.getText();
    String mbField = wMapBoxField.getText();
    String mbKeyField = wMapBoxKeyField.getText();
    String mbWaitField = wMapBoxWaitField.getText();
    String nomField = wNominatimField.getText();
    String nomWaitField = wNominatimWaitField.getText();
    String stateField = wStateCombo.getText();
    String streetField = wStreetCombo.getText();
    String zipField = wZipCombo.getText();
    boolean useMbox = wUseMbox.getSelection();

    if(mbWaitField == null || mbWaitField.trim().length() == 0){
      mbWaitField = "0";
    }

    if(nomField == null || nomField.trim().length() == 0){
      nomField = "0";
    }

    meta.setCityField(city);
    meta.setLatitudeField(latField);
    meta.setLongitudeField(longField);
    meta.setMapboxUrl(mbField);
    meta.setMapBoxKey(mbKeyField);
    meta.setPostMapboxWaitMillis(Long.parseLong(mbWaitField));
    meta.setNominatimUrl(nomField);
    meta.setPostNominatimWaitMillis(Long.parseLong(nomWaitField));
    meta.setStateField(stateField);
    meta.setStreetField(streetField);
    meta.setZipField(zipField);
    meta.setUseMapBoxFallbackIfPresent(useMbox);
    dispose();
  }
}