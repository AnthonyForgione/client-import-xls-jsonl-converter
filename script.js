(function () {
  // --- UTILITY FUNCTIONS ---
  function isEmpty(value) {
    if (value === null || value === undefined) return true;
    if (typeof value === 'number' && Number.isNaN(value)) return true;
    if (typeof value === 'string') return value.trim() === '';
    if (Array.isArray(value)) return value.length === 0;
    if (typeof value === 'object') return Object.keys(value).length === 0;
    return false;
  }

  function _to_string_id(value) {
    if (typeof value === 'number' && !Number.isNaN(value) && Number.isInteger(value)) return String(value);
    if (typeof value === 'number' && !Number.isNaN(value)) return String(Math.floor(value));
    return String(value);
  }

  function _to_string_list(value) {
    if (isEmpty(value)) return null;
    if (Array.isArray(value)) return value.map(v => String(v).trim()).filter(Boolean);
    if (typeof value === 'string') return value.split(',').map(s => s.trim()).filter(Boolean);
    return [String(value)];
  }

  function _to_unix_timestamp_ms(value) {
    if (isEmpty(value)) return null;
    const d = new Date(value);
    if (!isNaN(d.getTime())) return d.getTime();
    return null;
  }

  function normalizeKey(k) {
    if (k === undefined || k === null) return '';
    return String(k).replace(/^"+|"+$/g, '').trim().toLowerCase().replace(/[^a-z0-9]/g, '');
  }

  function normalizeRowKeys(row) {
    const out = {};
    for (const [k, v] of Object.entries(row)) {
      out[normalizeKey(k)] = v;
    }
    return out;
  }

  // --- MAIN TRANSFORMATION ---
  function transformRowToClientJson(rowRaw) {
    const row = normalizeRowKeys(rowRaw);
    const clientData = {objectType: 'client'};

    function addFieldIfNotEmpty(key, value) {
      if (!isEmpty(value)) clientData[key] = value;
    }

    addFieldIfNotEmpty('clientId', row['clientid']);
    addFieldIfNotEmpty('entityType', row['entitytype']);
    addFieldIfNotEmpty('status', row['status']);

    const entityTypeUpper = row['entitytype'] ? String(row['entitytype']).toUpperCase() : null;

    // --- Name fields ---
    if (entityTypeUpper === 'ORGANISATION') {
      addFieldIfNotEmpty('companyName', row['name']);
    } else {
      addFieldIfNotEmpty('name', row['name']);
      addFieldIfNotEmpty('forename', row['forename']);
      addFieldIfNotEmpty('middlename', row['middlename']);
      addFieldIfNotEmpty('surname', row['surname']);
    }

    // --- Titles & suffixes ---
    addFieldIfNotEmpty('titles', _to_string_list(row['titles']));
    addFieldIfNotEmpty('suffixes', _to_string_list(row['suffixes']));

    // --- Person-specific fields ---
    if (entityTypeUpper === 'PERSON') {
      let gender = row['gender'];
      if (typeof gender === 'string') gender = gender.toUpperCase();
      addFieldIfNotEmpty('gender', gender);

      addFieldIfNotEmpty('dateOfBirth', row['dateofbirth'] ? String(row['dateofbirth']) : row['dateofbirth']);
      addFieldIfNotEmpty('birthPlaceCountryCode', row['birthplacecountrycode']);
      addFieldIfNotEmpty('deceasedOn', row['deceasedon'] ? String(row['deceasedon']) : row['deceasedon']);
      addFieldIfNotEmpty('occupation', row['occupation']);
      addFieldIfNotEmpty('domicileCodes', _to_string_list(row['domicilecodes']));
      addFieldIfNotEmpty('nationalityCodes', _to_string_list(row['nationalitycodes']));
    }

    // --- Organisation-specific fields ---
    if (entityTypeUpper === 'ORGANISATION') {
      addFieldIfNotEmpty('incorporationCountryCode', row['incorporationcountrycode']);
      addFieldIfNotEmpty('dateOfIncorporation', row['dateofincorporation'] ? String(row['dateofincorporation']) : row['dateofincorporation']);
    }

    // --- Assessment fields ---
    const assessmentRequiredRaw = row['assessmentrequired'];
    let assessmentRequiredBoolean = false;
    if (!isEmpty(assessmentRequiredRaw)) {
      assessmentRequiredBoolean = ['true','1','1.0','t','yes','y'].includes(String(assessmentRequiredRaw).toLowerCase());
    }

    if (assessmentRequiredBoolean) addFieldIfNotEmpty('lastReviewed', _to_unix_timestamp_ms(row['lastreviewed']));
    addFieldIfNotEmpty('periodicReviewStartDate', _to_unix_timestamp_ms(row['periodicreviewstartdate']));
    addFieldIfNotEmpty('periodicReviewPeriod', row['periodicreviewperiod'] ? String(row['periodicreviewperiod']) : row['periodicreviewperiod']);

    // --- Addresses ---
    const addrKeys = ['addressline1','addressline2','addressline3','addressline4','pobox','city','state','province','postcode','country','countrycode'];
    const addrMap = {addressline1:'line1', addressline2:'line2', addressline3:'line3', addressline4:'line4', pobox:'poBox', city:'city', state:'state', province:'province', postcode:'postcode', country:'country', countrycode:'countryCode'};
    const currentAddress = {};
    addrKeys.forEach(k => {
      if (!isEmpty(row[k])) {
        currentAddress[addrMap[k]] = k==='countrycode'?String(row[k]).toUpperCase().substring(0,2):String(row[k]);
      }
    });
    if (Object.keys(currentAddress).length>0) addFieldIfNotEmpty('addresses',[currentAddress]);

    // --- Segment ---
    addFieldIfNotEmpty('segment', row['segment'] ? String(row['segment']) : row['segment']);

    // --- Identity Numbers ---
    const identityNumbers = [];
    
    // National Tax No. is now added for both PERSON and ORGANISATION
    if (!isEmpty(row['nationaltaxno'])) {
      identityNumbers.push({type:'tax_no', value:_to_string_id(row['nationaltaxno'])});
    }

    if (entityTypeUpper === 'ORGANISATION') {
      if (!isEmpty(row['dunsnumber'])) identityNumbers.push({type:'duns', value:_to_string_id(row['dunsnumber'])});
      if (!isEmpty(row['legalentityidentifierlei'])) identityNumbers.push({type:'lei', value:_to_string_id(row['legalentityidentifierlei'])});
    } else if (entityTypeUpper === 'PERSON') {
      if (!isEmpty(row['nationalid'])) identityNumbers.push({type:'national_id', value:_to_string_id(row['nationalid'])});
      if (!isEmpty(row['drivinglicenceno'])) identityNumbers.push({type:'driving_licence', value:_to_string_id(row['drivinglicenceno'])});
      if (!isEmpty(row['socialsecuritynumber'])) identityNumbers.push({type:'ssn', value:_to_string_id(row['socialsecuritynumber'])});
      if (!isEmpty(row['passportno'])) identityNumbers.push({type:'passport_no', value:_to_string_id(row['passportno'])});
    }

    if (identityNumbers.length>0) clientData['identityNumbers'] = identityNumbers;

    // --- Aliases ---
    const aliasesList = [];
    const aliasCols = ['aliases1','aliases2','aliases3','aliases4'];
    const aliasMap = {aliases1:'AKA1', aliases2:'AKA2', aliases3:'AKA3', aliases4:'AKA4'};
    aliasCols.forEach(col=>{
      if(!isEmpty(row[col])) {
        if(entityTypeUpper==='PERSON') aliasesList.push({name:String(row[col]), nameType:aliasMap[col]});
        else aliasesList.push({companyName:String(row[col]), nameType:aliasMap[col]});
      }
    });
    if(aliasesList.length>0) clientData['aliases']=aliasesList;

    // --- Security ---
    const securityEnabled=row['securityenabled'];
    if(!isEmpty(securityEnabled) && ['true','t','1','yes','y'].includes(String(securityEnabled).toLowerCase())){
      const securityTags={};
      if(!isEmpty(row['tag1'])) securityTags.orTags1=row['tag1'];
      if(!isEmpty(row['tag2'])) securityTags.orTags2=row['tag2'];
      if(!isEmpty(row['tag3'])) securityTags.orTags3=row['tag3'];
      clientData['security']=securityTags;
    }

    // --- assessmentRequired last ---
    if(!isEmpty(assessmentRequiredRaw)) addFieldIfNotEmpty('assessmentRequired', assessmentRequiredBoolean);

    return clientData;
  }

  // --- INIT UI ---
  function init(){
    const fileInput=document.getElementById('fileInput');
    const convertBtn=document.getElementById('convertBtn');
    const outputEl=document.getElementById('output');
    const downloadLink=document.getElementById('downloadLink');

    convertBtn.addEventListener('click',()=>{
      if(!fileInput.files || fileInput.files.length===0) return alert('Please choose an XLS/XLSX file first.');
      const reader=new FileReader();
      reader.onload=function(e){
        const data=e.target.result;
        const workbook=XLSX.read(data,{type:'array',cellDates:true});
        const firstSheet=workbook.Sheets[workbook.SheetNames[0]];
        const rows=XLSX.utils.sheet_to_json(firstSheet,{defval:null,raw:false});
        const transformed=rows.map(transformRowToClientJson).filter(rec=>{
          const hasEntityType=rec.entityType && !isEmpty(rec.entityType);
          const hasNameInfo=['name','forename','surname','companyName'].some(k=>rec[k] && !isEmpty(rec[k]));
          return hasEntityType || hasNameInfo;
        });
        if(!transformed.length){
          outputEl.textContent='No valid rows found for conversion.';
          downloadLink.style.display='none';
          return;
        }
        const jsonlContent=transformed.map(r=>JSON.stringify(r)).join('\n');
        outputEl.textContent=jsonlContent.slice(0,4000)+(jsonlContent.length>4000?'\n\n...preview truncated...':'');
        const blob=new Blob([jsonlContent],{type:'application/json'});
        const url=URL.createObjectURL(blob);
        downloadLink.href=url;
        downloadLink.download=fileInput.files[0].name.replace(/\.[^/.]+$/,'')+'.jsonl';
        downloadLink.style.display='inline-block';
        downloadLink.textContent='Download JSONL file';
      };
      reader.readAsArrayBuffer(fileInput.files[0]);
    });
  }

  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',init);
  else init();
})();
