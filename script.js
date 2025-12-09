// script.js
// Browser-side XLS/XLSX -> JSONL converter fully aligned with Python/Colab logic.

(function () {
  // --- UTILITY FUNCTIONS ---
  function isEmpty(value) {
    if (value === null || value === undefined) return true;
    if (typeof value === 'number' && isNaN(value)) return true;
    if (typeof value === 'string') return value.trim() === '';
    if (Array.isArray(value)) return value.length === 0;
    if (typeof value === 'object') {
      if (value instanceof Date) return isNaN(value.getTime());
      return Object.keys(value).length === 0;
    }
    return false;
  }

  function _to_string_list(value) {
    if (isEmpty(value)) return null;
    if (Array.isArray(value)) return value.map(v => String(v).trim()).filter(Boolean);
    if (typeof value === 'string') return value.split(',').map(s => s.trim()).filter(Boolean);
    return [String(value)];
  }

  function _to_unix_timestamp_ms(value) {
    if (isEmpty(value)) return null;
    if (value instanceof Date) return isNaN(value.getTime()) ? null : value.getTime();
    if (typeof value === 'number') {
      if (value > 1e12) return Math.floor(value);
      if (value > 1e9) return Math.floor(value * 1000);
    }
    const parsed = Date.parse(String(value));
    if (!isNaN(parsed)) return parsed;
    return null;
  }

  function normalizeKey(k) {
    if (k === undefined || k === null) return '';
    return String(k).replace(/^"+|"+$/g, '').trim();
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

    // 1. Primary fields
    addFieldIfNotEmpty('clientId', row['clientId']);
    addFieldIfNotEmpty('entityType', row['entityType']);
    addFieldIfNotEmpty('status', row['status']);

    const entityTypeUpper = !isEmpty(row['entityType']) ? String(row['entityType']).toUpperCase() : null;

    // 2. Name fields
    if (entityTypeUpper === 'ORGANISATION') {
      addFieldIfNotEmpty('companyName', row['name']);
    } else {
      addFieldIfNotEmpty('name', row['name']);
      addFieldIfNotEmpty('forename', row['forename']);
      addFieldIfNotEmpty('middlename', row['middlename']);
      addFieldIfNotEmpty('surname', row['surname']);
    }

    // 3. Titles and suffixes (array of strings)
    addFieldIfNotEmpty('titles', _to_string_list(row['titles']));
    addFieldIfNotEmpty('suffixes', _to_string_list(row['suffixes']));

    // 4. Person-specific fields
    if (entityTypeUpper === 'PERSON') {
      let genderValue = row['gender'];
      if (typeof genderValue === 'string' && !isEmpty(genderValue)) genderValue = genderValue.toUpperCase();
      addFieldIfNotEmpty('gender', genderValue);

      addFieldIfNotEmpty('dateOfBirth', isEmpty(row['dateOfBirth']) ? row['dateOfBirth'] : String(row['dateOfBirth']));
      addFieldIfNotEmpty('birthPlaceCountryCode', row['birthPlaceCountryCode']);
      addFieldIfNotEmpty('deceasedOn', isEmpty(row['deceasedOn']) ? row['deceasedOn'] : String(row['deceasedOn']));
      addFieldIfNotEmpty('occupation', row['occupation']);
      addFieldIfNotEmpty('domicileCodes', _to_string_list(row['domicileCodes']));
      addFieldIfNotEmpty('nationalityCodes', _to_string_list(row['nationalityCodes']));
    }

    // 5. Organisation-specific fields
    if (entityTypeUpper === 'ORGANISATION') {
      addFieldIfNotEmpty('incorporationCountryCode', row['incorporationCountryCode']);
      addFieldIfNotEmpty('dateOfIncorporation', isEmpty(row['dateOfIncorporation']) ? row['dateOfIncorporation'] : String(row['dateOfIncorporation']));
    }

    // 6. Assessment Required and review fields
    const assessmentRequiredRawValue = row['assessmentRequired'];
    let assessmentRequiredBoolean = false;
    if (!isEmpty(assessmentRequiredRawValue)) {
      assessmentRequiredBoolean = ['true','1','1.0','t','yes','y'].includes(String(assessmentRequiredRawValue).toLowerCase());
    }

    if (assessmentRequiredBoolean) addFieldIfNotEmpty('lastReviewed', _to_unix_timestamp_ms(row['lastReviewed']));
    addFieldIfNotEmpty('periodicReviewStartDate', _to_unix_timestamp_ms(row['periodicReviewStartDate']));
    addFieldIfNotEmpty('periodicReviewPeriod', isEmpty(row['periodicReviewPeriod']) ? row['periodicReviewPeriod'] : String(row['periodicReviewPeriod']));

    // 7. Addresses
    const currentAddress = {};
    if (!isEmpty(row['Address line1'])) currentAddress.line1 = String(row['Address line1']);
    if (!isEmpty(row['Address line2'])) currentAddress.line2 = String(row['Address line2']);
    if (!isEmpty(row['Address line3'])) currentAddress.line3 = String(row['Address line3']);
    if (!isEmpty(row['Address line4'])) currentAddress.line4 = String(row['Address line4']);
    if (!isEmpty(row['poBox'])) currentAddress.poBox = String(row['poBox']);
    if (!isEmpty(row['city'])) currentAddress.city = String(row['city']);
    if (!isEmpty(row['state'])) currentAddress.state = String(row['state']);
    if (!isEmpty(row['province'])) currentAddress.province = String(row['province']);
    if (!isEmpty(row['postcode'])) currentAddress.postcode = String(row['postcode']);
    if (!isEmpty(row['country'])) currentAddress.country = String(row['country']);
    if (!isEmpty(row['countryCode'])) currentAddress.countryCode = String(row['countryCode']).toUpperCase().substring(0,2);
    if (Object.keys(currentAddress).length > 0) addFieldIfNotEmpty('addresses', [currentAddress]);

    // 8. Segment
    addFieldIfNotEmpty('segment', isEmpty(row['segment']) ? row['segment'] : String(row['segment']));

    // 9. Aliases
    const aliasColumns = ['aliases1','aliases2','aliases3','aliases4'];
    const aliasNameTypes = {'aliases1':'AKA1','aliases2':'AKA2','aliases3':'AKA3','aliases4':'AKA4'};
    const aliasesList = [];
    for (const col of aliasColumns) {
      const val = row[col];
      if (!isEmpty(val)) {
        const nameType = aliasNameTypes[col] || col.toUpperCase();
        if (entityTypeUpper === 'PERSON') aliasesList.push({name:String(val), nameType});
        else aliasesList.push({companyName:String(val), nameType});
      }
    }
    if (aliasesList.length > 0) clientData.aliases = aliasesList;

    // 10. Security
    const securityEnabled = row['Security Enabled'];
    if (!isEmpty(securityEnabled) && ['true','t','1','yes','y'].includes(String(securityEnabled).toLowerCase())) {
      const securityTags = {};
      if (!isEmpty(row['Tag 1'])) securityTags.orTags1 = row['Tag 1'];
      if (!isEmpty(row['Tag 2'])) securityTags.orTags2 = row['Tag 2'];
      if (!isEmpty(row['Tag 3'])) securityTags.orTags3 = row['Tag 3'];
      clientData.security = securityTags;
    }

    // 11. AssessmentRequired last to preserve field order
    if (!isEmpty(assessmentRequiredRawValue)) addFieldIfNotEmpty('assessmentRequired', assessmentRequiredBoolean);

    return clientData;
  }

  // --- INIT UI ---
  function init() {
    const fileInput = document.getElementById('fileInput');
    const convertBtn = document.getElementById('convertBtn');
    const outputEl = document.getElementById('output');
    const downloadLink = document.getElementById('downloadLink');

    convertBtn.addEventListener('click', () => {
      if (!fileInput.files || fileInput.files.length === 0) return alert('Please choose an XLS/XLSX file first.');
      const reader = new FileReader();
      reader.onload = function (e) {
        const data = e.target.result;
        const workbook = XLSX.read(data, {type:'array', cellDates:true});
        const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
        const rows = XLSX.utils.sheet_to_json(firstSheet, {defval:null, raw:false});
        const transformed = rows.map(transformRowToClientJson).filter(rec => {
          const hasEntityType = rec.entityType && !isEmpty(rec.entityType);
          const hasNameInfo = ['name','forename','surname','companyName'].some(k => rec[k] && !isEmpty(rec[k]));
          return hasEntityType || hasNameInfo;
        });
        if (!transformed.length) {
          outputEl.textContent = 'No valid rows found for conversion.';
          downloadLink.style.display = 'none';
          return;
        }
        const jsonlContent = transformed.map(r => JSON.stringify(r)).join('\n');
        outputEl.textContent = jsonlContent.slice(0,4000) + (jsonlContent.length>4000?'\n\n...preview truncated...':'');
        const blob = new Blob([jsonlContent], {type:'application/json'});
        const url = URL.createObjectURL(blob);
        downloadLink.href = url;
        downloadLink.download = fileInput.files[0].name.replace(/\.[^/.]+$/, '') + '.jsonl';
        downloadLink.style.display = 'inline-block';
        downloadLink.textContent = 'Download JSONL file';
      };
      reader.readAsArrayBuffer(fileInput.files[0]);
    });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
