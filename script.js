import pandas as pd
import json
from google.colab import files
import io
import math # Import math for checking NaN values

# --- 1. UPLOAD THE FILE ---
print("Please upload your Excel (xls/xlsx) file now:")
uploaded = files.upload()

# Get the filename (assuming only one file is uploaded)
file_name = next(iter(uploaded))
print(f"File '{file_name}' uploaded successfully.")

# Read the uploaded Excel file into a pandas DataFrame
# Note: Excel files can have multiple sheets. We assume the data is on the first sheet (sheet_name=0).
try:
    df = pd.read_excel(io.BytesIO(uploaded[file_name]), sheet_name=0)
    # Fill NaN values with None for cleaner JSON conversion later
    # df = df.where(pd.notna(df), None)
except Exception as e:
    print(f"Error reading file: {e}")
    exit()

print(f"DataFrame loaded with {len(df)} rows.")

# --- 2. DEFINE THE TRANSFORMATION FUNCTION ---
# This function applies all your specified mapping and conditional logic row by row

def is_empty(value):
    """
    Checks if a given value is considered 'empty' based on the specified criteria.
    """
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value): # Handles pandas/numpy NaN
        return True
    if isinstance(value, str):
        return value.strip() == ''
    if isinstance(value, (list, dict)):
        return not bool(value)
    return False

def transform_row_to_client_json(row):
    client_data = {}
    client_data['objectType'] = 'client' # Always "client" and always present

    # Helper function to conditionally add a field
    def add_field_if_not_empty(key, value):
        if not is_empty(value):
            client_data[key] = value

    # Helper function to format ID values as clean strings
    def _to_string_id(value):
        if isinstance(value, float) and not math.isnan(value) and value.is_integer():
            return str(int(value))
        return str(value)

    # Helper function to parse comma-separated strings into a list of strings
    def _to_string_list(value):
        if is_empty(value):
            return None
        if isinstance(value, str):
            # Split by comma, strip whitespace, and filter out empty strings
            return [item.strip() for item in value.split(',') if item.strip()]
        # If it's not a string but not empty, convert to string and then process
        return [str(value)]

    # Helper function to convert date strings to Unix timestamp in milliseconds
    def _to_unix_timestamp_ms(value):
        if is_empty(value):
            return None
        try:
            dt_object = pd.to_datetime(value)
            return int(dt_object.timestamp() * 1000)
        except (ValueError, TypeError):
            # Return None if parsing fails, so the field is not added
            return None

    add_field_if_not_empty('clientId', row.get('clientId'))
    add_field_if_not_empty('entityType', row.get('entityType'))
    add_field_if_not_empty('status', row.get('status'))

    entity_type = row.get('entityType')
    # Ensure entity_type is treated consistently, especially if it could be None or NaN
    if not is_empty(entity_type):
        entity_type_upper = str(entity_type).upper()
    else:
        entity_type_upper = None

    # 2. Name fields based on entityType
    if entity_type_upper == 'ORGANISATION':
        add_field_if_not_empty('companyName', row.get('name'))
    elif entity_type_upper == 'PERSON':
        add_field_if_not_empty('name', row.get('name'))
        add_field_if_not_empty('forename', row.get('forename'))
        add_field_if_not_empty('middlename', row.get('middlename'))
        add_field_if_not_empty('surname', row.get('surname'))
    else: # Fallback for unknown/missing entityType
        add_field_if_not_empty('name', row.get('name'))
        add_field_if_not_empty('forename', row.get('forename'))
        add_field_if_not_empty('middlename', row.get('middlename'))
        add_field_if_not_empty('surname', row.get('surname'))

    # 3. Common fields
    add_field_if_not_empty('titles', row.get('titles'))
    add_field_if_not_empty('suffixes', row.get('suffixes'))

    # 4. Person-specific fields
    if entity_type_upper == 'PERSON':
        gender_value = row.get('gender')
        if isinstance(gender_value, str) and not is_empty(gender_value):
            gender_value = gender_value.upper()
        add_field_if_not_empty('gender', gender_value)

        # Handle dateOfBirth as string
        date_of_birth_value = row.get('dateOfBirth')
        add_field_if_not_empty('dateOfBirth', str(date_of_birth_value) if not is_empty(date_of_birth_value) else date_of_birth_value)

        add_field_if_not_empty('birthPlaceCountryCode', row.get('birthPlaceCountryCode'))

        # Handle deceasedOn as string
        deceased_on_value = row.get('deceasedOn')
        add_field_if_not_empty('deceasedOn', str(deceased_on_value) if not is_empty(deceased_on_value) else deceased_on_value)

        add_field_if_not_empty('occupation', row.get('occupation'))
        # Apply _to_string_list for domicileCodes and nationalityCodes
        add_field_if_not_empty('domicileCodes', _to_string_list(row.get('domicileCodes')))
        add_field_if_not_empty('nationalityCodes', _to_string_list(row.get('nationalityCodes')))

    # 5. Organisation-specific field
    if entity_type_upper == 'ORGANISATION':
        add_field_if_not_empty('incorporationCountryCode', row.get('incorporationCountryCode'))

        # Handle dateOfIncorporation as string
        date_of_incorporation_value = row.get('dateOfIncorporation')
        add_field_if_not_empty('dateOfIncorporation', str(date_of_incorporation_value) if not is_empty(date_of_incorporation_value) else date_of_incorporation_value)


    # Process assessmentRequired early to use its boolean value for other fields
    assessment_required_raw_value = row.get('assessmentRequired')
    assessment_required_boolean = False
    if not is_empty(assessment_required_raw_value):
        assessment_required_boolean = str(assessment_required_raw_value).lower() in ['true', '1', '1.0']

    # 6. Remaining Common fields
    # Handle lastReviewed as Unix timestamp in milliseconds, only if assessmentRequired is true
    if assessment_required_boolean:
        add_field_if_not_empty('lastReviewed', _to_unix_timestamp_ms(row.get('lastReviewed')))

    # Handle periodicReviewStartDate as Unix timestamp in milliseconds
    add_field_if_not_empty('periodicReviewStartDate', _to_unix_timestamp_ms(row.get('periodicReviewStartDate')))

    # Handle periodicReviewPeriod as string
    periodic_review_period_value = row.get('periodicReviewPeriod')
    add_field_if_not_empty('periodicReviewPeriod', str(periodic_review_period_value) if not is_empty(periodic_review_period_value) else periodic_review_period_value)

    # 7. Addresses (Universal Logic)
    addresses_list = []
    current_address = {}

    # Retrieve and conditionally add address components
    address_line1 = row.get('Address line1')
    if not is_empty(address_line1):
        current_address['line1'] = str(address_line1)

    address_line2 = row.get('Address line2')
    if not is_empty(address_line2):
        current_address['line2'] = str(address_line2)

    address_line3 = row.get('Address line3')
    if not is_empty(address_line3):
        current_address['line3'] = str(address_line3)

    address_line4 = row.get('Address line4')
    if not is_empty(address_line4):
        current_address['line4'] = str(address_line4)

    po_box = row.get('poBox')
    if not is_empty(po_box):
        current_address['poBox'] = str(po_box)

    city = row.get('city')
    if not is_empty(city):
        current_address['city'] = str(city)

    state = row.get('state')
    if not is_empty(state):
        current_address['state'] = str(state)

    province = row.get('province')
    if not is_empty(province):
        current_address['province'] = str(province)

    postcode = row.get('postcode')
    if not is_empty(postcode):
        current_address['postcode'] = str(postcode)

    country = row.get('country')
    if not is_empty(country):
        current_address['country'] = str(country)

    country_code = row.get('countryCode')
    if not is_empty(country_code):
        # Ensure countryCode is uppercase 2-character string
        current_address['countryCode'] = str(country_code).upper()[:2]

    # If the current_address object has any data, add it to the list
    if current_address:
        addresses_list.append(current_address)

    # Add the addresses list to client_data only if it's not empty
    add_field_if_not_empty('addresses', addresses_list)

    # 8. Segment field
    add_field_if_not_empty('segment', str(row.get('segment')) if not is_empty(row.get('segment')) else row.get('segment'))

    # 9. Conditional Identity Numbers Array
    identity_numbers_list = []

    if entity_type_upper == 'ORGANISATION':
        duns_number = row.get('Duns Number')
        if not is_empty(duns_number):
            identity_numbers_list.append({"type": "duns", "value": _to_string_id(duns_number)})

        national_tax_no = row.get('National Tax No.')
        if not is_empty(national_tax_no):
            identity_numbers_list.append({"type": "tax_no", "value": _to_string_id(national_tax_no)})

        legal_entity_identifier = row.get('Legal Entity Identifier (LEI)')
        if not is_empty(legal_entity_identifier):
            identity_numbers_list.append({"type": "lei", "value": _to_string_id(legal_entity_identifier)})

    elif entity_type_upper == 'PERSON':
        national_id = row.get('National ID')
        if not is_empty(national_id):
            identity_numbers_list.append({"type": "national_id", "value": _to_string_id(national_id)})

        # Updated to prioritize 'Driving Licence No.' as per user's request
        driving_licence_no = row.get('Driving Licence No.')
        if not is_empty(driving_licence_no):
            identity_numbers_list.append({"type": "driving_licence", "value": _to_string_id(driving_licence_no)})

        social_security_number = row.get('Social Security Number')
        if not is_empty(social_security_number):
            identity_numbers_list.append({"type": "ssn", "value": _to_string_id(social_security_number)})

        passport_number = row.get('Passport No.')
        if not is_empty(passport_number):
            identity_numbers_list.append({"type": "passport_no", "value": _to_string_id(passport_number)})

    if identity_numbers_list: # Only add if the list is not empty
        client_data['identityNumbers'] = identity_numbers_list

    # 10. Conditional Aliases Array (Modified Logic)
    aliases_list = []
    alias_columns = ['aliases1', 'aliases2', 'aliases3', 'aliases4']
    alias_name_types = {
        'aliases1': 'AKA1',
        'aliases2': 'AKA2',
        'aliases3': 'AKA3',
        'aliases4': 'AKA4',
    }

    # Apply AKA mapping universally for nameType
    for col_name in alias_columns:
        alias_value = row.get(col_name)
        if not is_empty(alias_value):
            name_type = alias_name_types.get(col_name.lower(), col_name.upper()) # Fallback to original upper if not found

            # Conditionally set the key for alias value based on entityType
            if entity_type_upper == 'PERSON':
                aliases_list.append({"name": str(alias_value), "nameType": name_type})
            else:
                aliases_list.append({"companyName": str(alias_value), "nameType": name_type})

    if aliases_list:
        client_data['aliases'] = aliases_list

    # 11. Conditional Security Object
    security_enabled = row.get('Security Enabled')

    # Check if the security column value indicates "true" (case-insensitive check)
    if security_enabled is not None and str(security_enabled).lower() in ['true', 't', '1']:
        security_tags = {}

        # Only add tag field if content exists and is not empty
        tag1 = row.get('Tag 1')
        if not is_empty(tag1):
            security_tags['orTags1'] = tag1

        tag2 = row.get('Tag 2')
        if not is_empty(tag2):
            security_tags['orTags2'] = tag2

        tag3 = row.get('Tag 3')
        if not is_empty(tag3):
            security_tags['orTags3'] = tag3

        # Always add the 'security' field as an object if security_enabled is true,
        # even if security_tags is empty.
        client_data['security'] = security_tags
    # If security_enabled is false or empty, the 'security' field will be omitted as it's not added to client_data.

    # This should always be the last field added to client_data
    if not is_empty(assessment_required_raw_value):
        add_field_if_not_empty('assessmentRequired', assessment_required_boolean)

    return client_data

# --- 3. APPLY TRANSFORMATION AND GENERATE JSONL ---

# Apply the transformation function to every row
transformed_data_raw = df.apply(transform_row_to_client_json, axis=1).tolist()

# Filter out records that are too sparse and likely represent header rows or empty entries
transformed_data = []
for record in transformed_data_raw:
    has_entity_type = 'entityType' in record and not is_empty(record['entityType'])
    has_name_info = any(key in record and not is_empty(record[key]) for key in ['name', 'forename', 'surname', 'companyName'])

    # A record is considered valid for output if it has an entityType OR any name information.
    if has_entity_type or has_name_info:
        transformed_data.append(record)

# Define the output filename
output_filename = file_name.rsplit('.', 1)[0] + '.jsonl'
jsonl_content = ""

# Write each dictionary as a JSON line
for record in transformed_data:
    # Use json.dumps to convert the Python dict to a JSON string
    # ensure_ascii=False handles special characters properly
    # Separators are used to make the JSON compact (no extra spaces/newlines)
    json_string = json.dumps(record, ensure_ascii=False, separators=(',', ':'))
    jsonl_content += json_string + '\n'

# --- 4. DOWNLOAD THE RESULTING FILE ---
with open(output_filename, 'w', encoding='utf-8') as f:
    f.write(jsonl_content)

print(f"\n✅ Transformation complete. The JSONL file is ready to download.")
print(f"Output file: {output_filename}")

# Trigger the file download in the browser
files.download(output_filename)
