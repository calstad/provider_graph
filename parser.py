import re

class RowParser:
    def __init__(self, row):
        self.row = row
        self.parsed_row = {}

    def parse(self):
        self.parse_npi()
        self.parse_provider()
        self.parse_addresses()
        self.parse_names()
        self.parse_licenses()
        self.parse_identifiers()
        self.parse_taxonomy_groups()
        return self.parsed_row

    def parse_npi(self):
        self.parsed_row['npi'] = {'npi' : self.row[0],
                                  'replacement_npi' : self.row[2],
                                  'deactivation_reason' : self.row[38],
                                  'deactivation_date' : self.row[39],
                                  'reactivation_date' : self.row[40]}

    def parse_provider(self):
        self.parsed_row['provider'] = {'employee_identifcation_number' : self.row[3],
                                       'organization_name' : self.row[4],
                                       'other_organization' : self.row[11],
                                       'other_organization_type' : self.row[12],
                                       'enumeration_date' : self.row[36],
                                       'last_update_date' : self.row[37],
                                       'gender' : self.row[41],
                                       'authorized_official_last_name' : self.row[42],
                                       'authorized_official_first_name'  : self.row[43],
                                       'authorized_official_middle_name'  : self.row[44],
                                       'authorized_official_title' : self.row[45],
                                       'authorized_official_phone_number' : self.row[46],
                                       'authorized_official_name_prefix_text' : self.row[311],
                                       'authorized_official_name_suffix_text' : self.row[312],
                                       'authorized_official_credential_text' : self.row[313],
                                       'is_sole_proprietor' : self.row[307],
                                       'is_organization_subpart' : self.row[308],
                                       'parent_organization_lbn' : self.row[309],
                                       'parent_organization_tin' : self.row[310]}

    def parse_addresses(self):
        addresses = [{'type' : 'mailing',
                      'street1' : self.row[20],
                      'street2' : self.row[21],
                      'city' : self.row[22],
                      'state' : self.row[23],
                      'zipcode' : self.row[24],
                      'country' : self.row[25],
                      'phone_number' : self.row[26],
                      'fax_number' : self.row[27]},
                     {'type' : 'practice_location',
                      'street1' : self.row[28],
                      'street2' : self.row[29],
                      'city' : self.row[30],
                      'state' : self.row[31],
                      'zipcode' : self.row[32],
                      'country' : self.row[33],
                      'phone_number' : self.row[34],
                      'fax_number' : self.row[35]}]
        self.remove_emtpy_records(addresses, ['type'])
        self.truncate_zipcodes(addresses)
        self.parsed_row['addresses'] = addresses

    def parse_names(self):
        names = [{'type' : 'primary',
                  'last_name' : self.row[5],
                  'first_name' : self.row[6],
                  'middle_name' : self.row[7],
                  'prefix' : self.row[8],
                  'suffix' : self.row[9]},
                 {'type' : 'secondary',
                  'last_name' : self.row[13],
                  'first_name' : self.row[14],
                  'middle_name' : self.row[15],
                  'prefix' : self.row[16],
                  'suffix' : self.row[17]}]
        self.parsed_row['names'] = self.remove_emtpy_records(names, ['type'])

    def parse_licenses(self):
        licenses = self.normalize_data(['taxonomy_code', 'number', 'state', 'taxonomy_switch'], self.row[48:106])
        self.parsed_row['licenses'] = licenses

    def parse_identifiers(self):
        identifiers = self.normalize_data(['type_code', 'state', 'issuer'], self.row[107:306])
        self.parsed_row['identifiers'] = identifiers

    def parse_taxonomy_groups(self):
        self.parsed_row['taxonomy_groups'] = filter(None, self.row[314:328])

    def remove_emtpy_records(self, coll, excluded_keys=[]):
        return filter(lambda r : not self.empty_record(r, excluded_keys), coll)

    def empty_record(self, props, excluded_keys):
        """Returns true if all values in props are blank strings exept for values
        keyed by keys in excluded_keys
        """
        tmp_dict = props.copy()
        for k in excluded_keys:
            tmp_dict.pop(k)
            return not any(tmp_dict.values())

    def truncate_zipcodes(self, records):
        """Truncates zipcodes to be 5 digits long"""
        for record in records:
            truncated_zipcode = re.search("^\d{5}", record['zipcode'])
            if truncated_zipcode:
                record['zipcode'] = truncated_zipcode.group(0)
            else:
                record['zipcode'] = ''
            
    def normalize_data(self, names, data):
        """Takes sequences of names and unnormalized records and returns a 
           a collection dicts keyed by names, one for each non-blank record.
        """
        result = []
        # Split data into appropriate chunks and filter out all blank chunks.
        tuples = filter(lambda t: any(t), self.partition(data, len(names)))
        for t in tuples:
            result.append(self.lists_to_dict(names, t))
        return result

    def partition(self, l, n):
        """Partitions list l into tuples of length n"""
        return zip(*[iter(l)]*n)

    def lists_to_dict(self, keys, values):
        """Turns a list of keys and a list of values into a dict"""
        d = {}
        for key, val in zip(keys, values):
            d[key] = val
        return d
