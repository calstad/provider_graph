import re
from py2neo import neo4j, node, rel

class BatchGraphImporter:
    def __init__(self, database_url):
        self.db = neo4j.GraphDatabaseService(database_url)

    def batch_persist(self, records):
        self.batch = neo4j.WriteBatch(self.db)
        for record in records:
            self.create_provider_subgraph(record)
        return self.batch.submit()

    def current_batch_idx(self):
        """Returns the index of the last item in the batch, which 
           is needed to construct relationships between nodes in the batch.
        """
        batch_length = len(self.batch.requests)
        if batch_length == 0:
            return 0
        else:
            return batch_length - 1

    def create_provider_subgraph(self, record):
        self.create_provider_node(record)
        self.create_npi_node(record)
        self.create_name_nodes(record)
        self.create_address_nodes(record)
        self.create_license_and_identifier_nodes(record)

    def create_provider_node(self, record):
        self.batch.create(node(record['provider']))
        self.current_provider_idx = self.current_batch_idx()
        self.batch.add_indexed_node_or_fail('providers', 'npi', record['npi']['npi'], self.current_provider_idx)

    def relate_to_provider(self, node_idx, entity_type):
        """Takes a node index and an entity type and creates a relation between the 
           provider and the node of 'provider has_kind node'.
        """
        relation = "has_" + entity_type
        self.batch.create(rel(self.current_provider_idx, relation, node_idx))

    def create_npi_node(self, record):
        npi = record['npi']['npi']
        npi_node = self.batch.create(record['npi'])
        self.relate_to_provider(self.current_batch_idx(), 'npi')

    def create_name_nodes(self, record):
        names = record['names']
        for name in names:
            self.batch.create(name)
            self.relate_to_provider(self.current_batch_idx(), 'name')

    def create_address_nodes(self, record):
        npi = record['npi']['npi']
        addresses = record['addresses']
        for address in addresses:
            self.batch.create(address)
            address_idx = self.current_batch_idx()
            self.relate_to_provider(address_idx, 'address')
            self.create_zipcode_node(address_idx, address, npi)
            self.create_city_node(address_idx, address, npi)

    def create_zipcode_node(self, address_idx, address, npi):
        zipcode = address['zipcode']
        if zipcode:
            self.batch.get_or_create_indexed_node('zipcodes', 'zipcode', zipcode, {'zipcode' : zipcode})
            zipcode_idx = self.current_batch_idx()
            # A hack due the fact py2neo doesent allow non-indexed
            # relationshihps between unique indexed nodes.
            zipcode_relationship_value = " ".join([npi, address['type'], zipcode])
            self.batch.get_or_create_indexed_relationship('addresses',
                                                          'zipcode',
                                                          zipcode_relationship_value,
                                                          address_idx,
                                                          'in_zipcode',
                                                          zipcode_idx)

    def create_city_node(self, address_idx, address, npi):
        """If given a valid city and state, creates a city to state path from address"""
        city, state = address['city'], address['state']
        if city and self.valid_state(state):
            city_state = city + ", " + state
            self.batch.get_or_create_indexed_node('cities', 'city_state', city_state, {'name' : city})
            city_idx = self.current_batch_idx()
            # Same hack as zipcode above.
            city_relationship_value = " ".join([npi, address['type'], city_state])
            # Relate city and address
            self.batch.get_or_create_indexed_relationship('addresses',
                                                          'city',
                                                          city_relationship_value,
                                                          address_idx,
                                                          'in_city',
                                                          city_idx)
            # Relate city to state
            city_state_idx = {'name' : 'city_states', 'key' : 'city_state', 'value' : city_relationship_value, 'relation' : 'in_state'}
            self.relate_state_to_node(city_idx, city_state_idx, state)

    def create_license_and_identifier_nodes(self, record):
        """Creates license and identifier nodes and relates them to the 
           provider and a state if a valid state is present.
        """
        npi = record['npi']['npi']
        entities = ['licenses', 'identifiers']
        for entity in entities:
            instances = record[entity]
            instance_count = 0
            for instance in instances:
                instance_count += 1
                self.batch.create(instance)
                instance_idx = self.current_batch_idx()
                self.relate_to_provider(instance_idx, entity)
                instance_state = instance['state']
                if self.valid_state(instance_state):
                    instance_rel_idx = npi + " " + entity + str(instance_count)
                    instance_rel = {'name' : entity + 's', 'key' : entity,  'value' : instance_rel_idx, 'relation' : entity + '_in_state'}
                    self.relate_state_to_node(instance_idx, instance_rel, instance_state)

    def relate_state_to_node(self, node_idx, rel_idx, state):
        """Takes a node index, a dict of relationship index properties, and the state
           and creates a releationship between node and state in the batch
        """
        self.batch.get_or_create_indexed_node('states', 'name', state, {'code' : state})
        state_idx = self.current_batch_idx()
        # Relate node and state
        self.batch.get_or_create_indexed_relationship(rel_idx['name'],
                                                      rel_idx['key'],
                                                      rel_idx['value'],
                                                      node_idx,
                                                      rel_idx['relation'],
                                                      state_idx)
    def valid_state(self, state):
        """Returns true if state is a two uppercase character code"""
        state_code_matcher = re.compile("^[A-Z]{2}$")
        if state_code_matcher.match(state):
            return True
        else:
            return False

