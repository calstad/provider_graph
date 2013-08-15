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
        
    def current_batch_index(self):
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

    def create_provider_node(self, record):
        self.batch.create(node(record['provider']))
        self.current_provider_index = self.current_batch_index()
        self.batch.add_indexed_node_or_fail('providers', 'npi', record['npi']['npi'], self.current_provider_index)

    def create_npi_node(self, record):
        npi = record['npi']['npi']
        npi_node = self.batch.create(record['npi'])
        self.batch.create(rel(self.current_provider_index, 'has_npi', self.current_batch_index()))
    
    def create_name_nodes(self, record):
        names = record['names']
        for name in names:
            self.batch.create(name)
            self.batch.create(rel(self.current_provider_index, 'has_name', self.current_batch_index()))

    def create_address_nodes(self, record):
        addresses = record['addresses']
        for address in addresses:
            self.batch.create(address)
            address_index = self.current_batch_index()
            self.batch.create(rel(self.current_provider_index, 'has_address', address_index))
            self.create_zipcode_node(address_index, address['zipcode'])
    
    def create_zipcode_node(self, address_index, zipcode):
        if zipcode:
            self.batch.get_or_create_indexed_node('zipcodes', 'zipcode', zipcode, {'zipcode' : zipcode})
            zipcode_index = self.current_batch_index()
            self.batch.get_or_create_indexed_relationship('addresses', 'zipcode', zipcode, address_index, 'in_zipcode', zipcode_index)
                
bgi = BatchGraphImporter('http://localhost:7474/db/data/')

