import sys
import csv
from data import ProviderGraph
from parser import RowParser

def load_npi_file(file_name, neo4j_url, skip_header=True):
    with open(file_name) as npi_csv:
        npi_reader = csv.reader(npi_csv)
        pg = ProviderGraph('http://localhost:7474/db/data/')
        if skip_header:
            npi_reader.next()
        rows = []
        for row in npi_reader:
            rp = RowParser(row).parse()
            rows.append(rp)
            if len(rows) % 10000 == 0:
                pg.create_provider_nodes(rows)
                rows = []

if __name__ == '__main__':
    load_npi_file(sys.argv[1], "foo")
