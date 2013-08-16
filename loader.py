import sys
import csv
from data import BatchGraphImporter
from parser import RowParser

def load_npi_file(file_name, neo4j_url):
    with open(file_name) as npi_csv:
        npi_reader = csv.reader(npi_csv)
        # Skip the csv header
        npi_reader.next()
        npi_importer = BatchGraphImporter(neo4j_url)
        rows = []
        for row in npi_reader:
            row_parser = RowParser(row)
            rows.append(row_parser.parse())
            # Import in batches of 3000
            if len(rows) % 3000 == 0:
                npi_importer.batch_persist(rows)
                rows = []
        #Import the last batch
        npi_importer.batch_persist(rows)

if __name__ == '__main__':
    load_npi_file(sys.argv[1], sys.argv[2])
