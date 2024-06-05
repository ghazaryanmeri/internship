import pandas as pd

class DataExtractor:
    def __init__(self, invoices_file, expired_ids_file):
        
        self.invoices_file = invoices_file
        self.expired_ids_file = expired_ids_file
        self.type_conversion = {0: 'Material', 1: 'Equipment', 2: 'Service', 3: 'Other'}

    def load_data(self):
        
        self.invoices = pd.read_pickle(self.invoices_file)
        with open(self.expired_ids_file, 'r') as file:
            self.expired_ids = set(file.read().splitlines())

    def safe_int(self, value):
        
        try:
            return int(value)
        except ValueError:
            return 0

    def transform_data(self):
        
        records = []
        for invoice in self.invoices:
            invoice_total = sum(self.safe_int(item['item']['unit_price']) * self.safe_int(item['quantity']) 
                                for item in invoice['items'])
            is_expired = invoice['id'] in self.expired_ids
            
            for item in invoice['items']:
                unit_price = self.safe_int(item['item']['unit_price'])
                quantity = self.safe_int(item['quantity'])
                total_price = unit_price * quantity
                percentage_in_invoice = total_price / invoice_total if invoice_total else 0
                
                record = {
                    'invoice_id': invoice['id'],
                    'created_on': invoice['created_on'],
                    'invoiceitem_id': item['item']['id'],
                    'invoiceitem_name': item['item']['name'],
                    'type': self.type_conversion[item['item']['type']],
                    'unit_price': unit_price,
                    'total_price': total_price,
                    'percentage_in_invoice': percentage_in_invoice,
                    'is_expired': is_expired
                }
                records.append(record)

        self.dataframe = pd.DataFrame(records)
        self.dataframe.sort_values(by=['invoice_id', 'invoiceitem_id'], inplace=True)
        self.dataframe['created_on'] = pd.to_datetime(self.dataframe['created_on'])
        self.dataframe['unit_price'] = self.dataframe['unit_price'].astype(int)
        self.dataframe['total_price'] = self.dataframe['total_price'].astype(int)
        self.dataframe['percentage_in_invoice'] = self.dataframe['percentage_in_invoice'].astype(float)
        self.dataframe['is_expired'] = self.dataframe['is_expired'].astype(bool)

    def get_transformed_data(self):
        
        return self.dataframe

# Assuming the paths to the files
invoices_path = '/mnt/data/unzipped_data/invoices_new.pkl'
expired_ids_path = '/mnt/data/unzipped_data/expired_invoices.txt'

# Create an instance of the DataExtractor
extractor = DataExtractor(invoices_path, expired_ids_path)

# Load and transform data
extractor.load_data()
extractor.transform_data()

# Get the transformed data
transformed_df = extractor.get_transformed_data()
transformed_df.head()




