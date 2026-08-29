import apache_beam as beam

def parse_csv(line):
    # Skip the header row if it's encountered
    if line.startswith('transaction_id'):
        return None
    
    # Split the CSV line by comma
    parts = line.split(',')
    
    # Extract columns based on positions: category (1), item (2), price (3)
    category = parts[1].strip()
    item = parts[2].strip()
    price = float(parts[3].strip())
    
    return {
        'category': category,
        'item': item,
        'price': price
    }

def calculate_tax(row):
    # Calculate a 10% tax and add it to the row dict
    tax_amount = row['price'] * 0.10
    row['total_with_tax'] = round(row['price'] + tax_amount, 2)
    return row

def format_output(row):
    # Convert the processed dictionary back into a readable string format
    return f"Item: {row['item']} | Category: {row['category']} | Total: ${row['total_with_tax']}"

def run_csv_pipeline():
    print("🚀 Processing local CSV file...")
    
    with beam.Pipeline() as p:
        (
            p
            # 1. Read the local CSV text file
            | 'Read CSV File'   >> beam.io.ReadFromText('sales_data.csv')
            
            # 2. Convert CSV text strings into Python dictionaries
            | 'Parse Rows'      >> beam.Map(parse_csv)
            
            # 3. Filter out the header row (which returned None)
            | 'Filter None'     >> beam.Filter(lambda row: row is not None)
            
            # 4. Only keep items that cost $50.00 or more
            | 'Filter Expenses' >> beam.Filter(lambda row: row['price'] >= 50.00)
            
            # 5. Apply the tax calculation transform
            | 'Compute Tax'     >> beam.Map(calculate_tax)
            
            # 6. Format the dictionary into a print-friendly string
            | 'Format Results'  >> beam.Map(format_output)
            
            # 7. Save final processed results to a local file
            | 'Write Output'    >> beam.io.WriteToText('processed_sales_output')
        )
        
    print("✅ Pipeline complete! Open 'processed_sales_output-00000-of-00001' to see results.")

if __name__ == '__main__':
    run_csv_pipeline()
