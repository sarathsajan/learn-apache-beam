import apache_beam as beam


def parse_csv(line):
    if line.startswith('transaction_id'):
        return None

    values = line.split(',')

    category = values[1].strip()
    item = values[2].strip()
    price = float(values[3].strip())

    return {'category': category, 'item': item, 'price': price}


def pipeline(data_file_path):
    print("starting pipeline")

    with beam.Pipeline() as p:
        (
            p
            | 'read csv file'       >>  beam.io.ReadFromText(data_file_path)
            | 'parse rows'          >>  beam.Map(parse_csv)
            | 'filter none vlaues'  >>  beam.
        )