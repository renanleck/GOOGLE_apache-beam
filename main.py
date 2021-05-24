import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

# Download these files to run the project.
# https://caelum-online-public.s3.amazonaws.com/1954-apachebeam/alura-apachebeam-basedados.rar  # noqa

dengue_columns = ['id',
                  'data_iniSE',
                  'casos',
                  'ibge_code',
                  'cidade',
                  'uf',
                  'cep',
                  'latitude',
                  'longitude']


# Receive two lists and return a dict
def list_to_dict(element, columns):
    return dict(zip(columns, element))


# Receive a dict and create a new field 'ANO-MES,
# return the same dict with this new field
def treatment_to_date(element):
    element['ano_mes'] = '-'.join(element['data_iniSE'].split("-")[:2])
    return element


# Receive a dict and return a tuple
def key_uf(element):
    key = element['uf']
    return key, element


# Receive a tuple like ('RS', [{}, {}]
# return a tuple like ('RS-2014-12', 8.0)
def dengue_cases(element):
    uf, registers = element
    for register in registers:
        if bool(re.search(r'\d', register['casos'])):
            yield f"{uf}-{register['ano_mes']}", float(register['casos'])
        else:
            yield f"{uf}-{register['ano_mes']}", 0.0


# Receive a list of elements and return a tuple contains a KEY, VALUE of
# 'chuvas' in 'mm' like ("UF-ANO-MES', 1.3)
def key_uf_year_month_list(element):
    date, mm, uf = element
    year_month = '-'.join(date.split('-')[:2])
    key = f"{uf}-{year_month}"
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return key, mm


# Receive a tuple and return a tuple with rounding
def rounding(element):
    key, mm = element
    return key, round(mm, 1)


# Remove data with empty values
def filter_empty_fields(element):
    key, values = element
    if all([
        values['chuvas'],
        values['dengue']
    ]):
        return True
    return False


# Receive a tuple ('CE-2015-01', {'chuvas': [85.8], 'dengue': [175.0]})
# return a tuple like ('CE', '2015', '11', '0.4', '21.0')
def unzip_elements(element):
    key, values = element
    chuva = values['chuvas'][0]
    dengue = values['dengue'][0]
    uf, year, month = key.split("-")
    return uf, year, month, str(chuva), str(dengue)


# receive a tuple ('CE', 2015, 1, 85.8, 175.0)
# return a string delimited like "CE;2015;1;85.8;175.0"
def prepare_csv(element, delimiter=";"):
    return f"{delimiter}".join(element)


dengue = (
        pipeline
        | "Read dataset 'dengue.txt'" >> ReadFromText(
            "casos_dengue.txt",
            skip_header_lines=1
        )
        | "From text to list" >> beam.Map(lambda x: x.split("|"))
        | "From list to dict" >> beam.Map(list_to_dict, dengue_columns)
        | "Create fields 'ANO_MES'" >> beam.Map(treatment_to_date)
        | "Create KEY for each UF" >> beam.Map(key_uf)
        | "Group by UF" >> beam.GroupByKey()
        | "Unzip cases of dengue" >> beam.FlatMap(dengue_cases)
        | "Sum cases for each KEY" >> beam.CombinePerKey(sum)
)

chuvas = (
        pipeline
        | "Read dataset 'chuvas.csv'" >> ReadFromText("chuvas.csv",
                                                      skip_header_lines=1)
        | "From texto to list (chuvas)" >> beam.Map(lambda x: x.split(","))
        | "Create KEY 'UF-ANO_MES'" >> beam.Map(key_uf_year_month_list)
        | "Grouping and summing 'chuvas' per KEY" >> beam.CombinePerKey(sum)
        | "Round results (chuvas)" >> beam.Map(rounding)
)

result = (
        ({'chuvas': chuvas, 'dengue': dengue})
        | "Merge Pcollections" >> beam.CoGroupByKey()
        | "Filter empty data" >> beam.Filter(filter_empty_fields)
        | "Unzip elements" >> beam.Map(unzip_elements)
        | "prepare CSV" >> beam.Map(prepare_csv)
)

header = 'UF;ANO;MES;CHUVA;DENGUE'

result | "Creating final file .csv" >> WriteToText("resultado",
                                                   file_name_suffix=".csv",
                                                   header=header)

pipeline.run()
