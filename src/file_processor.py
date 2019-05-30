import pandas as pd
import sys
import csv
import traceback

"""
Run in REPL:

FileProcessor().process_file(input_file_path, input_lookup_file_path)


Example params:

input_file_path = 'input_files/csv/input_file_3_data_type_mismatch.csv'
input_file_path = 'input_files/csv/input_file_4_missing_mandatory_fields.csv'
input_file_path = 'input_files/csv/input_file_1_happy_path.csv'
input_lookup_file_path = 'input_files/csv/lookup_file.csv'

input_file_path = 'input_files/excel/input_file_1_with_2_worksheets.xlsx'
input_lookup_file_path = None
"""


class FileProcessor:

	def generate_output_file(self, validated_and_matching_df, mime_type):
		output_file_path = 'output_files/{0}/output_file.parquet.gzip'.format(mime_type)
		validated_and_matching_df.to_parquet(output_file_path, compression='gzip')
		print("Done")

	def match_values_with_lookup_file_and_append_id(self, df, lookup_df):
		"""
		Matching of dataframes df and lookup_df occurs here.
		If successful return True, else raise exception and generate error file.
		Pass the row and col number of error to #generate_error_file() in except block.
		You could bind row and col number to instance class as attributes in matching loop,
		when non match occurs and pass them as arguments this way to except block to then
		pass to #generate_error_file().

		Appending of CentralCompanyId to df would also accur here.
		"""

		return df

	def generate_error_file(self, exception, row=None, col=None):
		csv_data = [['error', 'row', 'col'], [exception, row, col]]

		with open('output_files/error_files/error_file.csv', 'w') as csv_file:
		    writer = csv.writer(csv_file)
		    writer.writerows(csv_data)

		csv_file.close()	

	def process_csv_file(self, input_file_path, input_lookup_file_path):
		df = pd.read_csv(input_file_path,
				 dtype={'CustomParam1': float,
            			'CustomParam2': float,
            			'CustomParam3': float,
            			'CustomParam4': float,
            			'CustomParam5': float,
            			'CustomParam6': float,
            			'CustomParam7': float,
            			'Country': str,
            			'CompanyId': int,
            			'Currency': str,
            			'CompanyName': str})

		# validation of mandatory columns
		df['CompanyName']
		df['Country']
		df['Currency']
		df['CompanyId']

		lookup_df = pd.read_csv(input_lookup_file_path)
		return df, lookup_df

	def process_file(self, input_file_path, input_lookup_file_path):
		try:
			if '.csv' in input_file_path:
				mime_type = 'csv'
				df, lookup_df = self.process_csv_file(input_file_path, input_lookup_file_path)
			elif '.xlsx' in input_file_path:
				mime_type = 'excel'
				dfs = pd.read_excel('input_files/excel/input_file_1_with_2_worksheets.xlsx', sheet_name=None)
				df = dfs['InputData']
				lookup_df = dfs['LookupData']

			validated_and_matching_df = self.match_values_with_lookup_file_and_append_id(df, lookup_df)
			self.generate_output_file(validated_and_matching_df, mime_type)

		except Exception as e:
			type_, value_, traceback_ = sys.exc_info()
			exception = traceback.format_exception(type_, value_, traceback_)
			self.generate_error_file(exception)