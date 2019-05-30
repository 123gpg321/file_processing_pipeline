import unittest
from file_processor import *
import os


class TestParquetOutputFile(unittest.TestCase):
	input_file_path_csv = 'input_files/csv/input_file_1_happy_path.csv'
	input_lookup_file_path_csv = 'input_files/csv/lookup_file.csv'

	input_file_path_excel = 'input_files/excel/input_file_1_with_2_worksheets.xlsx'
	input_lookup_file_path_excel = None


	def test_parquet_output_file_generated_by_csv(self):
		output_file_path = 'output_files/csv/output_file.parquet.gzip'
		os.remove(output_file_path)
		output_file_exists_pre = os.path.isfile(output_file_path)
		self.assertEqual(output_file_exists_pre, False)

		FileProcessor().process_file(self.input_file_path_csv, self.input_lookup_file_path_csv)

		output_file_exists_post = os.path.isfile(output_file_path)
		self.assertEqual(output_file_exists_post, True)

	def test_parquet_output_file_generated_by_excel(self):
		output_file_path = 'output_files/excel/output_file.parquet.gzip'
		os.remove(output_file_path)
		output_file_exists_pre = os.path.isfile(output_file_path)
		self.assertEqual(output_file_exists_pre, False)

		FileProcessor().process_file(self.input_file_path_excel, self.input_lookup_file_path_excel)
		
		output_file_exists_post = os.path.isfile(output_file_path)
		self.assertEqual(output_file_exists_post, True)


class TestDataTypeMismatch(unittest.TestCase):
	input_file_path = 'input_files/csv/input_file_3_data_type_mismatch.csv'
	input_lookup_file_path = 'input_files/csv/lookup_file.csv'

	def test_data_type_mismatch_generates_error_file(self):
		error_file_path = 'output_files/error_files/error_file.csv'
		os.remove(error_file_path)
		error_file_exists_pre = os.path.isfile(error_file_path)
		self.assertEqual(error_file_exists_pre, False)

		FileProcessor().process_file(self.input_file_path, self.input_lookup_file_path)

		error_file_exists_post = os.path.isfile(error_file_path)
		self.assertEqual(error_file_exists_post, True)


class TestMissingMandatoryField(unittest.TestCase):
	input_file_path = 'input_files/csv/input_file_4_missing_mandatory_fields.csv'
	input_lookup_file_path = 'input_files/csv/lookup_file.csv'

	def test_missing_mandatory_field_generates_error_file(self):
		error_file_path = 'output_files/error_files/error_file.csv'
		os.remove(error_file_path)
		error_file_exists_pre = os.path.isfile(error_file_path)
		self.assertEqual(error_file_exists_pre, False)

		FileProcessor().process_file(self.input_file_path, self.input_lookup_file_path)

		error_file_exists_post = os.path.isfile(error_file_path)
		self.assertEqual(error_file_exists_post, True)


class TestMatchLookupFile(unittest.TestCase):
	input_file_path = 'input_file_2_non_match_lookup_file.csv'
	input_lookup_file_path = 'input_files/csv/lookup_file.csv'
	
	def test_match_lookup_successful(self):
		print('TODO')

	def test_failed_match_lookup_generates_error_file(self):
		print('TODO')


class TestCentralCompanyIdAppended(unittest.TestCase):
	input_file_path = 'input_files/csv/input_file_1_happy_path.csv'
	input_lookup_file_path = 'input_files/csv/lookup_file.csv'

	def test_central_company_id_appended(self):
		print('TODO')


if __name__ == '__main__':
    unittest.main()