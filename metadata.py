import sys
import csv
file_name = "metadata.txt"

class schema_and_data():
	"""Reads the metadata and stores it in a dict"""

	def __init__(self):
		self.schema = {}
		self.table_data = {}
		self._no_of_tables = 0
		self.table_names = []

	def open_metadata_file(self):
		"""Calls a function to open the file if file exists"""
		try:
			f = open(file_name, 'r')
			self.read_metadata(f)
			
		except IOError:
			sys.exit("Metadata file " + file_name + " not found.")

	def read_metadata(self, file):
		"""Makes a dictionary of tables and its attributes"""
		flag = False

		table_name = ''
		for line in file:
			temp = line.strip()

			if temp == '<begin_table>':
				self._no_of_tables += 1
				flag = True

			elif flag == True:
				table_name = temp
				self.schema[table_name] = []
				self.table_names.append(table_name)
				flag = False

			elif temp != '<end_table>':
				self.schema[table_name].append(temp)

	def open_tabledata_file(self):
		for i in range(0, self._no_of_tables):
			try:
				f = open((self.table_names[i]).strip()+'.csv', 'rb')
				self.read_tabledata(f, self.table_names[i])
			except IOError:
				sys.exit("No file for the given table : " + self.table_names[i] + "' found")

	def read_tabledata(self, file, table_name):
		"""Calls a function to read the tabledata file if file exists"""
		reader = csv.reader(file)
		temp = []
		for i in reader:
			temp.append(i)
		file.close()
		self.table_data[table_name] = temp
