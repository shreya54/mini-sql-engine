import sys
import re
from metadata import schema_and_data

class query_processing(schema_and_data):
	"""Processes the user's query"""

	def __init__(self, query):
		
		# inheriting parent class' init functions
		schema_and_data.__init__(self)

		# calling functions from parent class
		self.open_metadata_file()
		self.open_tabledata_file()

		#initializing new variables specific to this class
		self.query = self.format_string(query)
		self.operators = ['<', '>', '=']
		self.FUNCTIONS = ['distinct', 'max', 'sum', 'avg', 'min']
		self.what_to_select = []
		self.columns = []
		self.columns_to_remove = []
		self.distinct_process = []
		self.function_process = {}
		self.mapping = {}

		#calling functions from current class to start executing the query
		self.syntax_error_handling()
		self.start_process()
		
	def format_string(self, str):
		return re.sub(' +', ' ', str).strip()

	def syntax_error_handling(self):
		if 'select' not in (self.query).lower():
			sys.exit("Syntax Error : No SELECT clause given")

		if 'from' not in (self.query).lower():
			sys.exit("Syntax Error : No FROM clause given")

		if self.query.lower().count('from') > 1:
			sys.exit("Syntax Error : More than one FROM statement")
		
		if self.query.lower().count('select') > 1:
			sys.exit("Syntax Error : More than one SELECT statement")
		
		if self.query.lower().count('where') > 1:
			sys.exit("Syntax Error : More than one WHERE statement")
		
	def start_process(self):
		from_index = (self.query.lower()).find('from')

		fromsplit = []
		fromsplit.append(self.format_string(self.query[:from_index]))
		fromsplit.append(self.format_string(self.query[from_index + len('from '):]))

		self.what_to_select = self.format_string(fromsplit[0][len('select'):])

		if (self.query.lower().count('where')) != 0:
			where_index = (fromsplit[1].lower()).find('where')
			wheresplit = []
				
			wheresplit.append(self.format_string(fromsplit[1][where_index + len('where'):]))
			table_to_select = self.format_string(str(wheresplit[0]))
			conditions = self.format_string(str(wheresplit[1]))

		else:
			table_to_select = self.format_string(str(fromsplit[1]))
			conditions = ""

		# print self.what_to_select, table_to_select, conditions

		if self.what_to_select == '':
			sys.exit("Error : Nothing given to select")

		if len(table_to_select) == 0:
			sys.exit("Error : No table specified")

		table_to_select = table_to_select.split(',')
		
		for i in range(0, len(table_to_select)):			# Checking if the table exists in the database
			table_to_select[i] = self.format_string(table_to_select[i])
			if table_to_select[i] not in (self.schema).keys():
				sys.exit("No such table \'" + table_to_select[i] + "\' exists!")

		self.what_to_select = self.what_to_select.split(',')
		self.select()

		if len(self.distinct_process) and len(self.function_process):
			sys.exit("DISTINCT and aggregate functions cannot be used together")

		if len(self.distinct_process) != 0:
			if len(table_to_select) == 1:
				answer = self.table_data[table_to_select[0]]
				if len(conditions) != 0:
					answer = self.records_satisfying_where_single_table(conditions, table_to_select[0])
				if len(self.distinct_process) != 1:
	 				self.multiple_distincts(table_to_select, answer)
				elif len(self.distinct_process) == 1:
					self.single_distinct(table_to_select, answer)
			else:
				sys.exit('DISTINCT clause works only on one table')

		elif len(self.function_process) != 0:
			answer = self.table_data[table_to_select[0]]
			if len(conditions) != 0:
				answer = self.records_satisfying_where_single_table(conditions, table_to_select[0])
			self.aggregate_function(table_to_select, answer)

		elif len(conditions) == 0:
			if len(table_to_select) == 1:
				self.just_select_single_table(table_to_select[0], self.table_data[table_to_select[0]])
			else:
				self.just_select_multiple_table(table_to_select, 1)

		elif len(conditions) > 0:
			if len(table_to_select) == 1: 
				answer = self.records_satisfying_where_single_table(conditions, table_to_select[0])
				self.just_select_single_table(table_to_select[0], answer)
			else:
				self.join_tables(table_to_select, conditions)

	def select(self):
		"""Process the select part of the query"""
		col = '';
		for i in self.what_to_select:
			i = self.format_string(i)
			flag = False
			
			for j in self.FUNCTIONS:
				if j + '(' in i.lower():
					flag = True
					if ')' not in i.lower():
						sys.exit("Syntax Error : Expected \')\'")
					else:
						col = i.strip(')')
						col = col[len(j)+1 :]
					if j == 'distinct':
						self.distinct_process.append(col)
					else:
						if j not in self.function_process.keys():
							self.function_process[j] = []
						self.function_process[j].append(col)
			if not flag:
				if i != '':
					self.columns.append(i.strip())

	def just_select_single_table(self, table_to_select, table_data):
		"""When there is no WHERE condition at all and single table to select from"""

		if len(self.columns) == 1 and self.columns[0] == '*':
			self.columns = self.schema[table_to_select]
		
		for i in self.columns:
			table, column = self.search_column(i, [table_to_select])

		for i in range(len(self.columns)):
			print '%9s' % self.columns[i],
		print

		for i in table_data:
			for j in self.columns:
				table, column = self.search_column(j, [table_to_select])
				print '%9s' % i[self.schema[table].index(column)],
			print

	def just_select_multiple_table(self, table_to_select, flag):
		"""When there in no WHERE condition but two tables to select from"""
		if len(self.columns) == 1 and self.columns[0] == '*':
			self.columns = []
			for i in table_to_select:
				for j in range(len(self.schema[i])):
					self.columns.append(i + '.' + self.schema[i][j])

		for i in self.columns:
			print '%9s' % i,
			table, column = self.search_column(i, table_to_select)
		print

		for i in self.table_data[table_to_select[0]]:	
			row = []
			ans = ''
			for j in self.columns:
				table, column = self.search_column(j, table_to_select)
				if table == table_to_select[0]:
					ans += '%9s' % i[self.schema[table].index(column)]
					if flag == 2:
						row.append(i[self.schema[table].index(column)])

			if len(table_to_select) > 1:
				for k in self.table_data[table_to_select[1]]:
					temp = ans
					for l in self.columns:
						table, column = self.search_column(l, table_to_select)
						if table == table_to_select[1]:
							if flag == 2:
								row.append(i[self.schema[table].index(column)])
							temp +='%9s' % k[self.schema[table].index(column)]
					if len(table_to_select) > 2:
						for m in self.table_data[table_to_select[2]]:
							temp2 = temp
							for n in self.columns:
								table, column = self.search_column(n, table_to_select)
								if table == table_to_select[2]:
									if flag == 2:
										row.append(i[self.schema[table].index(column)])
									temp2 +='%9s' % m[self.schema[table].index(column)]		
							if flag == 2:
								self.table_data['temp'].append(row)
							else:
								print temp2
					else:
						if flag == 2:
							self.table_data['temp'].append(row)
						else:
							print temp
			else:
				if flag == 2:
					self.table_data['temp'].append(row)
				else:
					print ans

	def single_distinct(self, table_to_select, table_data):
		"""Process the queries with DISTINCT"""
		header = ''
		column_data = {}
		data = []

		table_needed, column = self.search_column(self.distinct_process[0], table_to_select)
		header += table_needed + '.' + column + ', '
		print '%9s' % (table_needed + '.' + column),
		for i in self.columns:
			print '%9s' %i,
		print

		remaining_data = []

		for j in table_data:
			value = j[self.schema[table_needed].index(self.distinct_process[0])]
			# print value
			temp_remaining_data = []
			if value not in data:
				data.append(value)
				print '%9s' %(value),
				for k in self.columns:
					temp_remaining_data.append(j[self.schema[table_needed].index(k)])
					print '%9s' %j[self.schema[table_needed].index(k)],
				print
				remaining_data.append(temp_remaining_data)


			# SUPPOSE IM HAVING A QUERY SELECT DISTINCT(A), B FROM TABLE1, 
			# I NEED TO TAKE RECORDS WHICH HAVE REDUNDANT A's BUT BASICALLY DIFFERENT B'S
			# THE FOLLOWING CODE DOES THAT
			else:
				count = 0
				flag = False
				if len(self.columns) > 0:
					for l in self.columns:
						tempvar = j[self.schema[table_needed].index(l)]
						temp_remaining_data.append(j[self.schema[table_needed].index(l)])
						if count == 0:
							for k in range(len(remaining_data)):
								if tempvar == remaining_data[k][count]:
									flag = True
						count += 1
					if flag == False:
						data.append(value)
						print '%9s' %value,
						remaining_data.append(temp_remaining_data)
						for i in temp_remaining_data:
							print '%9s' %i,
						print

	def multiple_distincts(self, table_to_select, table_data):
		data = []
		for i in self.distinct_process:
			table_needed, column = self.search_column(i, table_to_select)
			for j in table_data:
				value = j[self.schema[table_needed].index(i)]
				if value not in data:
					data.append(value)
					print '%9s' % value

	def records_satisfying_where_single_table(self, conditions, table_to_select):
		"""WHERE clause on a single table"""
		conditions = conditions.split(' ')
		if len(self.columns) == 1 and self.columns[0] == '*':
			self.columns = self.schema[table_to_select]

		for i in range(len(self.columns)):
			table, column = self.search_column(self.columns[i], [table_to_select])

		final_list = []
		for i in self.table_data[table_to_select]:
			evaluated = self.evaluator_constructor(conditions, table_to_select, i, 1)
			ans = []
			for j in range(len(self.schema[table_to_select])):
				try:
					if eval(evaluated):
						ans.append(i[j])
				except NameError:
					sys.exit("Condition is invalid")
			if len(ans):
				final_list.append(ans)
		return final_list

	def join_tables(self, table_to_select, conditions):
		table_to_select.reverse()

		# self.just_select_multiple_table(table_to_select, 2)
		fileData = []

		for i in self.table_data[table_to_select[0]]:
			for j in self.table_data[table_to_select[1]]:
				fileData.append(j+i)

		self.schema["sample"] = []
		for i in self.schema[table_to_select[1]]:
			self.schema["sample"].append(table_to_select[1] + '.' + i)

		for i in self.schema[table_to_select[0]]:
			self.schema["sample"].append(table_to_select[0] + '.' + i)

		self.schema["test"] = self.schema[table_to_select[1]] + self.schema[table_to_select[1]]

		table_to_select.remove(table_to_select[0])
		table_to_select.remove(table_to_select[0])
		table_to_select.insert(0, "sample")

		if len(self.columns) == 1 and self.columns[0] == '*':
			self.columns = self.schema[table_to_select[0]]

		header = []
		for i in self.columns:
			header.append(i)

		a = conditions.split(" ")

		check = 0

		ans = []
		for data in fileData:
			temp = []
			evaluated = self.evaluator_constructor(a, table_to_select[0], data, 2)
			for col in self.columns:
				if eval(evaluated):
					check = 1
					if '.' in col:
						temp.append(data[self.schema[table_to_select[0]].index(col)])
						# ans += '%9s' % data[self.schema[table_to_select[0]].index(col)],
					else:
						temp.append(data[self.schema["test"].index(col)])
						# ans += '%9s' % data[self.schema["test"].index(col)],
			if check == 1:
				check = 0
				ans.append(temp)

		for i in range(len(header)):
			if i not in self.columns_to_remove:
				print '%9s' % header[i],
		print
		for i in range(len(ans)):
			for j in range(len(ans[i])):
				if j not in self.columns_to_remove:
					print '%9s' % ans[i][j],
			print
		
		del self.schema["sample"]


	def aggregate_function(self, table_to_select, table_data):
		header = []
		result = []
		for i in self.function_process.keys():
			for b in range(len(self.function_process[i])):
				column_name = (self.function_process[i])[b]
				
				table, column = self.search_column(column_name, table_to_select)

				data = []
				header.append('%9s' %(i + '(' + table + '.' + column + ')')),

				for j in table_data:
					data.append(int(j[self.schema[table].index(column)]))

				if i.lower() == 'max':
					result.append('%9s' % (str(max(data))))
				elif i.lower() == 'min':
					result.append('%9s' % (str(min(data))))
				elif i.lower() == 'sum':
					result.append('%9s' % (str(sum(data))))
				elif i.lower() == 'avg':
					result.append('%9s' % (str(float(sum(data)) / len(data))))

		for i in header:
			print '%15s' % i,
		print
		for i in result:
			print '%15s' %i,
		print

	def evaluator_constructor(self, conditions, table_to_select, data, flag):
		evaluated = ""
		count = 0
		for i in conditions:
			column = i
			#the next two lines are to handle the case IF table1.A is specified instead of just A in the conditions
			if flag == 1 and '.' in i:
				table, column = self.search_column(i, table_to_select)
			if i == '=':
				evaluated += i*2
			elif i == '<>':
				evaluated += '!='
			elif column in self.schema[table_to_select]:
				count += 1
				evaluated += data[self.schema[table_to_select].index(column)]
				if count == 2 or count == 4:
					if column in self.columns and self.columns.index(column) not in self.columns_to_remove:
						self.columns_to_remove.append(self.columns.index(column))
			elif flag == 2 and column in self.schema['test']:
				evaluated += data[self.schema['test'].index(column)]
			elif i.lower() == 'and' or i.lower() == 'or':
				evaluated += ' ' + i.lower() + ' '
			else:
				evaluated += i
		return evaluated

	def search_column(self, column, table_to_select):
		if '.' in column:
			table, column = column.split('.')
			table = self.format_string(table)
			column = self.format_string(column)

			if table not in table_to_select:
				sys.exit("No such table \'" + table + "\' exists.")
			return table, column
		count = 0
		table_needed = ''

		# print 'printing table_to_select from search_column', table_to_select
		for i in table_to_select:
			if column in self.schema[i]:
				count += 1
				if count > 1:
					sys.exit("Ambiguous column name \'" + column + "\'")
				table_needed = i
		if count == 0:
			sys.exit("No such column \'" + column + "\'")
		return table_needed, column
