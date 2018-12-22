import synapseclient
import pandas as pd
import mock
from nose.tools import assert_raises, assert_equals
import genie

class test_update_database:
	
	def setup(self):
		self.databasedf = pd.DataFrame({'UNIQUE_KEY':['test1','test2','test3'],
							  "test":['test1','test2','test3'],
							  "foo":[1,2,3],
							  "baz":[float('nan'),float('nan'),float('nan')]})
		self.databasedf.index = ['1_3','2_3','3_5']
	
	def test_append_rows_to_database(self):

		new_datadf = pd.DataFrame({'UNIQUE_KEY':['test1','test2','test3','test4'],
							  "test":['test1','test2','test3','test4'],
							  "foo":[1,2,3,4],
							  "baz":[float('nan'),float('nan'),float('nan'),3.2]})
		expecteddf = pd.DataFrame({'test':['test4'],
										  'foo':[4],
										  'baz':[3.2],
										  'ROW_ID':[float('nan')],
										  'ROW_VERSION':[float('nan')]})
		append_rows = genie.process_functions._append_rows(new_datadf, self.databasedf, 'UNIQUE_KEY')
		assert append_rows.equals(expecteddf)


	def test_update_rows_to_database(self):
		new_datadf = pd.DataFrame({'UNIQUE_KEY':['test1','test2','test3'],
							  "test":['test','test2','test3'],
							  "foo":[1,3,3],
							  "baz":[float('nan'),5,float('nan')]})

		expecteddf = pd.DataFrame({"test":['test','test2','test3'],
							  "foo":[1,3,3],
							  "baz":[float('nan'),5,float('nan')],
							  'ROW_ID':['1','2','3'],
							  'ROW_VERSION':['3','3','5']})
		update_rows = genie.process_functions._update_rows(new_datadf, self.databasedf, 'UNIQUE_KEY')
		assert update_rows.equals(expecteddf)

	def test_delete_rows_to_database(self):
		new_datadf = pd.DataFrame({'UNIQUE_KEY':['test1'],
							  "test":['test1'],
							  "foo":[1],
							  "baz":[float('nan')]})
		expecteddf = pd.DataFrame({0:['2','3'],
							  	   1:['3','5']})
		delete_rows = genie.process_functions._delete_rows(new_datadf, self.databasedf, 'UNIQUE_KEY')
		assert delete_rows.equals(expecteddf)

		# syn = mock.create_autospec(synapseclient.Synapse) 
		# return_table = synapseclient.Table("foo")
		# with  mock.patch.object(syn, "store", return_value=return_table) as patch_syn_store:
		# 	genie.process_functions._append_rows(syn,databasedf, new_datadf,  database_synid, uniqueKeyCols)
			
		# 	patch_syn_store.assert_called_once_with(synapseclient.Table(databaseSynId, SCRIPT_DIR))
