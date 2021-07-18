#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
import sqlalchemy 
import glob


# In[2]:


#connection to mysql db
constr = 'mysql+mysqlconnector://root:mysqlroot@localhost:3306/school_statistics'
engine = sqlalchemy.create_engine(constr,echo=False)


# In[4]:


#dataframe with string values mapped to numbers
str_data_values_in = pd.read_csv('str_data_values_in.csv')

#build dict to reference a numeric value for each possible string value
str_replace = dict(zip(str_data_values_in.str_value, str_data_values_in.number))


# In[19]:


class test_data_prepper:
    
    '''Class that identifies non-numeric items in csv file's data columns. '''
    def __init__(self,file,str_replmts):
        self.file = file
        self.data = pd.read_csv(self.file,dtype=str)
        self.str_replmts = str_replmts
    
    def find_data_cols(self):
        '''find the first column in the dataframe that starts with all'''

        #get columns in dataframe
        cols = list(self.data.columns)
        
        #return first column  that starts with all as this is where the data starts
        first_data = [i for i in cols if i.startswith('ALL')==True][0]
        first_data_ind = list(self.data.columns).index(first_data)
        
        return list(self.data.columns)[first_data_ind:]
    
    def school_index_data(self):
        '''get index data for each school'''
        
        #school index columns
    school_index_cols = ['STNAM', 'FIPST', 'LEAID', 'LEANM', 'NCESSCH']
    
    def get_school_index_2012(self):
        '''Get index columns from testing files for years after 2011.'''        
        self.data_index = self.data[['STNAM', 'FIPST', 'LEAID', 'LEANM', 'NCESSCH','SCHNAM']]
        return self.data_index
    
    def get_school_index_2009(self):
        '''Get index columns from testing files for 2009 files.''' 
        self.data_index = self.data[['STNAM', 'FIPST', 'leaid', 'leanm', 'NCESSCH','schnam09']]
        self.data_index.columns = ['STNAM', 'FIPST', 'LEAID', 'LEANM', 'NCESSCH','SCHNAM']
        return self.data_index
    
    def get_school_index_2010(self):
        '''Get index columns from testing files for  2010 files.''' 
        self.data_index = self.data[['stnam', 'fipst', 'leaid', 'leanm10', 'ncessch','schnam10']]
        self.data_index.columns = ['STNAM', 'FIPST', 'LEAID', 'LEANM', 'NCESSCH','SCHNAM']
        return self.data_index
    
    def get_school_index_2011(self):
        '''Get index columns from testing files for 2011 files.''' 
        self.data_index = self.data[['STNAM', 'FIPST', 'LEAID', 'LEANM', 'NCESSCH','schnam11']]
        self.data_index.columns = ['STNAM', 'FIPST', 'LEAID', 'LEANM', 'NCESSCH','SCHNAM']
        return self.data_index
        
       
    def convert_int(self,value):
        '''try to convert a value to an integer. If it does not work, convert using a dictionary.'''
        
        #first try to convert to float
        try:
            return float(value)
        #if that doesn't work, use the dictionary
        except:
            return float(self.str_replmts[value])
    
    def get_scores_data(self):
        '''This function converts the wide data from the school files to vertical data 
        so that it can be stored in an entity attribute value structure.'''

        #school index columns
        school_index = ['NCESSCH']

        #get list of columns containing test score data
        score_cols = self.find_data_cols()
        
        #columns to be selected for stacking
        cols_used = school_index + score_cols
        cols_used = [col.upper() for col in cols_used]
        
        #convert to uppercase as some files have lower case headers
        self.data.columns = [col.upper() for col in list(self.data.columns)]
        
        #stack data vertically and break columns into different attributes
        score_data = self.data[cols_used].set_index('NCESSCH').stack().reset_index()
        
        data_cols = ['NCESSCH','result_desc','result_value']

        score_data.columns = data_cols

        score_data['dem_grp'] = score_data['result_desc'].str[:3]
        score_data['schl_yr'] = score_data['result_desc'].str[-4:]
        score_data['metric'] = score_data['result_desc'].str[3:-4]
        score_data['metric'] = score_data['metric'].str.strip('_')
        score_data['gradelevel'] = score_data['metric'].str[3:5]
        score_data['metrictype'] = score_data['metric'].str[5:]
        score_data['testtype'] = score_data['metric'].str[:3]
        

        score_data = score_data[['NCESSCH','dem_grp','schl_yr','gradelevel','metrictype','testtype','result_value']]
        
        score_data['result_value'] = score_data['result_value'].apply(self.convert_int)
        
        #perform filter and join steps to remove test instances with zero students
        
        #break into different dataframes numvalid and pct prof metrics
        #convert numeric values to to numerics
        numvalid = score_data[score_data['metrictype']=='NUMVALID']
        numvalid['result_value'] = numvalid['result_value'].astype(float)

        pctprof = score_data[score_data['metrictype']=='PCTPROF']
        pctprof['result_value'] = pctprof['result_value'].astype(float)
        
        #eliminate the test groups with zero participants and join numvalid participants and pct proficient groups
        score_rpt = numvalid[numvalid['result_value'] > 0 ].merge(pctprof,how='left',on=                                                                  ['NCESSCH', 'dem_grp', 'schl_yr', 'gradelevel', 'testtype'])
        
        #drop unnecessary columns
        score_rpt.columns = ['NCESSCH', 'dem_grp', 'schl_yr', 'gradelevel', 'metrictype_x',
       'testtype', 'numvalid', 'metrictype_y', 'pctprof']
        score_rpt = score_rpt.drop(['metrictype_x','metrictype_y'],axis=1)
        
                
        return score_rpt
        


# In[6]:


os.chdir(r'C:\Users\henry\OneDrive\Documents\school_performance_data\test_scores')


# In[11]:


#get list of csv files in directory
csv = glob.glob('*.csv')


# In[23]:


#loop to write all score groups for 10 year period to 1 mysql table

for file in csv:
    data = test_data_prepper(file,str_replace).get_scores_data()
    data.to_sql('test_score_history',if_exists='append',index=False,con=engine,chunksize=10000)
    del data


# In[15]:


mth11 = test_data_prepper('math-achievement-sch-sy2011-12_out.csv').get_school_index_2011()
rla11 = test_data_prepper('rla-achievement-sch-sy-2011-12_out.csv').get_school_index_2011()
mth10 = test_data_prepper('math-achievement-sch-sy2010-11_out.csv').get_school_index_2010()
rla10 = test_data_prepper('rla-achievement-sch-sy-2010-11_out.csv').get_school_index_2010()
mth09 = test_data_prepper('math-achievement-sch-sy2009-10_out.csv').get_school_index_2009()
rla09 = test_data_prepper('rla-achievement-sch-sy2009-10_out.csv').get_school_index_2009()
mth12 = test_data_prepper('math-achievement-sch-sy2012-13_out.csv').get_school_index_2012()
rla12 = test_data_prepper('rla-achievement-sch-sy2012-13_out.csv').get_school_index_2012()
mth13 = test_data_prepper('math-achievement-sch-sy2013-14_out.csv').get_school_index_2012()
rla13 = test_data_prepper('rla-achievement-sch-sy2013-14_out.csv').get_school_index_2012()
mth14 = test_data_prepper('math-achievement-sch-sy2014-15_out.csv').get_school_index_2012()
rla14 = test_data_prepper('rla-achievement-sch-sy2014-15_out.csv').get_school_index_2012()
mth15 = test_data_prepper('math-achievement-sch-sy2015-16_out.csv').get_school_index_2012()
rla15 = test_data_prepper('rla-achievement-sch-sy2015-16_out.csv').get_school_index_2012()
mth16 = test_data_prepper('math-achievement-sch-sy2016-17_out.csv').get_school_index_2012()
rla16 = test_data_prepper('rla-achievement-sch-sy2016-17_out.csv').get_school_index_2012()
mth17 = test_data_prepper('math-achievement-sch-sy2017-18_out.csv').get_school_index_2012()
rla17 = test_data_prepper('rla-achievement-sch-sy2017-18_out.csv').get_school_index_2012()
mth18 = test_data_prepper('math-achievement-sch-sy2018-19-wide_out.csv').get_school_index_2012()
rla18 = test_data_prepper('rla-achievement-sch-sy2018-19-wide_out.csv').get_school_index_2012()


# In[20]:


all_school_index = pd.concat([mth09,mth10,mth11,mth12,mth13,mth14,mth15,mth16,mth17,mth18,                             rla09,rla10,rla11,rla12,rla13,rla14,rla15,rla16,rla17,rla18]).drop_duplicates(subset='NCESSCH',keep='last')


# In[22]:


#for file in csv:
#run a loop to write all of the test history data to the mysql database
data = all_school_index
table = 'school_id'
data.to_sql(table,if_exists='append',index=False,con=engine,chunksize=10000)

