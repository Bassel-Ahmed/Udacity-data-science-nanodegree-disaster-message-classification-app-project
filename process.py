import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


def load_data(messages_filepath, categories_filepath):

    """Load and merge messages and categories datasets
    
    Args:
    messages_filepath: string. Filepath for csv file containing messages dataset.
    categories_filepath: string. Filepath for csv file containing categories dataset.
       
    Returns:
    df: dataframe. Dataframe containing merged content of messages and categories datasets.
    """
    
    # Load messages dataset
    messages = pd.read_csv(messages_filepath)
    
    # Load categories dataset
    categories = pd.read_csv(categories_filepath)
    categories.drop_duplicates(inplace=True)
    messages.drop_duplicates(inplace=True)
    # Merge datasets
    df = messages.merge(categories, how = 'inner', on = ['id'])
    
    return df

def clean_data(data):
            data.drop(['original'],axis=1,inplace =True)
            data['genre'].astype('category')
            names=[]
            l=data['categories'][15].strip().split(';')
            for name in l:
                        names.append(name.split('-')[0])
            classes=data['categories'].str.split(';',expand=True)
            classes.columns=names
            for name in names:
                        classes[name]=classes[name].apply(lambda x: x.split('-')[1])
            for column in classes.columns:
                        classes[column]=pd.to_numeric(classes[column])
            df=pd.concat([data,classes],axis=1)
            df=df[df['related']!=2]
            df.drop(['categories','child_alone'],axis=1,inplace=True)
            return df
def save_data(df, database_filename):
    """Save cleaned data into an SQLite database.
    
    Args:
    df: dataframe. Dataframe containing cleaned version of merged message and 
    categories data.
    database_filename: string. Filename for output database.
       
    Returns:
    None
    """
    engine = create_engine('sqlite:///' + database_filename)
    df.to_sql('Messages', engine, index=False, if_exists='replace')
    





