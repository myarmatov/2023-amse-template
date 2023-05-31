from extract_transform_load import extract, transform, load
import pandas as pd
from pandas.testing import assert_frame_equal
import sqlite3

def test_transform():
    df = pd.DataFrame(columns=['column1', 'station', 'column2'])
    df['column1'] = [1, 2, 3]
    df['station'] = ['aaa', 'bbb', 'ccc']
    df['column2'] = [100, 200, 300]
    
    df = transform(df)

    assert df.shape == (3,2)
    print("Transform test passed.")

def test_load():
    df = pd.DataFrame(columns=['column1', 'station', 'column2'])
    df['column1'] = [1, 2, 3]
    df['station'] = ['aaa', 'bbb', 'ccc']
    df['column2'] = [100, 200, 300]

    load(df, 'test_load')

    conn = sqlite3.connect('test_load.sqlite')
    result = pd.read_sql_query('SELECT * FROM test_load',conn)
    conn.close()

    assert_frame_equal(result, df)
    print("Load test passed.")

if __name__ == '__main__':
    test_transform()
    test_load()
