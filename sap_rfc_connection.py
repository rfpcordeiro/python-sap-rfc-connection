import pandas as pd
import time
import sys
import constants as c
from pyrfc import Connection 

def rfc_func_desc(dict_sap_con, func_name):
    '''consult the RFC description and needed input fields
    
    Parameters
    ----------
    dict_sap_con : dict
        key to create connection with SAP, must contain: user, passwd, ashost, sysnr, client
    func_name : str
        name of the function that you want to verify
        
    Returns
    -------
    funct_desc : pyrfc.pyrfc.FunctionDescription
        RFC functional description object
    '''
    print(f'{time.ctime()}, Start getting function description from RFC')
    print(f'{time.ctime()}, Start SAP connection')
    # create connection with SAP based on data inside the dict
    with Connection(**dict_sap_con) as conn:
        print(f'{time.ctime()}, SAP connection stablished')
        # get data from the desired RFC function
        funct_desc = conn.get_function_description(func_name)
        # display the information about the RFC to user
        display(funct_desc.parameters[0],funct_desc.parameters[0]['type_description'].fields)
        # return it as a variable
        return funct_desc
    # how the whole command is inside the 'with' when it ends the connection with SAP is closed
    print(f'{time.ctime()}, SAP connection closed')
    print(f'{time.ctime()}, End getting function description from RFC')

def get_rfc_parameters(dict_sap_con, func_name):
    '''create a list with the fields that the RFC need to insert data and the respective data types
    
    Parameters
    ----------
    dict_sap_con : dict
        key to create connection with SAP, must contain: user, passwd, ashost, sysnr, client
    func_name : str
        name of the function that you want to verify
        
    Returns
    -------
    lst_res : list
        list of dictionaries with field names and data types used in RFC
    '''
    print(f'{time.ctime()}, Start getting fields and types from RFC')
    # create an empty list that is going to recive the result
    lst_res = []
    print(f'{time.ctime()}, Start SAP connection')
    # create connection with SAP based on data inside the dict
    with Connection(**dict_sap_con) as conn:
        print(f'{time.ctime()}, SAP connection stablished')
        # get data from the desired RFC function
        funct_desc = conn.get_function_description(func_name)
        print(f'{time.ctime()}, RFC description loaded')
    # how the whole command is inside the 'with' when it ends 
    # the connection with SAP is closed
    print(f'{time.ctime()}, SAP connection closed')
    # for each field specified in the type description
    for field in funct_desc.parameters[0]['type_description'].fields:
        # create the dict with name of the field and the data type needed
        dict_return = {'name': field['name'],'field_type': field['field_type']}
        # insert it to the result list
        lst_res.append(dict_return)
    print(f'{time.ctime()}, RFC description formatted')
    print(f'{time.ctime()}, End getting fields and types from RFC')
    return lst_res

def check_input_format(df, lst_param):
    '''Check if the data frame that is going to be used to insert data to SAP has the same size, column names and data types, if not stop code execution
    
    Parameters
    ----------
    df : pandas.DataFrame
        data frame that is going to be used to insert data to SAP
    lst_param : list
        list of dictionaries with field names and data types used in RFC
        
    Returns
    -------
    None
    '''
    print(f'{time.ctime()}, Start data frame format check')
    # create the error indicator as false
    err_ind = False
    # check if the data frame has the same columns quantity as the RFC needs
    if len(df.columns) == len(lst_param):
        print(f'{time.ctime()}, The data frame has the same column quantity as desired by RFC specification')
        # for each column in the data frame
        for col in df.columns:
            # get the data type of the column
            df_col_typ = str(df[col].dtype)
            # search in the list for the dict with the same name column name
            rfc_col_dict = next(item for item in lst_param if item["name"] == col)
            # get the value from this dict key 
            rfc_col_typ = rfc_col_dict.get('field_type')
            # check if the data types of RFC and data frame is the same
            # string check
            if (rfc_col_typ == 'RFCTYPE_CHAR') and (df_col_typ != 'object'):
                err_ind = True
            # numeral check
            if(rfc_col_typ == 'RFCTYPE_BCD') and ((df_col_typ != 'float64') and (df_col_typ != 'int64')):
                err_ind = True
            # if one of them is different
            if err_ind:
                # print the name of the column and needed data type
                print(f'{time.ctime()}, Different data types in column {col},'\
                      +f'needed is {rfc_col_typ} and now is {df_col_typ}')
                # stop code execution
                sys.exit()
            else:
                # everything is ok
                print(f'{time.ctime()}, {col} format validated')
        print(f'{time.ctime()}, data frame format validated')
    else:
        # if the data frame and RFC aren't the same size means that we are trying to send 
        # or more data than needed or less, so tell which case it is
        if len(df.columns) > len(lst_param):
            print(f'{time.ctime()}, There are more columns in data frame than needed')
        else:
            print(f'{time.ctime()}, There are less columns in data frame than needed')
        # print the needed columns
        print(f'{time.ctime()}, Columns needed:')
        for item in lst_param:
            print(item['name'])
        # stop code execution
        sys.exit()
    print(f'{time.ctime()}, End data frame format check')
        
def insert_df_in_sap_rfc(df, dict_sap_con, func_name, rfc_table):
    '''Ingest data that is in a data frame in SAP using a defined RFC
    
    Parameters
    ----------
    df : pandas.DataFrame
        data frame that is going to be used to insert data to SAP
    dict_sap_con : dict
        dictionary with SAP logon credentials (user, passwd, ashost, sysnr, client)
    func_name : string
        name of the function that you want to remotelly call
    rfc_table : string
        name of the table which your RFC populates
        
    Returns
    -------
    lst_res : list
        list of dictionaries with field names and data types used in RFC
    '''
    print(f'{time.ctime()}, Start data ingestion to SAP process')
    # create an empty list that is going to recive the result
    lst_res = []
    # get the quantity of rows of the data frame
    rows_qty = len(df)
    # define the number of execution, getting the entire part of the division and 
    # adding 1 to it, to execute the last rows that don't achieve the quantity of 
    # an extra execution
    iter_qty = (rows_qty // c.rows_per_exec) + 1
    print(f'{time.ctime()}, Start SAP connection')
    # create connection with SAP based on data inside the dict
    with Connection(**dict_sap_con) as conn:
        print(f'{time.ctime()}, SAP connection stablished')
        # for each iteration
        for i in range(iter_qty):
            # define the first and last row for this execution
            f_r = i*c.rows_per_exec
            l_r = min((i+1)*c.rows_per_exec, rows_qty)
            # define an auxiliar data frame with only the rows of this iteration
            df_aux = df.iloc[f_r:l_r]
            print(f'{time.ctime()}, Rows defined')
            # convert this data frame to a json format, oriented by records
            # this is the needed format to do a multirow input with a RFC
            # by last all the json data must be inside of a list
            lst_dicts_rows = eval(df_aux.to_json(orient='records'))
            # once we have the desired rows well formatted we must tell for
            # which table we are going to insert it
            dict_insert = {rfc_table: lst_dicts_rows}
            print(f'{time.ctime()}, RFC input format applied')
            print(f'{time.ctime()}, Start sending rows {f_r} to {l_r-1}')
            # with everything set just call the RFC by its name 
            # and pass the connection dict
            try:
                result = conn.call(func_name, **dict_insert)
                exec_ind = True
            except:
                result = None
                exec_ind = False
            print(f'{time.ctime()}, Rows {f_r} to {l_r-1} sent')
            # save the row's numbers, execution indicator and the result of the call in the list
            # as a dict
            lst_res.append({'row':f'{f_r}_{l_r-1}', 'exec_ind':exec_ind, 'rfc_result':result})
    # how the whole command is inside the 'with' when it ends the connection with SAP is closed
    print(f'{time.ctime()}, SAP connection closed')
    print(f'{time.ctime()}, End data ingestion to SAP process')
    return lst_res
        
def df_to_sap_rfc(df, dict_sap_con, func_name, rfc_table):
    '''ingest the data that is inside of a data frame in SAP using a defined RFC, checking if the dataframe has the same size, column names and data types
    
    Parameters
    ----------
    df : pandas.DataFrame
        data frame that is going to be used to insert data to SAP
    dict_sap_con : dict
        dictionary with SAP logon credentials (user, passwd, ashost, sysnr, client)
    func_name : string
        name of the function that you want to remotelly call
    rfc_table : string
        name of the table which your RFC populates
        
    Returns
    -------
    None
    '''
    # get needed parameters from RFC
    lst_param = get_rfc_parameters(dict_sap_con, func_name)
    # check data frame input
    check_input_format(df, lst_param)
    # insert data
    lst_res = insert_df_in_sap_rfc(df, dict_sap_con, func_name, rfc_table)
