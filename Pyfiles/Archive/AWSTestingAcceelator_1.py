import math
import os
import base64
import webbrowser
import time
import datetime
# from datetime import datetime
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import Pyfiles.ReadConfigData as rdconfig , Pyfiles.QueryGenerator as QG, Pyfiles.Databricks_conn as dbricks, Pyfiles.SrcToLake as SL,Pyfiles.MetaDataValidation as MD, Pyfiles.ETLValidation as ETL , Pyfiles.JiraGenerator as JIRA



external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets = external_stylesheets, suppress_callback_exceptions = True)
app.layout = html.Div([
    html.Div([
        html.H4('Welcome to AWS Testing Tool', style = {'text-align' : 'center'}),
        html.Div([
        html.H5('Select Configuration File', style = {'width': '88.8%', 'display': 'inline-block'}),
        html.A(html.Button('Clear', id = 'refresh_button', n_clicks = 0, style = {'width': '10%', 'display': 'inline-block'}), href = '/')]),
        dcc.Upload(
            id = 'upload-file',
            children = html.Div(["Upload Configuration File"]
                               ),
            style = {
                "width": "98%", "height": "60px", "lineHeight": "60px", "borderWidth": "1px", "borderStyle": "dashed", "borderRadius": "5px", "textAlign": "center","margin": "10px",
            }
        ),
        html.Div(id = 'level_1'),
        html.Div(id = 'level_2'),
        html.Div(id = 'level_3'),
        html.Div(id = 'level_4'),
        html.Div(id = 'level_5'),
        html.Div(id = 'generation_dialog_box'),
        html.Div(id = 'execution_dialog_box'),
        html.Div(id = 'execution_dialog_box_1'),
        html.Br(),
        html.Div([
            html.H5('Message Board', style={'width': '88.8%', 'display': 'inline-block'}),
            html.Button('Export', id = 'export_button', n_clicks = 0, style={'width': '10%', 'display': 'inline-block'})
        ]),
        html.Div(id = 'test_case_generation'),
        html.Div(id = 'lake_hist_query'),
        html.Div(id = 'hub_query'),
        html.Div(id = 'hub_to_mart_query'),
        html.Div(id = 'raw_lake_data_validation'),
        html.Div(id = 'source_lake_data_validtion'),
        html.Div(id = 'lake_lake_hist_validation'),
        html.Div(id = 'lake_hist_hub_validation'),
        html.Div(id = 'hub_to_mart_validation'),
        html.Div(id = 'source_raw_data_validation'),
        html.Div(id = 'mart_table_validation'),
        html.Div(id = 'hub_table_validation'),
        html.Div(id = 'lake_hist_table_validation'),
        html.Div(id = 'lake_table'),
        html.Div(id = 'export_dialog'),
        dcc.Store(id = 'config_input'),
        dcc.Store(id = 'test_case_generation_export_data'),
        dcc.Store(id = 'lake_hist_query_export_data'),
        dcc.Store(id = 'hub_query_export_data'),
        dcc.Store(id = 'hub_to_mart_query_export_data'),
        dcc.Store(id = 'raw_lake_data_validation_export_data'),
        dcc.Store(id = 'source_lake_data_validtion_export_data'),
        dcc.Store(id = 'lake_lake_hist_validation_export_data'),
        dcc.Store(id = 'lake_hist_hub_validation_export_data'),
        dcc.Store(id = 'hub_to_mart_validation_export_data'),
        dcc.Store(id = 'source_raw_data_validation_export_data'),
        dcc.Store(id = 'mart_table_validation_export_data'),
        dcc.Store(id = 'hub_table_validation_export_data'),
        dcc.Store(id = 'lake_hist_table_validation_export_data'),
        dcc.Store(id = 'lake_table_export_data')
        ])
])




@app.callback(
    Output('export_dialog', 'children'),
    Input('export_button', 'n_clicks'),
    State('config_input', 'data'),
    State('test_case_generation_export_data', 'data'),
    State('lake_hist_query_export_data', 'data'),
    State('hub_query_export_data', 'data'),
    State('hub_to_mart_query_export_data', 'data'),
    State('raw_lake_data_validation_export_data', 'data'),
    State('source_lake_data_validtion_export_data', 'data'),
    State('lake_lake_hist_validation_export_data', 'data'),
    State('lake_hist_hub_validation_export_data', 'data'),
    State('hub_to_mart_validation_export_data', 'data'),
    State('source_raw_data_validation_export_data', 'data'),
    State('mart_table_validation_export_data', 'data'),
    State('hub_table_validation_export_data', 'data'),
    State('lake_hist_table_validation_export_data', 'data'),
    State('lake_table_export_data', 'data'))
def export_message_board(n_clicks, config_dict, a, b, c, d, e, f, g, h, i, j, k, l, m, n):



    # ------------------------------------------------------------------------------------------------
    # Function exports the message board to a file if export button is clicked.
    # ------------------------------------------------------------------------------------------------



    if n_clicks > 0:
        _logs_to_store = []
        for __log_from_store in [a, b, c, d, e, f, g, h, i, j, k, l, m, n]:
            if __log_from_store != None:
                _logs_to_store.append(__log_from_store.replace('<BR>', ' '))

        timestamp_str = str(datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S').replace('/', '').replace(':', '').replace(' ', '-'))
        output_folder_path = str(config_dict['Output folder path']).strip().replace('\\', '/')
        
        
        final_path = output_folder_path + '/logs/'
        if not os.path.exists(final_path):
            os.makedirs(final_path)
        final_path = final_path + 'logs-' + timestamp_str + '.txt'
        with open(str(final_path), "a") as _f:
            for item in _logs_to_store:
                _f.write(item + '\n')

        return([dcc.ConfirmDialog(
            id = 'export_dialog_1',
            message = f"Logs stored in \n{final_path}",
            displayed = True)])



@app.callback(
    Output('config_input', 'data'),
    Input('upload-file', 'contents'))
def fetch_config_dict(contents):



    # ------------------------------------------------------------------------------------------------
    # Function fetches the data from uploaded config file into a python dictionary.
    # ------------------------------------------------------------------------------------------------

    
    
    if contents != None:
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string).decode('utf-8').replace('\r', '').replace('\"', '').split('\n')
        decoded = [i.strip() for i in decoded if not (i.startswith('#') or i == '')]
        final_dict = {}
        for item in decoded:
            _l = item.split(':', 1)

            if _l[0].lower() in ['source data file name', 'raw data file name', 'lake table name']:
                _l[1] = _l[1].split(',')

            final_dict[_l[0]] = _l[1]
        return(final_dict)
    return(None)



@app.callback(
    Output('upload-file', 'children'),
    Input('config_input', 'data'))
def indicate_upload_file(_configdata):



    # ------------------------------------------------------------------------------------------------
    # Function indicates whether input file is uploaded or not by changing the text within the textbox.
    # ------------------------------------------------------------------------------------------------



    if _configdata != None:

        return(html.Div(["Configuration File Uploaded"]))
    return(html.Div(["Upload Configuration File"]))



@app.callback(
    Output('level_1', 'children'),
    Input('upload-file', 'contents'))
def update_radio_buttons(_configdata):



    # ------------------------------------------------------------------------------------------------
    # Function activates the radio button options when a config file is successfully uploaded.
    # ------------------------------------------------------------------------------------------------



    if _configdata != None:
        _l = [html.H6('Select an Option:'),
        dcc.RadioItems(id = 'level_1_generated', options = [
        {'label': 'Validation Script Generation', 'value': 'Query Generation'},
        {'label': 'Validation Script Execution', 'value': 'Execution'}
        ])]
        return(_l)
    return(None)



@app.callback(
    Output('generation_dialog_box', 'children'),
    Input('level_1_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_checklist_1(radio_selection, config_input):



    # ------------------------------------------------------------------------------------------------
    # Function generates a warning pop up for user to make sure files are present at appropriate location
    # ------------------------------------------------------------------------------------------------



    if radio_selection == 'Query Generation':
        return([dcc.ConfirmDialog(
            id = 'generation_dialog_box_1',
            message = f"Please check STTMs are present at \n{config_input['Input folder path']}\\Input Files",
            displayed = True)])

@app.callback(
    Output('execution_dialog_box_1', 'children'),
    Input('level_1_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_checklist_1(radio_selection, config_input):



    # ------------------------------------------------------------------------------------------------
    # Function generates a warning pop up for user to make sure cluster is active
    # ------------------------------------------------------------------------------------------------



    if radio_selection == 'Execution':
        return([dcc.ConfirmDialog(
            id = 'execution_dialog_box_1',
            message = f"Please make sure Cluster is active.",
            displayed = True)])



@app.callback(
    Output('execution_dialog_box', 'children'),
    Input('submit_2', 'n_clicks'),
    State('level_2_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_checklist_1(n_clicks, selection_list, config_input):



    # ------------------------------------------------------------------------------------------------
    # Function generates a warning pop up for user to make sure files are present at appropriate location
    # ------------------------------------------------------------------------------------------------



    if n_clicks > 0:
        message_ = ''
        for item in selection_list:
            if item in ['Meta Data Validation', 'Source Data Validation', 'ETL Data Validation']:
                if item == 'Meta Data Validation':
                    if message_ != '':
                        message_ = message_ + '\n\n' + f"Please check STTMs are present at \n{config_input['Input folder path']}"
                    else:
                        message_ = f"Please check STTMs are present at \n{config_input['Input folder path']}"
                if item == 'Source Data Validation':
                    if message_ != '':
                        message_ = message_ + '\n\n' + f"Please check Source and Raw Data Files are present at \n{config_input['Input folder path']}"
                    else:
                        message_ = f"Please check Source and Raw Data Files are present at \n{config_input['Input folder path']}"

                if item == 'ETL Data Validation':
                    if message_ != '':
                        message_ = message_ + '\n\n' + f"Please check Queries are present at \n{config_input['Input folder path']}"
                    else:
                        message_ = f"Please check Queries are present at \n{config_input['Input folder path']}"
        
        if message_ != '':
            return([dcc.ConfirmDialog(
                id = 'execution_dialog_box_1',
                message = message_,
                displayed = True)])
        else:
            return(None)


@app.callback(
    Output('level_2', 'children'),
    Input('level_1_generated', 'value'))
def update_dashboard_checklist_1(radio_selection):



    # ------------------------------------------------------------------------------------------------
    # Function generates checklists based upom radio button selected
    # ------------------------------------------------------------------------------------------------



    if radio_selection == 'Query Generation':
        _l = [html.H6('Select from following Options:'),
        dcc.Checklist(id = 'level_2_generated', options = [
                {'label': 'JIRA Test Case Generation', 'value': 'Test Case Generation'},
                {'label': 'Lake Layer to Lake Hist Layer Query Generation', 'value': 'Lake - Lake Hist Query Generation'},
                {'label': 'Lake Hist Layer to Hub Layer Query Generation', 'value': 'Lake Hist - Hub query Generation'},
                {'label': 'Hub Layer to Mart Layer Query Generataion', 'value': 'Hub to Mart Query Generataion'}
                ]),
        html.Br(),
        html.Button('Submit', id = 'submit_2', n_clicks = 0)]
        return(_l)
    
    elif radio_selection == 'Execution':
        _l = [html.H6('Select from following Options:'),
        dcc.Checklist(id = 'level_2_generated', options = [
            {'label': 'Meta Data Validation', 'value': 'Meta Data Validation'},
            {'label': 'Source Data Validation', 'value': 'Source Data Validation'},
            {'label': 'ETL Data Validation', 'value': 'ETL Data Validation'}
            ]),
        html.Br(),
        html.Button('Submit', id = 'submit_2', n_clicks = 0)]
        return(_l)
    


@app.callback(
    Output('level_3', 'children'),
    Input('submit_2', 'n_clicks'),
    State('level_2_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_checklist_2(n_clicks, selection_list, config_data):



    # ------------------------------------------------------------------------------------------------
    # Function generates checklists based upon values selected in checklist generated from previous function
    # ------------------------------------------------------------------------------------------------



    if n_clicks > 0:
        _flag = 0
        child_list = [html.H6('Select from following Options:')]
        _option_list = []
        if selection_list != None:
            if 'Meta Data Validation' in selection_list:
                _flag += 1
                _l = [
                    {'label': 'Meta Data Validation  -  Lake Layer', 'value': 'Lake Table Validation'},
                    {'label': 'Meta Data Validation  -  Lake Hist Layer', 'value': 'Lake Hist Table Validation'},
                    {'label': 'Meta Data Validation  -  Hub Layer', 'value': 'Hub Table Validation'},
                    {'label': 'Meta Data Validation  -  Mart Layer', 'value': 'Mart Table Validation'}
                ]
                _option_list.extend(_l)

            if 'Source Data Validation' in selection_list:
                _flag += 1
                _l = [
                    {'label': 'Source Data Validation  -  Source to Raw Layer', 'value': 'Source - Raw Data Validation'},
                    {'label': 'Source Data Validation  -  Raw to Lake Layer', 'value': 'Raw - Lake Data Validation'},
                    {'label': 'Source Data Validation  -  Source to Lake Layer', 'value': 'Source - Lake Data Validtion'}
                ]
                _option_list.extend(_l)

            if 'ETL Data Validation' in selection_list:
                _flag += 1
                _l = [
                    {'label': 'ETL Data Validation - Lake to Lake Hist Layer', 'value': 'Lake - Lake Hist Validation'},
                    {'label': 'ETL Data Validation - Lake Hist to Hub Layer', 'value': 'Lake Hist - Hub Validation'},
                    {'label': 'ETL Data Validation - Hub to Mart Layer', 'value': 'Hub to Mart Validation'}
                ]
                _option_list.extend(_l)

            if (_flag != 0) and (len(_option_list) == 0):
                _err = [dcc.ConfirmDialog(id = 'level_3_generated', message = 'Atleast 1 option should be selected to proceed', displayed = True)]
                return(_err)
            elif (_flag != 0) and (len(_option_list) != 0):
                child_list.append(dcc.Checklist(options = _option_list, id = 'level_3_generated'))
                child_list.append(html.Br())
                child_list.append(html.Button('Submit', id = 'submit_3', n_clicks = 0))        
                return(child_list)
            elif _flag == 0:
                return(None)

        return(None)



@app.callback(
    Output('test_case_generation', 'children'),
    Output('test_case_generation_export_data', 'data'),
    Input('submit_2', 'n_clicks'),
    State('level_2_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_message_board_1(n_clicks, selection_list, config_data):



    # ------------------------------------------------------------------------------------------------
    # Function prints logs generated from running test_case_generation
    # ------------------------------------------------------------------------------------------------



    if n_clicks > 0:
        if selection_list != None:
            if 'Test Case Generation' in selection_list:
                children_list = []
                read_data_obj = JIRA.ReadConfigData(config_data['src_sys_name'], config_data['Input folder path'],
                                                    config_data['Output folder path'],
                                                    config_data['Src to Lake STTM  name'],
                                                    config_data['Lake to Lake Hist STTM name'],
                                                    config_data['Lake Hist to Hub STTM name'])
                _val = read_data_obj.ReadExcelData()
                # _val = 'Hello!!!<BR>Hello!!!Hello!!!Hello!!!Hello!!!<BR>Hello!!!Hello!!!Hello!!!Hello!!!<BR>Hello!!!Hello!!!Hello!!!<BR>Hello!!!Hello!!!Hello!!!' # add python file here
                # children_list.append(html.H6('Message from Execution of Test Case Generation'))
                children_list.append(html.Div([html.Iframe(id = "test_case_generation", srcDoc = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}', style = {'border' : 0, 'width' : '100%', 'height' : math.ceil(len(_val)/170) * 25}
)]))
                return(children_list, f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}')
    return(None, None)



@app.callback(
    Output('lake_hist_query', 'children'),
    Output('lake_hist_query_export_data', 'data'),
    Input('submit_2', 'n_clicks'),
    State('level_2_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_message_board_2(n_clicks, selection_list, config_data):



    # ------------------------------------------------------------------------------------------------
    # Function prints logs generated from running lake_hist_query
    # ------------------------------------------------------------------------------------------------



    if n_clicks > 0:
        if selection_list != None:
            if 'Lake - Lake Hist Query Generation' in selection_list:
                children_list = []
                run_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '').replace('.', '')
                sttm_data = rdconfig.ReadConfigData.ReadSttmData("", config_data['Input folder path'] + '\\Input Files\\' + config_data['Lake to Lake Hist STTM name'])
                query_gen = QG.QueryGenerator(sttm_data, config_data['Output folder path'], run_time)
                _val = query_gen.generate_queries(config_data['Input folder path'] + '\\Pyfiles\\Query_tempelate.xlsx','Lake-LakeHist')
                _val = "Lake to Lake Hist Query Generation Completed Successfully, Validation results placed at : %s" % _val
                # children_list.append(html.H6('Message from Execution of Lake - Lake Hist Query Generation'))
                children_list.append(html.Div([html.Iframe(id = "lake_hist_query", srcDoc = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}', style = {'border' : 0, 'width' : '100%', 'height' : math.ceil(len(_val)/170) * 25}
)]))
                return(children_list, f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}')
    return(None, None)



@app.callback(
    Output('hub_query', 'children'),
    Output('hub_query_export_data', 'data'),
    Input('submit_2', 'n_clicks'),
    State('level_2_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_message_board_3(n_clicks, selection_list, config_data):



    # ------------------------------------------------------------------------------------------------
    # Function prints logs generated from running hub_query
    # ------------------------------------------------------------------------------------------------



    if n_clicks > 0:
        if selection_list != None:
            if 'Lake Hist - Hub query Generation' in selection_list:
                children_list = []
                run_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '').replace('.', '')
                sttm_data = rdconfig.ReadConfigData.ReadSttmData("",
                                                                 config_data['Input folder path'] + '\\Input Files\\' +config_data['Lake Hist to Hub STTM name'])
                query_gen = QG.QueryGenerator(sttm_data, config_data['Output folder path'], run_time)
                _val = query_gen.generate_queries(
                    config_data['Input folder path'] + '\\Pyfiles\\Query_tempelate.xlsx','LakeHist-Hub')
                _val = "Lake Hist to Hub Query Generation Completed Successfully, Validation results placed at : %s" % _val
                # children_list.append(html.H6('Message from Execution of Lake Hist - Hub query Generation'))
                children_list.append(html.Div([html.Iframe(id = "hub_query", srcDoc = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}', style = {'border' : 0, 'width' : '100%', 'height' : math.ceil(len(_val)/170) * 25}
)]))
                return(children_list, f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}')
    return(None, None)



@app.callback(
    Output('hub_to_mart_query', 'children'),
    Output('hub_to_mart_query_export_data', 'data'),
    Input('submit_2', 'n_clicks'),
    State('level_2_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_message_board_4(n_clicks, selection_list, config_data):



    # ------------------------------------------------------------------------------------------------
    # Function prints logs generated from running hub_to_mart_query
    # ------------------------------------------------------------------------------------------------



    if n_clicks > 0:
        if selection_list != None:
            if 'Hub to Mart Query Generataion' in selection_list:
                children_list = []
                run_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '').replace('.', '')
                sttm_data = rdconfig.ReadConfigData.ReadSttmData("",
                                                                 config_data['Input folder path'] + '\\Input Files\\' +config_data['Hub to Mart STTM name'])
                query_gen = QG.QueryGenerator(sttm_data, config_data['Output folder path'], run_time)
                _val = query_gen.generate_queries(
                    config_data['Input folder path'] + '\\Pyfiles\\Query_tempelate.xlsx','Hub-Mart')
                _val = "Hub to Mart Query Generation Completed Successfully, Validation results placed at : %s" % _val
                children_list.append(html.Div([html.Iframe(id = "hub_to_mart_query", srcDoc = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}', style = {'border' : 0, 'width' : '100%', 'height' : math.ceil(len(_val)/170) * 25}
)]))
                return(children_list, f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}')
    return(None, None)



@app.callback(
    Output('lake_table', 'children'),
    Output('lake_table_export_data', 'data'),
    Input('submit_3', 'n_clicks'),
    State('level_3_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_message_board_5(n_clicks, selection_list, config_data):



    # ------------------------------------------------------------------------------------------------
    # Function prints logs generated from running lake_table
    # ------------------------------------------------------------------------------------------------



    if n_clicks > 0:
        if selection_list != None:
            if 'Lake Table Validation' in selection_list:
                children_list = []
                run_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '').replace('.', '')
                conn = dbricks.DatabricksConnection.getConnection('', config_data['Databricks connection string'])
                sttm_lake_data = rdconfig.ReadConfigData.ReadLakeSttmData("",
                                                                          config_data['Input folder path'] + '\\Input Files\\' + config_data['Src to Lake STTM  name'])
                metadata_obj = MD.Metadatavalidation(sttm_lake_data, config_data['Output folder path'], conn, run_time)
                _val = metadata_obj.validate_metadata('Lake')
                _val = 'Lake Table Meta Data Validation Completed Successfully, Validation results placed at : %s' % _val
                # children_list.append(html.H6('Message from Execution of Lake Table Validation'))
                children_list.append(html.Div([html.Iframe(id = "lake_table", srcDoc = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}', style = {'border' : 0, 'width' : '100%', 'height' : math.ceil(len(_val)/170) * 25}
)]))
                return(children_list, f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}')
    return(None, None)



@app.callback(
    Output('lake_hist_table_validation', 'children'),
    Output('lake_hist_table_validation_export_data', 'data'),
    Input('submit_3', 'n_clicks'),
    State('level_3_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_message_board_6(n_clicks, selection_list, config_data):



    # ------------------------------------------------------------------------------------------------
    # Function prints logs generated from running lake_hist_table_validation
    # ------------------------------------------------------------------------------------------------



    if n_clicks > 0:
        if selection_list != None:
            if 'Lake Hist Table Validation' in selection_list:
                children_list = []
                run_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '').replace('.', '')
                conn = dbricks.DatabricksConnection.getConnection('', config_data['Databricks connection string'])
                sttm_data = rdconfig.ReadConfigData.ReadSttmData("",
                                                                 config_data['Input folder path'] + '\\Input Files\\' + config_data['Lake to Lake Hist STTM name'])
                metadata_obj = MD.Metadatavalidation(sttm_data, config_data['Output folder path'], conn, run_time)
                _val = metadata_obj.validate_metadata('Lake-Hist')
                _val = 'Lake Hist Table Meta Data Validation Completed Successfully, Validation results placed at : %s' % _val
                children_list.append(html.Div([html.Iframe(id = "lake_hist_table_validation", srcDoc = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}', style = {'border' : 0, 'width' : '100%', 'height' : math.ceil(len(_val)/170) * 25}
)]))
                return(children_list, f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}')
    return(None, None)



@app.callback(
    Output('hub_table_validation', 'children'),
    Output('hub_table_validation_export_data', 'data'),
    Input('submit_3', 'n_clicks'),
    State('level_3_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_message_board_7(n_clicks, selection_list, config_data):



    # ------------------------------------------------------------------------------------------------
    # Function prints logs generated from running hub_table_validation
    # ------------------------------------------------------------------------------------------------



    if n_clicks > 0:
        if selection_list != None:
            if 'Hub Table Validation' in selection_list:
                children_list = []
                run_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '').replace('.', '')
                conn = dbricks.DatabricksConnection.getConnection('', config_data['Databricks connection string'])
                sttm_data = rdconfig.ReadConfigData.ReadSttmData("",
                                                                 config_data['Input folder path'] + '\\Input Files\\' + config_data['Lake Hist to Hub STTM name'])
                metadata_obj = MD.Metadatavalidation(sttm_data, config_data['Output folder path'], conn, run_time)
                _val = metadata_obj.validate_metadata('Hub')
                _val = 'HUb Table Meta Data Validation Completed Successfully, Validation results placed at : %s' % _val
                children_list.append(html.Div([html.Iframe(id = "hub_table_validation", srcDoc = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}', style = {'border' : 0, 'width' : '100%', 'height' : math.ceil(len(_val)/170) * 25}
)]))
                return(children_list, f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}')
    return(None, None)



@app.callback(
    Output('mart_table_validation', 'children'),
    Output('mart_table_validation_export_data', 'data'),
    Input('submit_3', 'n_clicks'),
    State('level_3_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_message_board_8(n_clicks, selection_list, config_data):



    # ------------------------------------------------------------------------------------------------
    # Function prints logs generated from running mart_table_validation
    # ------------------------------------------------------------------------------------------------



    if n_clicks > 0:
        if selection_list != None:
            if 'Mart Table Validation' in selection_list:
                children_list = []
                run_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '').replace('.', '')
                conn = dbricks.DatabricksConnection.getConnection('', config_data['Databricks connection string'])
                sttm_data = rdconfig.ReadConfigData.ReadSttmData("",
                                                                 config_data['Input folder path'] + '\\Input Files\\' + config_data['Hub to Mart STTM name'])
                metadata_obj = MD.Metadatavalidation(sttm_data, config_data['Output folder path'], conn, run_time)
                _val = metadata_obj.validate_metadata('Mart')
                _val = 'Mart Table Meta Data Validation Completed Successfully, Validation results placed at : %s' % _val
                # children_list.append(html.H6('Message from Execution of Mart Table Validation'))
                children_list.append(html.Div([html.Iframe(id = "mart_table_validation", srcDoc = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}', style = {'border' : 0, 'width' : '100%', 'height' : math.ceil(len(_val)/170) * 25}
)]))
                return(children_list, f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}')
    return(None, None)



@app.callback(
    Output('source_raw_data_validation', 'children'),
    Output('source_raw_data_validation_export_data', 'data'),
    Input('submit_3', 'n_clicks'),
    State('level_3_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_message_board_9(n_clicks, selection_list, config_data):



    # ------------------------------------------------------------------------------------------------
    # Function prints logs generated from running source_raw_data_validation
    # ------------------------------------------------------------------------------------------------



    if n_clicks > 0:
        if selection_list != None:
            if 'Source - Raw Data Validation' in selection_list:
                children_list = []
                run_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '').replace('.', '')
                conn = dbricks.DatabricksConnection.getConnection('', config_data['Databricks connection string'])
                src_lake = SL.ValidateSrcToLake(conn, config_data['Output folder path'], run_time)
                for file_num in range(len(config_data['Raw data file name'])):
                    run_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '').replace('.', '')
                    _val = src_lake.srctolake('%s\\Input Files\\%s' % (config_data['Input folder path'], config_data['Source data file name'][file_num]),
                                    '%s\\Input Files\\%s' % (config_data['Input folder path'], config_data['Raw data file name'][file_num]),
                                    False, run_time)
                _val = 'Src to Raw Validation Completed Successfully, Validation results placed at : %s' % _val
                children_list.append(html.Div([html.Iframe(id = "source_raw_data_validation", srcDoc = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}', style = {'border' : 0, 'width' : '100%', 'height' : math.ceil(len(_val)/170) * 25}
)]))
                return(children_list, f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}')
    return(None, None)



@app.callback(
    Output('raw_lake_data_validation', 'children'),
    Output('raw_lake_data_validation_export_data', 'data'),
    Input('submit_3', 'n_clicks'),
    State('level_3_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_message_board_10(n_clicks, selection_list, config_data):



    # ------------------------------------------------------------------------------------------------
    # Function prints logs generated from running raw_lake_data_validation
    # ------------------------------------------------------------------------------------------------



    if n_clicks > 0:
        if selection_list != None:
            if 'Raw - Lake Data Validation' in selection_list:
                children_list = []
                run_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '').replace('.', '')
                conn = dbricks.DatabricksConnection.getConnection('', config_data['Databricks connection string'])
                src_lake = SL.ValidateSrcToLake(conn, config_data['Output folder path'], run_time)
                for file_num in range(len(config_data['Raw data file name'])):
                    run_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '').replace('.', '')
                    _val = src_lake.srctolake(False,
                                   '%s\\Input Files\\%s' % (config_data['Input folder path'], config_data['Raw data file name'][file_num]),
                                        config_data['Lake table name'][file_num], run_time)
                _val = 'Raw to Lake Validation Completed Successfully, Validation results placed at : %s' % _val
                children_list.append(html.Div([html.Iframe(id = "raw_lake_data_validation", srcDoc = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}', style = {'border' : 0, 'width' : '100%', 'height' : math.ceil(len(_val)/170) * 25}
)]))
                return(children_list, f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}')
    return(None, None)



@app.callback(
    Output('source_lake_data_validtion', 'children'),
    Output('source_lake_data_validtion_export_data', 'data'),
    Input('submit_3', 'n_clicks'),
    State('level_3_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_message_board_11(n_clicks, selection_list, config_data):



    # ------------------------------------------------------------------------------------------------
    # Function prints logs generated from running source_lake_data_validtion
    # ------------------------------------------------------------------------------------------------



    if n_clicks > 0:
        if selection_list != None:
            if 'Source - Lake Data Validtion' in selection_list:
                children_list = []
                run_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '').replace('.', '')
                conn = dbricks.DatabricksConnection.getConnection('', config_data['Databricks connection string'])
                src_lake_obj = SL.ValidateSrcToLake(conn, config_data['Output folder path'], run_time)
                for file_num in range(len(config_data['Raw data file name'])):
                    run_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '').replace('.', '')
                    _val = src_lake_obj.srctolake(
                        '%s\\Input Files\\%s' % (config_data['Input folder path'], config_data['Source data file name'][file_num]),
                        '%s\\Input Files\\%s' % (config_data['Input folder path'], config_data['Raw data file name'][file_num]),
                        config_data['Lake table name'][file_num], run_time)
                _val = 'Src to Lake Validation Completed Successfully, Validation results placed at : %s' % _val
                children_list.append(html.Div([html.Iframe(id = "source_lake_data_validtion", srcDoc = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}', style = {'border' : 0, 'width' : '100%', 'height' : math.ceil(len(_val)/170) * 25}
)]))
                return(children_list, f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}')
    return(None, None)



@app.callback(
    Output('lake_lake_hist_validation', 'children'),
    Output('lake_lake_hist_validation_export_data', 'data'),
    Input('submit_3', 'n_clicks'),
    State('level_3_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_message_board_12(n_clicks, selection_list, config_data):



    # ------------------------------------------------------------------------------------------------
    # Function prints logs generated from running lake_lake_hist_validation
    # ------------------------------------------------------------------------------------------------



    if n_clicks > 0:
        if selection_list != None:
            if 'Lake - Lake Hist Validation' in selection_list:
                children_list = []
                run_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '').replace('.', '')
                conn = dbricks.DatabricksConnection.getConnection('', config_data['Databricks connection string'])
                etl_obj = ETL.ValidateData(conn, config_data['Output folder path'], run_time)
                queries_output_file =  config_data['Input folder path'] + '\\Input Files\\' + config_data['Hub to Mart ETL Queries']
                _val, path = etl_obj.GenerateReport(queries_output_file,'Lake-LakeHist')
                _val = _val + ' Lake to Lake Hist Validation Completed, Validation results placed at : %s' % path
                children_list.append(html.Div([html.Iframe(id = "lake_lake_hist_validation", srcDoc = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}', style = {'border' : 0, 'width' : '100%', 'height' : math.ceil(len(_val)/170) * 25}
)]))
                return(children_list, f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}')
    return(None, None)



@app.callback(
    Output('lake_hist_hub_validation', 'children'),
    Output('lake_hist_hub_validation_export_data', 'data'),
    Input('submit_3', 'n_clicks'),
    State('level_3_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_message_board_13(n_clicks, selection_list, config_data):



    # ------------------------------------------------------------------------------------------------
    # Function prints logs generated from running lake_hist_hub_validation
    # ------------------------------------------------------------------------------------------------



    if n_clicks > 0:
        if selection_list != None:
            if 'Lake Hist - Hub Validation' in selection_list:
                children_list = []
                run_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '').replace('.', '')
                conn = dbricks.DatabricksConnection.getConnection('', config_data['Databricks connection string'])
                etl_obj = ETL.ValidateData(conn, config_data['Output folder path'], run_time)
                queries_output_file = config_data['Input folder path'] + '\\Input Files\\' + config_data[
                    'Hub to Mart ETL Queries']
                _val, path = etl_obj.GenerateReport(queries_output_file,'LakeHist-Hub')
                _val = _val + ' Lake Hist to Hub Validation Completed, Validation results placed at : %s' % path
                children_list.append(html.Div([html.Iframe(id = "lake_hist_hub_validation", srcDoc = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}', style = {'border' : 0, 'width' : '100%', 'height' : math.ceil(len(_val)/170) * 25}
)]))
                return(children_list, f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}')
    return(None, None)



@app.callback(
    Output('hub_to_mart_validation', 'children'),
    Output('hub_to_mart_validation_export_data', 'data'),
    Input('submit_3', 'n_clicks'),
    State('level_3_generated', 'value'),
    State('config_input', 'data'))
def update_dashboard_message_board_14(n_clicks, selection_list, config_data):



    # ------------------------------------------------------------------------------------------------
    # Function prints logs generated from running hub_to_mart_validation
    # ------------------------------------------------------------------------------------------------



    if n_clicks > 0:
        if selection_list != None:
            if 'Hub to Mart Validation' in selection_list:
                children_list = []
                run_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '').replace('.', '')
                conn = dbricks.DatabricksConnection.getConnection('', config_data['Databricks connection string'])
                etl_obj = ETL.ValidateData(conn, config_data['Output folder path'], run_time)
                queries_output_file = config_data['Input folder path'] + '\\Input Files\\' + config_data[
                    'Hub to Mart ETL Queries']
                _val , path = etl_obj.GenerateReport(queries_output_file,'Hub-Mart')
                _val = _val + ' Hub to Mart Validation Completed Successfully and %s' % path
                children_list.append(html.Div([html.Iframe(id = "hub_to_mart_validation", srcDoc = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}', style = {'border' : 0, 'width' : '100%', 'height' : math.ceil(len(_val)/170) * 25}
)]))
                return(children_list, f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : {_val}')
    return(None, None)



if __name__ == '__main__':
    webbrowser.open('http://127.0.0.1:8051/')
    app.run_server(debug = True, port = 8051)