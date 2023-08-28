import os
import time
import datetime
import Pyfiles.SessionState as SessionState
import streamlit as st
from stqdm import stqdm
import Pyfiles.ReadConfigData as rdconfig, Pyfiles.QueryGenerator as QG, Pyfiles.Databricks_conn as dbricks, \
    Pyfiles.SrcToLake as SL, Pyfiles.MetaDataValidation as MD, Pyfiles.ETLValidation as ETL, \
    Pyfiles.JiraGenerator as JIRA, Pyfiles.Defect_Tracker as DT, Pyfiles.Mail as Mail

session_state = SessionState.get(checklist_1_gene=False, checklist_1_exec=False, checklist_2_exec=False,
                                 gene_message_board_main='', exec_message_board_main='', global_timestamp_str=str(
        datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S').replace('/', '').replace(':', '').replace(' ', '-')),
                                 email_button_gene=False, email_button_exec=False, email_add_file_gene=None,
                                 email_add_file_exec=None)

st.set_page_config(layout="wide")
st.markdown(""" <style>div[role="radiogroup"] >  :first-child{display: none !important;}</style> """,
            unsafe_allow_html=True)


# def send_email_function(contents):
# 	decoded = contents.decode('utf-8').split(',')
# 	email_list = [i.strip() for i in decoded]
# 	print("Hello1")
# 	print(contents)
# 	Mail.Mailer.send_email('',email_list)

def fetch_config_dict(contents):
    decoded = contents.decode('utf-8').replace('\r', '').replace('\"', '').split('\n')
    decoded = [i.strip() for i in decoded if not (i.startswith('#') or i == '')]
    final_dict = {}
    for item in decoded:
        _l = item.split(':', 1)

        if _l[0].lower() in ['source data file name', 'raw data file name', 'lake table name']:
            _l[1] = _l[1].split(',')

        final_dict[_l[0]] = _l[1]
    return (final_dict)


st.markdown("<h1 style='text-align: center; color: black'>Welcome to AWS Testing Tool</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align: left; color: black'>Upload Configuration File</h3>", unsafe_allow_html=True)
uploaded_file = st.file_uploader('', type=("txt"))
global_timestamp_str = session_state.global_timestamp_str
print(global_timestamp_str)

if uploaded_file:

    config_dict = fetch_config_dict(uploaded_file.read())

    global_output_folder_path = str(config_dict['Output folder path']).strip().replace('\\',
                                                                                       '/') + '/Output files/default_logs/'
    if not os.path.exists(global_output_folder_path):
        os.makedirs(global_output_folder_path)

    global_output_folder_path = global_output_folder_path + global_timestamp_str + '.txt'

    st.markdown("<h3 style='text-align: left; color: black'>Select an Option</h3>", unsafe_allow_html=True)
    radio_options = st.radio('', options=['None', 'Validation Script Generation', 'Validation Script Execution'],
                             index=0)
    if radio_options == 'Validation Script Generation':

        st.warning(f"Please check STTMs are present at {config_dict['Input folder path']}")
        st.markdown("<h3 style='text-align: left; color: black'>Select from following Options</h3>",
                    unsafe_allow_html=True)
        jira_test_case_checklist_1 = st.checkbox('JIRA Test Case Generation')
        lake_to_lake_hist_checklist_1 = st.checkbox('Lake Layer to Lake Hist Layer Query Generation')
        lake_hist_to_hub_checklist_1 = st.checkbox('Lake Hist Layer to Hub Layer Query Generation')
        hub_to_mart_checklist_1 = st.checkbox('Hub Layer to Mart Layer Query Generataion')
        select_all_checklist_1 = st.checkbox('All of the Above')
        submit_1 = st.button('Submit')
        st.markdown("<h2 style='text-align: center; color: black'>Message Board</h2>", unsafe_allow_html=True)
        button_columns = st.beta_columns(4)
        defect_generator_button = button_columns[0].button('Defect Generator')
        defect_uploader_button = button_columns[1].button('Defect Uploader')
        email_button = button_columns[2].button('Email Results')
        export_button = button_columns[3].button('Export Board')

        checklist_1 = []
        if select_all_checklist_1:
            jira_test_case_checklist_1 = True
            lake_to_lake_hist_checklist_1 = True
            lake_hist_to_hub_checklist_1 = True
            hub_to_mart_checklist_1 = True
        if jira_test_case_checklist_1:
            checklist_1.extend(['JIRA Test Case Generation'])
        if lake_to_lake_hist_checklist_1:
            checklist_1.extend(['Lake Layer to Lake Hist Layer Query Generation'])
        if lake_hist_to_hub_checklist_1:
            checklist_1.extend(['Lake Hist Layer to Hub Layer Query Generation'])
        if hub_to_mart_checklist_1:
            checklist_1.extend(['Hub Layer to Mart Layer Query Generataion'])

        if submit_1:
            session_state.checklist_1_gene = True
            gene_message_board = ''
            for selected_item in stqdm(checklist_1):

                if selected_item == 'JIRA Test Case Generation':
                    jira_test_val_gene = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {selected_item}'
                    st.write(jira_test_val_gene)

                    read_data_obj = JIRA.ReadConfigData(config_dict['src_sys_name'], config_dict['Input folder path'],
                                                        config_dict['Output folder path'],
                                                        config_dict['Src to Lake STTM  name'],
                                                        config_dict['Lake to Lake Hist STTM name'],
                                                        config_dict['Lake Hist to Hub STTM name'])
                    _val = read_data_obj.ReadExcelData()
                    _val = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {_val}'
                    st.write(_val)
                    jira_test_val_gene = jira_test_val_gene + _val

                    gene_message_board = gene_message_board + '  \n  \n' + jira_test_val_gene
                    with open(str(global_output_folder_path), "a") as _f:
                        _f.write(jira_test_val_gene + '\n')

                if selected_item == 'Lake Layer to Lake Hist Layer Query Generation':
                    lake_to_lake_hist_val_gene = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {selected_item}'
                    st.write(lake_to_lake_hist_val_gene)

                    run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(':',
                                                                                                               '').replace(
                        '/', '')
                    sttm_data = rdconfig.ReadConfigData.ReadSttmData("", config_dict[
                        'Input folder path'] + '\\Input Files\\' + config_dict['Lake to Lake Hist STTM name'])
                    query_gen = QG.QueryGenerator(sttm_data, config_dict['Output folder path'], run_time)
                    _val = query_gen.generate_queries(
                        config_dict['Input folder path'] + '\\Pyfiles\\Query_tempelate.xlsx', 'Lake-LakeHist')
                    _val = "Lake Layer to Lake Hist Layer Query Generation Completed Successfully, Queries placed at : %s" % _val

                    _val = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {_val}'
                    st.write(_val)
                    lake_to_lake_hist_val_gene = lake_to_lake_hist_val_gene + _val

                    gene_message_board = gene_message_board + '  \n  \n' + lake_to_lake_hist_val_gene
                    with open(str(global_output_folder_path), "a") as _f:
                        _f.write(lake_to_lake_hist_val_gene + '\n')

                if selected_item == 'Lake Hist Layer to Hub Layer Query Generation':
                    lake_hist_to_hub_val_gene = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {selected_item}'
                    st.write(lake_hist_to_hub_val_gene)

                    run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(':',
                                                                                                               '').replace(
                        '/', '')
                    sttm_data = rdconfig.ReadConfigData.ReadSttmData("",
                                                                     config_dict[
                                                                         'Input folder path'] + '\\Input Files\\' +
                                                                     config_dict['Lake Hist to Hub STTM name'])
                    query_gen = QG.QueryGenerator(sttm_data, config_dict['Output folder path'], run_time)
                    _val = query_gen.generate_queries(
                        config_dict['Input folder path'] + '\\Pyfiles\\Query_tempelate.xlsx', 'LakeHist-Hub')
                    _val = "Lake Hist Layer to Hub Layer Query Generation Completed Successfully, Queries placed at : %s" % _val

                    _val = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {_val}'
                    st.write(_val)
                    lake_hist_to_hub_val_gene = lake_hist_to_hub_val_gene + _val

                    gene_message_board = gene_message_board + '  \n  \n' + lake_hist_to_hub_val_gene
                    with open(str(global_output_folder_path), "a") as _f:
                        _f.write(lake_hist_to_hub_val_gene + '\n')

                if selected_item == 'Hub Layer to Mart Layer Query Generataion':
                    time.sleep(1)
                    hub_to_mart_val_gene = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {selected_item}'
                    st.write(hub_to_mart_val_gene)

                    run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(':',
                                                                                                               '').replace(
                        '/', '')
                    sttm_data = rdconfig.ReadConfigData.ReadSttmData("",
                                                                     config_dict[
                                                                         'Input folder path'] + '\\Input Files\\' +
                                                                     config_dict['Hub to Mart STTM name'])
                    query_gen = QG.QueryGenerator(sttm_data, config_dict['Output folder path'], run_time)
                    _val = query_gen.generate_queries(
                        config_dict['Input folder path'] + '\\Pyfiles\\Query_tempelate.xlsx', 'Hub-Mart')
                    _val = "Hub Layer to Mart Layer Query Generation Completed Successfully, Queries placed at : %s" % _val
                    _val = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {_val}'
                    st.write(_val)
                    hub_to_mart_val_gene = hub_to_mart_val_gene + _val

                    gene_message_board = gene_message_board + '  \n  \n' + hub_to_mart_val_gene
                    with open(str(global_output_folder_path), "a") as _f:
                        _f.write(hub_to_mart_val_gene + '\n')

                session_state.gene_message_board_main = gene_message_board

        if export_button:
            if session_state.gene_message_board_main:
                st.write(session_state.gene_message_board_main)

            timestamp_str = str(
                datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S').replace('/', '').replace(':', '').replace(' ',
                                                                                                                '-'))
            output_folder_path = str(config_dict['Output folder path']).strip().replace('\\', '/')
            final_path = output_folder_path + f'/Output Files/logs/'

            if not os.path.exists(final_path):
                os.makedirs(final_path)

            final_path = final_path + 'logs-' + timestamp_str + '.txt'

            with open(str(final_path), "a") as _f:
                _f.write(session_state.gene_message_board_main + '\n')

            st.success(f'Logs stored in {final_path}')

        if email_button:  # or session_state.email_button_gene
            session_state.email_button_gene = True
            if session_state.gene_message_board_main:
                st.write(session_state.gene_message_board_main)

            if email_button:
                st.markdown("<h3 style='text-align: left; color: black'>Upload email recipient address file</h3>",
                            unsafe_allow_html=True)
                uploaded_email_file = st.file_uploader('', type=("txt"), key='email_file_uploader')
                if uploaded_email_file:
                    session_state.email_add_file_gene = uploaded_email_file

        if session_state.email_button_gene and session_state.email_add_file_gene:
            print("Hello")
            print(session_state.email_add_file_gene)

        # decoded = contents.decode('utf-8').split(',')
        # email_list = [i.strip() for i in decoded]
        # print("Hello1")
        # print(contents)
        # Mail.Mailer.send_email('',email_list)
        # decoded = contents.decode('utf-8').split(',')
        #
        # send_email_function(session_state.email_add_file_gene.read())
        # st.success('Email sent successfully')

        if defect_uploader_button:
            if session_state.gene_message_board_main:
                st.write(session_state.gene_message_board_main)
            run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(':', '').replace(
                '/',
                '')
            c = DT.DefectTracker('Philosophy', r'C:\Users\jtd2199\Desktop\AWS Testing Accelerator', run_time)
            server = 'https://ata99.atlassian.net'
            username = 'farhhan786@gmail.com'
            API_token = 'CRCHRHtSQugZGnhLTy4QCC31'
            c.createdefects(server, username, API_token)
        st.success('Defect Uploader')

        if defect_generator_button:
            if session_state.gene_message_board_main:
                st.write(session_state.gene_message_board_main)
                run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(':',
                                                                                                           '').replace(
                    '/',
                    '')
                prj_key = 'ATA'
                reporter = 'farhhan786@gmail.com'
                assignee = 'farhhan786@gmail.com'
                c = DT.DefectTracker('Philosophy', r'C:\Users\jtd2199\Desktop\AWS Testing Accelerator', run_time)
                c.createdefecttracker(prj_key, reporter, assignee)
            st.success('Defect Generator')




    elif radio_options == 'Validation Script Execution':
        st.warning("Please make sure Cluster is active.")
        st.markdown("<h3 style='text-align: left; color: black'>Select from following Options</h3>",
                    unsafe_allow_html=True)
        meta_data_checklist_1 = st.checkbox('Meta Data Validation')
        source_data_checklist_1 = st.checkbox('Source Data Validation')
        etl_data_checklist_1 = st.checkbox('ETL Data Validation')
        select_all_checklist_1 = st.checkbox('All of the Above', key='checklist_1')
        submit_1 = st.button('Submit', key='submit_1')

        checklist_1 = []
        if select_all_checklist_1:
            checklist_1.extend(['Meta Data Validation', 'Source Data Validation', 'ETL Data Validation'])
        else:
            if meta_data_checklist_1:
                checklist_1.extend(['Meta Data Validation'])
            if source_data_checklist_1:
                checklist_1.extend(['Source Data Validation'])
            if etl_data_checklist_1:
                checklist_1.extend(['ETL Data Validation'])

        session_state = SessionState.get(checklist_1=False, checklist_2=False)
        if submit_1 or session_state.checklist_1_exec:
            session_state.checklist_1_exec = True

            if 'Meta Data Validation' in checklist_1:
                st.warning(f"Please check STTMs are present at \n{config_dict['Input folder path']}")
            if 'Source Data Validation' in checklist_1:
                st.warning(
                    f"Please check Source and Raw Data Files are present at \n{config_dict['Input folder path']}")

            if 'ETL Data Validation' in checklist_1:
                st.warning(f"Please check Queries are present at \n{config_dict['Input folder path']}")

            mdv_lake_layer_checklist_2 = False
            mdv_lake_hist_layer_checklist_2 = False
            mdv_hub_layer_checklist_2 = False
            mdv_mart_layer_checklist_2 = False
            rdv_source_to_raw_checklist_2 = False
            rdv_raw_to_lake_checklist_2 = False
            rdv_source_to_lake_checklist_2 = False
            edv_lake_to_lake_hist_checklist_2 = False
            edv_lake_hist_to_hub_checklist_2 = False
            edv_hub_to_mart_checklist_2 = False

            if len(checklist_1) > 0:
                st.markdown("<h3 style='text-align: left; color: black'>Select from following Options</h3>",
                            unsafe_allow_html=True)

            if 'Meta Data Validation' in checklist_1:
                mdv_lake_layer_checklist_2 = st.checkbox('Meta Data Validation  -  Lake Layer')
                mdv_lake_hist_layer_checklist_2 = st.checkbox('Meta Data Validation  -  Lake Hist Layer')
                mdv_hub_layer_checklist_2 = st.checkbox('Meta Data Validation  -  Hub Layer')
                mdv_mart_layer_checklist_2 = st.checkbox('Meta Data Validation  -  Mart Layer')

            if 'Source Data Validation' in checklist_1:
                rdv_source_to_raw_checklist_2 = st.checkbox('Source Data Validation  -  Source to Raw Layer')
                rdv_raw_to_lake_checklist_2 = st.checkbox('Source Data Validation  -  Raw to Lake Layer')
                rdv_source_to_lake_checklist_2 = st.checkbox('Source Data Validation  -  Source to Lake Layer')

            if 'ETL Data Validation' in checklist_1:
                edv_lake_to_lake_hist_checklist_2 = st.checkbox('ETL Data Validation - Lake to Lake Hist Layer')
                edv_lake_hist_to_hub_checklist_2 = st.checkbox('ETL Data Validation - Lake Hist to Hub Layer')
                edv_hub_to_mart_checklist_2 = st.checkbox('ETL Data Validation - Hub to Mart Layer')

            select_all_checklist_2 = False
            submit_2 = False
            if len(checklist_1) > 0:
                select_all_checklist_2 = st.checkbox('All of the Above', key='checlist_2')
                submit_2 = st.button('Submit', key='submit_2')
            st.markdown("<h2 style='text-align: center; color: black'>Message Board</h2>", unsafe_allow_html=True)

            button_columns = st.beta_columns(4)
            defect_generator_button = button_columns[0].button('Defect Generator')
            defect_uploader_button = button_columns[1].button('Defect Uploader')
            email_button = button_columns[2].button('Email Results')
            export_button = button_columns[3].button('Export Board')

            checklist_2 = []
            if select_all_checklist_2:
                mdv_lake_layer_checklist_2 = True
                mdv_lake_hist_layer_checklist_2 = True
                mdv_hub_layer_checklist_2 = True
                mdv_mart_layer_checklist_2 = True
                rdv_source_to_raw_checklist_2 = True
                rdv_raw_to_lake_checklist_2 = True
                rdv_source_to_lake_checklist_2 = True
                edv_lake_to_lake_hist_checklist_2 = True
                edv_lake_hist_to_hub_checklist_2 = True
                edv_hub_to_mart_checklist_2 = True

            if mdv_lake_layer_checklist_2:
                checklist_2.append('Meta Data Validation  -  Lake Layer')
            if mdv_lake_hist_layer_checklist_2:
                checklist_2.append('Meta Data Validation  -  Lake Hist Layer')
            if mdv_hub_layer_checklist_2:
                checklist_2.append('Meta Data Validation  -  Hub Layer')
            if mdv_mart_layer_checklist_2:
                checklist_2.append('Meta Data Validation  -  Mart Layer')
            if rdv_source_to_raw_checklist_2:
                checklist_2.append('Source Data Validation  -  Source to Raw Layer')
            if rdv_raw_to_lake_checklist_2:
                checklist_2.append('Source Data Validation  -  Raw to Lake Layer')
            if rdv_source_to_lake_checklist_2:
                checklist_2.append('Source Data Validation  -  Source to Lake Layer')
            if edv_lake_to_lake_hist_checklist_2:
                checklist_2.append('ETL Data Validation - Lake to Lake Hist Layer')
            if edv_lake_hist_to_hub_checklist_2:
                checklist_2.append('ETL Data Validation - Lake Hist to Hub Layer')
            if edv_hub_to_mart_checklist_2:
                checklist_2.append('ETL Data Validation - Hub to Mart Layer')

            if submit_2:
                exec_message_board = ''
                session_state.checklist_2_exec = True

                for selected_item in stqdm(checklist_2):
                    if selected_item == 'Meta Data Validation  -  Lake Layer':
                        exec_lake_layer = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {selected_item}'
                        st.write(exec_lake_layer)

                        run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(':',
                                                                                                                   '').replace(
                            '/', '')
                        print("getting conn")
                        conn = dbricks.DatabricksConnection.getConnection('',
                                                                          config_dict['Databricks connection string'])
                        print(conn)
                        sttm_lake_data = rdconfig.ReadConfigData.ReadLakeSttmData("",
                                                                                  config_dict[
                                                                                      'Input folder path'] + '\\Input Files\\' +
                                                                                  config_dict['Src to Lake STTM  name'])
                        metadata_obj = MD.Metadatavalidation(sttm_lake_data, config_dict['Output folder path'], conn,
                                                             run_time)
                        _val = metadata_obj.validate_metadata('Lake')
                        _val = 'Lake Table Meta Data Validation Completed Successfully, Validation results placed at : %s' % _val

                        _val = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {_val}'
                        exec_lake_layer = _val + exec_lake_layer
                        st.write(exec_lake_layer)

                        exec_message_board = exec_message_board + '  \n  \n' + exec_lake_layer
                        with open(str(global_output_folder_path), "a") as _f:
                            _f.write(exec_lake_layer + '\n')

                    if selected_item == 'Meta Data Validation  -  Lake Hist Layer':
                        exec_hist_layer = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {selected_item}'
                        st.write(exec_hist_layer)

                        run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(':',
                                                                                                                   '').replace(
                            '/', '')
                        conn = dbricks.DatabricksConnection.getConnection('',
                                                                          config_dict['Databricks connection string'])
                        sttm_data = rdconfig.ReadConfigData.ReadSttmData("",
                                                                         config_dict[
                                                                             'Input folder path'] + '\\Input Files\\' +
                                                                         config_dict['Lake to Lake Hist STTM name'])
                        metadata_obj = MD.Metadatavalidation(sttm_data, config_dict['Output folder path'], conn,
                                                             run_time)
                        _val = metadata_obj.validate_metadata('Lake-Hist')
                        _val = 'Lake Hist Table Meta Data Validation Completed Successfully, Validation results placed at : %s' % _val
                        _val = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {_val}'
                        exec_hist_layer = _val + exec_hist_layer
                        st.write(exec_hist_layer)

                        exec_message_board = exec_message_board + '  \n  \n' + exec_hist_layer
                        with open(str(global_output_folder_path), "a") as _f:
                            _f.write(exec_hist_layer + '\n')

                    if selected_item == 'Meta Data Validation  -  Hub Layer':
                        exec_hub_layer = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {selected_item}'
                        st.write(exec_hub_layer)

                        run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(':',
                                                                                                                   '').replace(
                            '/', '')
                        conn = dbricks.DatabricksConnection.getConnection('',
                                                                          config_dict['Databricks connection string'])
                        sttm_data = rdconfig.ReadConfigData.ReadSttmData("",
                                                                         config_dict[
                                                                             'Input folder path'] + '\\Input Files\\' +
                                                                         config_dict['Lake Hist to Hub STTM name'])
                        metadata_obj = MD.Metadatavalidation(sttm_data, config_dict['Output folder path'], conn,
                                                             run_time)
                        _val = metadata_obj.validate_metadata('Hub')
                        _val = 'Hub Table Meta Data Validation Completed Successfully, Validation results placed at : %s' % _val
                        _val = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {_val}'
                        exec_hub_layer = _val + exec_hub_layer
                        st.write(exec_hub_layer)

                        exec_message_board = exec_message_board + '  \n  \n' + exec_hub_layer
                        with open(str(global_output_folder_path), "a") as _f:
                            _f.write(exec_hub_layer + '\n')

                    if selected_item == 'Meta Data Validation  -  Mart Layer':
                        exec_mart_layer = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {selected_item}'
                        st.write(exec_mart_layer)

                        run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(':',
                                                                                                                   '').replace(
                            '/', '')
                        conn = dbricks.DatabricksConnection.getConnection('',
                                                                          config_dict['Databricks connection string'])
                        sttm_data = rdconfig.ReadConfigData.ReadSttmData("",
                                                                         config_dict[
                                                                             'Input folder path'] + '\\Input Files\\' +
                                                                         config_dict['Hub to Mart STTM name'])
                        metadata_obj = MD.Metadatavalidation(sttm_data, config_dict['Output folder path'], conn,
                                                             run_time)
                        _val = metadata_obj.validate_metadata('Mart')
                        _val = 'Mart Table Meta Data Validation Completed Successfully, Validation results placed at : %s' % _val
                        _val = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {_val}'
                        exec_mart_layer = _val + exec_mart_layer
                        st.write(exec_mart_layer)

                        exec_message_board = exec_message_board + '  \n  \n' + exec_mart_layer
                        with open(str(global_output_folder_path), "a") as _f:
                            _f.write(exec_mart_layer + '\n')

                    if selected_item == 'Source Data Validation  -  Source to Raw Layer':
                        exec_source_to_raw_layer = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {selected_item}'
                        st.write(exec_source_to_raw_layer)

                        run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(':',
                                                                                                                   '').replace(
                            '/', '')
                        conn = dbricks.DatabricksConnection.getConnection('',
                                                                          config_dict['Databricks connection string'])
                        src_lake = SL.ValidateSrcToLake(conn, config_dict['Output folder path'], run_time)
                        for file_num in range(len(config_dict['Raw data file name'])):
                            run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(
                                ':', '').replace('/', '')
                            _val = src_lake.srctolake('%s\\Input Files\\%s' % (
                                config_dict['Input folder path'], config_dict['Source data file name'][file_num]),
                                                      '%s\\Input Files\\%s' % (config_dict['Input folder path'],
                                                                               config_dict['Raw data file name'][
                                                                                   file_num]),
                                                      False, run_time)
                        _val = 'Source to Raw Data Validation Completed Successfully, Validation results placed at : %s' % _val
                        _val = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {_val}'
                        exec_source_to_raw_layer = _val + exec_source_to_raw_layer
                        st.write(exec_source_to_raw_layer)

                        exec_message_board = exec_message_board + '  \n  \n' + exec_source_to_raw_layer
                        with open(str(global_output_folder_path), "a") as _f:
                            _f.write(exec_source_to_raw_layer + '\n')

                    if selected_item == 'Source Data Validation  -  Raw to Lake Layer':
                        exec_raw_lake_layer = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {selected_item}'
                        st.write(exec_raw_lake_layer)

                        run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(':',
                                                                                                                   '').replace(
                            '/', '')
                        conn = dbricks.DatabricksConnection.getConnection('',
                                                                          config_dict['Databricks connection string'])
                        src_lake = SL.ValidateSrcToLake(conn, config_dict['Output folder path'], run_time)
                        for file_num in range(len(config_dict['Raw data file name'])):
                            run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(
                                ':', '').replace('/', '')
                            _val = src_lake.srctolake(False,
                                                      '%s\\Input Files\\%s' % (config_dict['Input folder path'],
                                                                               config_dict['Raw data file name'][
                                                                                   file_num]),
                                                      config_dict['Lake table name'][file_num], run_time)
                        _val = 'Raw to Lake Layer Validation Completed Successfully, Validation results placed at : %s' % _val

                        _val = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {_val}'
                        exec_raw_lake_layer = _val + exec_raw_lake_layer
                        st.write(exec_raw_lake_layer)

                        exec_message_board = exec_message_board + '  \n  \n' + exec_raw_lake_layer
                        with open(str(global_output_folder_path), "a") as _f:
                            _f.write(exec_raw_lake_layer + '\n')

                    if selected_item == 'Source Data Validation  -  Source to Lake Layer':
                        exec_source_lake_layer = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {selected_item}'
                        st.write(exec_source_lake_layer)

                        run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(':',
                                                                                                                   '').replace(
                            '/', '')
                        conn = dbricks.DatabricksConnection.getConnection('',
                                                                          config_dict['Databricks connection string'])
                        src_lake = SL.ValidateSrcToLake(conn, config_dict['Output folder path'], run_time)
                        for file_num in range(len(config_dict['Raw data file name'])):
                            run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(
                                ':', '').replace('/', '')
                            _val = src_lake.srctolake('%s\\Input Files\\%s' % (
                                config_dict['Input folder path'], config_dict['Source data file name'][file_num]),
                                                      '%s\\Input Files\\%s' % (config_dict['Input folder path'],
                                                                               config_dict['Raw data file name'][
                                                                                   file_num]),
                                                      False, run_time)
                        _val = 'Source to Raw Data Validation Completed Successfully, Validation results placed at : %s' % _val
                        _val = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {_val}'
                        exec_raw_lake_layer = _val + exec_raw_lake_layer
                        st.write(exec_raw_lake_layer)
                        exec_message_board = exec_message_board + '  \n  \n' + exec_source_lake_layer
                        with open(str(global_output_folder_path), "a") as _f:
                            _f.write(exec_source_lake_layer + '\n')

                    if selected_item == 'ETL Data Validation - Lake to Lake Hist Layer':
                        exec_lake_to_lake_hist = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {selected_item}'
                        st.write(exec_lake_to_lake_hist)

                        run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(':',
                                                                                                                   '').replace(
                            '/', '')
                        conn = dbricks.DatabricksConnection.getConnection('',
                                                                          config_dict['Databricks connection string'])
                        etl_obj = ETL.ValidateData(conn, config_dict['Output folder path'], run_time)
                        _val, path = etl_obj.GenerateReport('Lake-LakeHist')
                        _val = _val + ' Lake Layer to Lake Hist Layer Validation Completed, Validation results placed at : %s' % path
                        _val = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {_val}'
                        exec_lake_to_lake_hist = _val + exec_lake_to_lake_hist
                        st.write(exec_lake_to_lake_hist)

                        exec_message_board = exec_message_board + '  \n  \n' + exec_lake_to_lake_hist
                        with open(str(global_output_folder_path), "a") as _f:
                            _f.write(exec_lake_to_lake_hist + '\n')

                    if selected_item == 'ETL Data Validation - Lake Hist to Hub Layer':
                        exec_lake_hist_to_hub = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {selected_item}'
                        st.write(exec_lake_hist_to_hub)

                        run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(':',
                                                                                                                   '').replace(
                            '/', '')
                        conn = dbricks.DatabricksConnection.getConnection('',
                                                                          config_dict['Databricks connection string'])
                        etl_obj = ETL.ValidateData(conn, config_dict['Output folder path'], run_time)
                        _val, path = etl_obj.GenerateReport('LakeHist-Hub')
                        _val = _val + ' Lake Hist Layer to Hub Layer Validation Completed, Validation results placed at : %s' % path
                        _val = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {_val}'
                        exec_lake_hist_to_hub = _val + exec_lake_hist_to_hub
                        st.write(exec_lake_hist_to_hub)

                        exec_message_board = exec_message_board + '  \n  \n' + exec_lake_hist_to_hub
                        with open(str(global_output_folder_path), "a") as _f:
                            _f.write(exec_lake_hist_to_hub + '\n')

                    if selected_item == 'ETL Data Validation - Hub to Mart Layer':
                        exec_hub_to_mart = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {selected_item}'
                        st.write(exec_hub_to_mart)

                        run_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", '-').replace(':',
                                                                                                                   '').replace(
                            '/', '')
                        conn = dbricks.DatabricksConnection.getConnection('',
                                                                          config_dict['Databricks connection string'])
                        etl_obj = ETL.ValidateData(conn, config_dict['Output folder path'], run_time)
                        _val, path = etl_obj.GenerateReport('Hub-Mart')
                        _val = _val + ' Hub Layer to Mart Layer Validation Completed Successfully %s' % path
                        _val = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}: {_val}'
                        exec_hub_to_mart = _val + exec_hub_to_mart
                        st.write(exec_hub_to_mart)

                        exec_message_board = exec_message_board + '  \n  \n' + exec_hub_to_mart
                        with open(str(global_output_folder_path), "a") as _f:
                            _f.write(exec_hub_to_mart + '\n')

                    session_state.exec_message_board_main = exec_message_board

            if export_button:
                if session_state.exec_message_board_main:
                    st.write(session_state.exec_message_board_main)

                timestamp_str = str(
                    datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S').replace('/', '').replace(':', '').replace(' ',
                                                                                                                    '-'))
                output_folder_path = str(config_dict['Output folder path']).strip().replace('\\', '/')
                final_path = output_folder_path + '/Output Files/logs/'

                if not os.path.exists(final_path):
                    os.makedirs(final_path)

                final_path = final_path + 'logs-' + timestamp_str + '.txt'

                with open(str(final_path), "a") as _f:
                    _f.write(session_state.exec_message_board_main + '\n')

                st.success(f'Logs stored in {final_path}')

            if email_button or session_state.email_button_exec:
                session_state.email_button_exec = True
                if session_state.exec_message_board_main:
                    st.write(session_state.exec_message_board_main)

                if email_button:
                    st.markdown("<h3 style='text-align: left; color: black'>Upload email recipient address file</h3>",
                                unsafe_allow_html=True)
                    uploaded_email_file = st.file_uploader('', type=("txt"), key='email_file_uploader')
                    session_state.email_add_file_exec = uploaded_email_file

                if session_state.email_button_exec:
                    send_email_function(session_state.email_add_file_exec.read())
                    st.success('Email sent successfully')

            if defect_uploader_button:
                if session_state.exec_message_board_main:
                    st.write(session_state.exec_message_board_main)

                st.success('Defect Uploader')

            if defect_generator_button:
                if session_state.exec_message_board_main:
                    st.write(session_state.exec_message_board_main)
                st.success('Defect Generator')
