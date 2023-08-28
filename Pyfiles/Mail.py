import shutil
import win32com.client as win32
import yaml

class Mailer:

    def __init__(self,input_file_path ,output_file_path,src_sys_name):
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path
        self.src_sys_name = src_sys_name


    def send_email(self, content):
        outlook = win32.Dispatch('outlook.application')
        print("Executing Send mail fun")
        email_list = content[0].split(':')[1].split(',')
        group = content[1]
        mail = outlook.CreateItem(0)
        mails = email_list#['farhhan.adil@takeda.com','madhu.reddy@takeda.com']
        for mail_item in mails:
            mail.Recipients.Add(mail_item)
        mail.Subject = 'AWS Testing Accelerator Results'
        mail.Body = 'Message body'
        mail.HTMLBody = """<p>Hi,</p>
        <p>The attached zip folder contains the output files generated from the AWS Testing Accelerator.</p>
        <p>NOTE- Unzip the folder to extract all the Testing documents.</p>
        <p>Thanks and Regard,</p>
        <p>AWS Testing Accelerator Team</p> """
        if 'TEST' in group.upper():
            shutil.make_archive("Output Files", 'zip', self.output_file_path)
            mail.Attachments.Add(self.output_file_path + '\\Output Files.zip')
        elif 'DEV' in group.upper():
            shutil.make_archive("Output Files", 'zip', self.output_file_path)
            mail.Attachments.Add(self.output_file_path + '\\Output Files.zip')
        elif 'CLIENT' in group.upper():
            shutil.make_archive("Output Files", 'zip', self.output_file_path)
            mail.Attachments.Add(self.output_file_path + '\\Output Files.zip')
        mail.Send()