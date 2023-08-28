import subprocess

cmd='aws s3api put-object-acl --bucket takdevlanding --key microserviceteamfolder/JUB3949/Phosphorylation_2021-09-14T12:01:57.004Z/20210914_120157_00016_6igs2_bucket-00000.gz --acl bucket-owner-full-control --profile Takeda_RDIT_Data_Dev --no-verify-ssl'
push=subprocess.Popen(cmd, shell=True, stdout = subprocess.PIPE)
print(push.returncode)