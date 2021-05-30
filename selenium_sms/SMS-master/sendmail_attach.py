import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
 
email_user = 'suk555@mobigen.com'
email_send = 'kim2100@mobigen.com'
subject ='Python'
 
msg =MIMEMultipart()
msg['From'] = email_user
msg['To'] = email_send
msg['Subject'] = subject
 
body = 'Hi thre'
msg.attach(MIMEText(body,'plain'))
 
filename = 'mail.txt'
attachment = open(filename,'rb')
 
part =MIMEBase('application','octet-stream')
part.set_payload((attachment).read())
encoders.encode_base64(part)
part.add_header('Content-disposition',"attachment,filename= "+filename)
 
 
msg.attach(part)
text= msg.as_string()
 
server = smtplib.SMTP('shield.mobigen.com',25)
 
 
#server.starttls()
server.login(email_user,'suk852456')
server.sendmail(email_user,email_send,text)
server.quit()
