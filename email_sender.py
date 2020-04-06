import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import os


def send(email, pw, to_addr, subject, output_image_name):
  #create instances for message and image
  msg = MIMEMultipart()
  msgImage = MIMEMultipart()  

  #set email details
  msg['From'] = email
  msg['To'] = to_addr
  msg['Subject'] = subject

  #read the image from directory, one instance is used for attachment another is for embedding image into email body
  img_data = open(output_image_name, 'rb').read()
  msgImage = MIMEImage(img_data)
  image = MIMEImage(img_data, name=os.path.basename(output_image_name))
  msg.attach(image)

  #through content-id we add the image into the body of email through html 
  msgImage.add_header('Content-ID', '<image1>')

  html = """\
	<html>
	  <head>
		  <style>
			  h3, p {font-family: roboto;clear:right;}          
			  h3 {font-size: 20px;}
			  p {font-size:15px;}          
			  img {width: 40%;float: right;border: 2px solid #FFD433;}
			 .clearfix {overflow: auto;}
		  </style>    
	  </head>
	  <body>    
		<h3>Hello sir/madam</h3>
		<p class="clearfix">This email contains the information you requested
		<img class="img" src="cid:image1"></p>       
	  </body>
	</html>
"""

  #add html and image to MIME message
  msg.attach(MIMEText(html, 'html'))
  msg.attach(msgImage)

  #login to smtp, convert MIME message to string, send email
  server = smtplib.SMTP('smtp.gmail.com', 587)
  server.starttls()
  server.login(email, pw)
  text = msg.as_string()
  server.sendmail(email, to_addr, text)
  server.quit()