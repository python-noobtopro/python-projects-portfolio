from PdfTextExtract import TextExtract

# url = 'https://oncoresponse.com/careers/Director-Sr-Director-of-Translational-Medicine.pdf'
url = "https://southtexasruralhealth.com/wp-content/uploads/2021/06/Maintenance-Custodian.pdf"
print(TextExtract(url).extract_all())
