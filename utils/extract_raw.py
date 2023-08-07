import re

class raw_credit_report_data_extraction():
    def __init__(self):
        self.regex_payment = re.compile(r'\n\d{2}\/\d{2}\/\d{4}\n.+\n[-]?\$\d{1,1000}\.\d{1,1000}')
        self.regex_transaction = re.compile(r'\n\d{2}\/\d{2}\/\d{4}\n.+\n\d{1}%\n[-+]?\$\d{1,1000}\.\d{1,1000}\n[-+]?\$\d{1,1000}\.\d{1,1000}')
        self.regex_installments = re.compile(r'\n\d{2}\/\d{2}\/\d{4}\n.+\n[-+]?\$\d{1,1000}\.\d{1,1000}\s\nTRANSACTION\s.*')
    
    def extract(self, my_credit_report):
        holder_payment = []
        holder_transaction = []
        holder_installments = []

        for page in range(len(my_credit_report.pages)):
            page_content = my_credit_report.pages[page].extract_text().replace(',','')

            holder_installments.append(self.regex_installments.findall(page_content))
            page_content = self.regex_installments.sub('', page_content)

            holder_transaction.append(self.regex_transaction.findall(page_content))
            page_content = self.regex_transaction.sub('', page_content)

            holder_payment.append(self.regex_payment.findall(page_content))
            page_content = self.regex_payment.sub('', page_content)
            
        return (
            [y for x in holder_payment for y in x], 
            [y for x in holder_transaction for y in x], 
            [y for x in holder_installments for y in x]
        )